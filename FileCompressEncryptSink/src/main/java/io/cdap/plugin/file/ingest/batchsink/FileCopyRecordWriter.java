/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.file.ingest.batchsink;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import io.cdap.plugin.file.ingest.common.FileListData;
import io.cdap.plugin.file.ingest.encryption.FileCompressEncrypt;
import io.cdap.plugin.file.ingest.encryption.PGPCertUtil;
import io.cdap.plugin.file.ingest.utils.FileMetaData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * The record writer that takes file metadata and streams data from source database
 * to destination database
 */
public class FileCopyRecordWriter extends RecordWriter<NullWritable, FileListData> {
    private static final Logger LOG = LoggerFactory.getLogger(FileCopyRecordWriter.class);

    static Configuration conf = null;

    static {
        conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }

    private final boolean compression;
    private final boolean encryption;
    private final String bucketname;
    private final String publicKeyPath;
    private final String project;
    private final String destpath;
    private final String suffix;
    private final String gcsserviceaccountjson;
    private PGPPublicKey encKey = null;
    private Storage storage = null;
    private Bucket bucket = null;

    /**
     * Construct a RecordWriter given user configurations.
     *
     * @param conf The configuration that contains required information to intialize the recordWriter.
     * @throws IOException
     */
    public FileCopyRecordWriter(Configuration conf) throws IOException {

        LOG.info("Initializing of RecordWriter");

        if (conf.get(FileCopyOutputFormat.NAME_FILECOMPRESSION).equals("NONE")) {
            compression = false;
            LOG.info("Compression is set to false");
        } else {
            compression = true;
            LOG.info("Compression is set to true");
        }

        if (conf.get(FileCopyOutputFormat.NAME_FILEENCRYPTION).equals("NONE")) {
            encryption = false;
            LOG.info("Encryption is set to false");
        } else {
            encryption = true;
            LOG.info("Encryption is set to true");
        }

        bucketname = conf.get(FileCopyOutputFormat.NAME_GCS_BUCKET, null);
        LOG.info("Bucket Name - " + bucketname);

        publicKeyPath = conf.get(FileCopyOutputFormat.NAME_PGP_PUBKEY, null);
        LOG.info("PubKeyPath - " + publicKeyPath);

        project = conf.get(FileCopyOutputFormat.NAME_GCS_PROJECTID, null);
        LOG.info("GCS Project ID - " + project);

        gcsserviceaccountjson = conf.get(FileCopyOutputFormat.NAME_GCS_SERVICEACCOUNTJSON, null);
        LOG.info("GCS Service Account-" + gcsserviceaccountjson);

        destpath = conf.get(FileCopyOutputFormat.NAME_GCS_DESTPATH, null);
        LOG.info("Dest Path - " + destpath);

        suffix = conf.get(FileCopyOutputFormat.NAME_GCS_DESTPATH_SUFFIX, null);
        LOG.info("Suffix - " + suffix);

        if (encryption) {
            //Read the Public Key to Encrypt Data
            try {
                encKey = PGPCertUtil.readPublicKey(publicKeyPath);
                LOG.info("Retreived PublicKey");
            } catch (PGPException ex) {
                LOG.error(ex.getMessage());
                throw new IOException(ex.getMessage());
            }
        }

        // Create GCS Storage using the credentials
        storage = getGoogleStorage(gcsserviceaccountjson, project);
        LOG.info("Created GCS Storage");
        bucket = getBucket(storage, bucketname);
        LOG.info("Created GCS Bucket");
    }

    private static FileMetaData getFileMetaData(String filePath, String uri) throws IOException {
        return new FileMetaData(uri + '/' + filePath, conf);
    }

    private Storage getGoogleStorage(String serviceAccountJSON, String project) {
        Credentials credentials = null;
        try {
            credentials = GoogleCredentials.fromStream(new FileInputStream(serviceAccountJSON));
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }

        Storage storage = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId(project)
                .build()
                .getService();

        return storage;
    }

    private Bucket getBucket(Storage storage, String bucketname) {
        Bucket bucket = storage.get(bucketname);
        if (bucket == null) {
            LOG.info("Creating new bucket.");
            bucket = storage.create(BucketInfo.of(bucketname));
        }
        return bucket;
    }

    /**
     * This method connects to the source filesystem and copies the file specified by the FileMetadata input to the
     * destination filesystem.
     *
     * @param key          Unused key.
     * @param fileListData Contains metadata for the file we wish to copy.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(NullWritable key, FileListData fileListData) throws IOException, InterruptedException {

        if (fileListData.getRelativePath().isEmpty()) {
            return;
        }

        // construct file paths for source and destination
        //Path srcPath = new Path(fileMetadata.getFullPath());

        String outFileName = destpath + fileListData.getRelativePath();
        String contentType = "application/octet-stream";
        if (compression) {
            outFileName += ".zip";
            contentType = "application/zip";
        }

        if (encryption) {
            outFileName += ".pgp";
            contentType = "application/pgp-encrypted";
        }

        FileMetaData fileMetaData = null;
        String fileName = fileListData.getFullPath();
        if (fileName != null) {
            try {
                fileMetaData = getFileMetaData(fileName, fileListData.getHostURI());
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }

        LOG.info("Output File Name " + outFileName);

        BlobId blobId = BlobId.of(bucket.getName(), outFileName);

        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(contentType).build();

        InputStream inputStream = null;

        try {
            inputStream = FileCompressEncrypt.gcsWriter(fileMetaData, compression, encryption, encKey);
            byte[] buffer = new byte[1 << 16];
            try (WriteChannel writer =
                         storage.writer(blobInfo)) {
                int limit;
                while ((limit = inputStream.read(buffer)) >= 0) {
                    LOG.info("upload file " + limit);
                    writer.write(ByteBuffer.wrap(buffer, 0, limit));
                }
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
        try {
            inputStream.close();
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // attempts to close the other even if one fails
        // try {
        //   destFileSystem.close();
        // } finally {
        //  safelyCloseSourceFilesystems(sourceFilesystemMap.values().iterator());
        // }
    }

    /**
     * this method attempts to close every Filesystem object in the list, logs a warning
     * for each object that fails to close
     *
     * @param fs The iterator over all the Filesystems we wish to close.
     */
    private void safelyCloseSourceFilesystems(Iterator<FileSystem> fs) {
        while (fs.hasNext()) {
            try {
                fs.next().close();
            } catch (IOException e) {
                LOG.warn(e.getMessage());
            }
        }
    }
}
