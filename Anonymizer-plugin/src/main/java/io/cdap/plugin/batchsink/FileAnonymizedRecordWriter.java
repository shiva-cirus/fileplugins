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

package io.cdap.plugin.batchsink;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;
import com.voltage.securedata.enterprise.FPE;
import com.voltage.securedata.enterprise.LibraryContext;
import com.voltage.securedata.enterprise.VeException;
import io.cdap.plugin.batchsink.common.FileListData;
import io.cdap.plugin.batchsink.utils.FileMetaData;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;

/**
 * The record writer that takes file metadata and streams data from source database
 * to destination database
 */
public class FileAnonymizedRecordWriter extends RecordWriter<NullWritable, FileListData> {
    private static final Logger LOG = LoggerFactory.getLogger(FileAnonymizedRecordWriter.class);

    static Configuration conf = null;

    static {
        conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }

    //private final boolean compression;
    //private final boolean encryption;
    private final String bucketName;
    private final String project;
    private final String destinationPath;
    private final String suffix;
    private final String gcsServiceAccountJson;
    private Integer bufferSize;
    private Storage storage = null;
    private Bucket bucket = null;
    private String policyUrl;
    private String identity;
    private String sharedSecret;
    private String trustStorePath;
    private String cachePath;

    FPE fpe     = null;

    /**
     * Construct a RecordWriter given user configurations.
     *
     * @param conf The configuration that contains required information to intialize the recordWriter.
     * @throws IOException
     */
    public FileAnonymizedRecordWriter(Configuration conf) throws IOException {
        LOG.info("Initializing of RecordWriter");

        bucketName = conf.get(FileAnonymizedOutputFormat.NAME_GCS_BUCKET, null);
        LOG.info("Bucket Name - " + bucketName);

        project = conf.get(FileAnonymizedOutputFormat.NAME_GCS_PROJECTID, null);
        LOG.info("GCS Project ID - " + project);

        gcsServiceAccountJson = conf.get(FileAnonymizedOutputFormat.NAME_GCS_SERVICEACCOUNTJSON, null);
        LOG.info("GCS Service Account-" + gcsServiceAccountJson);

        destinationPath = conf.get(FileAnonymizedOutputFormat.NAME_GCS_DESTPATH, null);
        LOG.info("Dest Path - " + destinationPath);

        suffix = conf.get(FileAnonymizedOutputFormat.NAME_GCS_DESTPATH_SUFFIX, null);
        LOG.info("Suffix - " + suffix);

        String size = conf.get(FileAnonymizedOutputFormat.NAME_BUFFER_SIZE, null);
        LOG.info("Buffer size - " + size);
        bufferSize = StringUtils.isNumeric(size) ? Integer.parseInt(size) : 1024;
        if (bufferSize <= 0) {
            bufferSize = 1024;
        }
        LOG.info("Buffer size applied - " + bufferSize);

        // Create GCS Storage using the credentials
        storage = getGoogleStorage(gcsServiceAccountJson, project);
        LOG.info("Created GCS Storage");
        bucket = getBucket(storage, bucketName);
        LOG.info("Created GCS Bucket");

        try {
            LOG.info("In initialize");
            // Load the JNI library
            System.loadLibrary("vibesimplejava");

            // Print the API version
            LOG.info("SimpleAPI version: " + LibraryContext.getVersion());

            // Sample configuration
            /*
            https://voltage-pp-0000.dataprotection.voltage.com/policy/clientPolicy.xml
            accounts22@dataprotection.voltage.com
            voltage123

            /opt/DA/javasimpleapi/trustStore
            /opt/DA/javasimpleapi/cache

            /opt/cdap-test-data/csv/

            midyear-courage-256620

            gs://shiva-cirus-test/disttest/

            /opt/cdap-test-data/midyear-courage-256620-0b0bc77ed7d8.json

            String policyURL      = "https://"
                    + "voltage-pp-0000.dataprotection.voltage.com"
                    + "/policy/clientPolicy.xml";
            String identity       = "accounts22@dataprotection.voltage.com";
            String sharedSecret   = "voltage123";
            String format         = "SSN";
            String trustStorePath = "../trustStore";
            String cachePath      = "../cache";
            */

            policyUrl = conf.get(FileAnonymizedOutputFormat.NAME_POLICY_URL, null);
            LOG.info("Policy URL - " + policyUrl);

            identity = conf.get(FileAnonymizedOutputFormat.NAME_IDENTITY, null);
            LOG.info("Identity - " + identity);

            sharedSecret = conf.get(FileAnonymizedOutputFormat.NAME_SHARED_SECRET, null);
            LOG.info("Shared Secret - " + sharedSecret);

            trustStorePath = conf.get(FileAnonymizedOutputFormat.NAME_TRUST_STORE_PATH, null);
            LOG.info("Trust Store Path - " + trustStorePath);

            cachePath = conf.get(FileAnonymizedOutputFormat.NAME_CACHE_PATH, null);
            LOG.info("Cache Path - " + cachePath);

            String format = "SSN";

            LibraryContext library = null;

            LOG.info("Building LibraryContext...");

            // Create the context for crypto operations
            library = new LibraryContext.Builder()
                    .setPolicyURL(policyUrl)
                    .setFileCachePath(cachePath)
                    .setTrustStorePath(trustStorePath)
                    .build();

            LOG.info("Building library.getFPEBuilder...");

            // Dev Guide Code Snippet: FPEBUILD; LC:5
            // Protect and access the credit card number
            fpe = library.getFPEBuilder(format)
                    .setSharedSecret(sharedSecret)
                    .setIdentity(identity)
                    .build();
        }catch (VeException ve) {
            LOG.error("Error loading library", ve);
            throw new IOException(ve);
        }
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

    private Bucket getBucket(Storage storage, String bucketName) {
        Bucket bucket = storage.get(bucketName);
        if (bucket == null) {
            LOG.info("Creating new bucket '{}'.", bucketName);
            bucket = storage.create(BucketInfo.of(bucketName));
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
        String outFileName = destinationPath + fileListData.getRelativePath();
        String contentType = "application/octet-stream";
        /*
        if (compression) {
            outFileName += ".zip";
            contentType = "application/zip";
        }

        if (encryption) {
            outFileName += ".pgp";
            contentType = "application/pgp-encrypted";
        }
        */

        FileMetaData fileMetaData = null;
        String fullPath = fileListData.getFullPath();
        if (fullPath != null) {
            try {
                fileMetaData = getFileMetaData(fullPath, fileListData.getHostURI());
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }

        LOG.info("Output File Name " + outFileName);

        try {
            String plaintext = "123-45-6789";

            LOG.info("Anonymizing {}...", plaintext);
            // Dev Guide Code Snippet: FPEPROTECT; LC:1
            String ciphertext = fpe.protect(plaintext);

            LOG.info("Anonymizing {} = {}...", plaintext, ciphertext);
        } catch (VeException ve) {
            LOG.error("Error while anonymizing.", ve);
        }

        /*
        BlobId blobId = BlobId.of(bucket.getName(), outFileName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(contentType).build();

        InputStream inputStream = null;

        try {
            inputStream = FileCompressEncrypt.gcsWriter(fileMetaData, compression, encryption, encKey, bufferSize);
            byte[] buffer = new byte[bufferSize];
            try (WriteChannel writer =
                         storage.writer(blobInfo)) {
                int limit;
                while ((limit = inputStream.read(buffer)) >= 0) {
                    LOG.debug("upload file " + limit);
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
        */
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
