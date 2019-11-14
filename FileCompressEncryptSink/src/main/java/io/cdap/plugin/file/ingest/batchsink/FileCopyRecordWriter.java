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
import io.cdap.plugin.file.ingest.batchsink.encryption.FileCompressEncrypt;
import io.cdap.plugin.file.ingest.batchsink.encryption.PGPUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.cdap.plugin.file.ingest.encryption.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * The record writer that takes file metadata and streams data from source database
 * to destination database
 */
public class FileCopyRecordWriter extends RecordWriter<NullWritable, FileMetadata> {
    private final boolean compression;
    private final boolean encryption;
    private final String bucketname;
    private final String publicKeyPath;
    private final String project;
    private final String destpath;
    private final String suffix;
    private final String gcsserviceaccountjson;

    private static final Logger LOG = LoggerFactory.getLogger(FileCopyRecordWriter.class);
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

        LOG.info("Initializating of RecordWriter");

    if (conf.get(FileCopyOutputFormat.NAME_FILECOMPRESSION).equals("NONE")){
        compression=false;
    } else {
        compression=true;
    }

    if (conf.get(FileCopyOutputFormat.NAME_FILEENCRYPTION).equals("NONE")){
          encryption=false;
    } else {
          encryption=true;
    }

    bucketname= conf.get( FileCopyOutputFormat.NAME_GCS_BUCKET,null );

    publicKeyPath = conf.get( FileCopyOutputFormat.NAME_PGP_PUBKEY, null);

    project = conf.get( FileCopyOutputFormat.NAME_GCS_PROJECTID, null);

    gcsserviceaccountjson = conf.get( FileCopyOutputFormat.NAME_GCS_SERVICEACCOUNTJSON, null);

    destpath = conf.get( FileCopyOutputFormat.NAME_GCS_DESTPATH , null);

    suffix = conf.get( FileCopyOutputFormat.NAME_GCS_DESTPATH_SUFFIX , null);

    if (encryption) {
        //Read the Public Key to Encrypt Data

        try {
            encKey = PGPUtil.readPublicKey(publicKeyPath);
            LOG.info("Retreived PublicKey");
        }catch (PGPException ex) {
            LOG.error(ex.getMessage());
            throw new IOException(ex.getMessage());
        }
    }

    // Create GCS Storage using the credentials

      storage = getGoogleStorage(gcsserviceaccountjson,project);
      LOG.info("Created GCS Storage");
      bucket = getBucket(storage,bucketname);
      LOG.info("Created GCS Bucket");

  }



  private Storage getGoogleStorage(String serviceAccountJSON, String project) {
    Credentials credentials = null;
    try {
      credentials = GoogleCredentials.fromStream(new FileInputStream(serviceAccountJSON));
    } catch (IOException e) {
      e.printStackTrace();
    }

    Storage storage = StorageOptions.newBuilder()
            .setCredentials(credentials)
            .setProjectId(project)
            .build()
            .getService();

    return storage;

  }

  private Bucket getBucket(Storage storage,String bucketname) {

    Bucket bucket = storage.get(bucketname);
    if (bucket == null) {
      System.out.println("Creating new bucket.");
      bucket = storage.create(BucketInfo.of(bucketname));
    }
    return bucket;
  }






  /**
   * This method connects to the source filesystem and copies the file specified by the FileMetadata input to the
   * destination filesystem.
   *
   * @param key Unused key.
   * @param fileMetadata Contains metadata for the file we wish to copy.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public void write(NullWritable key, FileMetadata fileMetadata) throws IOException, InterruptedException {

    if (fileMetadata.getRelativePath().isEmpty()) {
      return;
    }

    // construct file paths for source and destination
    //Path srcPath = new Path(fileMetadata.getFullPath());

    String outFileName = destpath + fileMetadata.getRelativePath();

    if ( encryption) outFileName+=".pgp";
    LOG.info("Output File Name " + outFileName);

      BlobId blobId = BlobId.of(bucket.getName(), outFileName);

      BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/pgp-encrypted").build();


      InputStream inputStream = null;

      try {
        inputStream = FileCompressEncrypt.gcsWriter(fileMetadata.getFullPath(),encKey );
      } catch (IOException e) {
        e.printStackTrace();
      }

      byte[] buffer = new byte[1 << 16];
      try {
        try (WriteChannel writer =
                     storage.writer(blobInfo)) {
          int limit;
          while ((limit = inputStream.read(buffer)) >= 0) {
            System.out.println("upload file " + limit);
            writer.write(ByteBuffer.wrap(buffer, 0, limit));
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      try {
        inputStream.close();
      } catch (IOException e) {
        e.printStackTrace();
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
