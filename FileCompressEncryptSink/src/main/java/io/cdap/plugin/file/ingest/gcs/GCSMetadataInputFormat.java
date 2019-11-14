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

package io.cdap.plugin.file.ingest.gcs;

import io.cdap.plugin.file.ingest.FileMetadata;
import io.cdap.plugin.file.ingest.MetadataInputFormat;
import io.cdap.plugin.file.ingest.MetadataInputSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * MetadataInputFormat for S3 Filesystem. Implements credentials setters
 * specific for S3
 */
public class GCSMetadataInputFormat extends MetadataInputFormat {



  // configs for s3a
  public static final String GCS_PROJECT_ID = "fs.gcs.project.id";
  public static final String GCS_SERVICE_ACCOUNT_JSON = "fs.gcs.serviceaccount.json";
  public static final String GCS_FS_CLASS = "fs.gcs.impl";


  public static final Logger LOG = LoggerFactory.getLogger(GCSMetadataInputFormat.class);


  public static void setGcsProjectId(Configuration conf, String value) {
    conf.set(GCS_PROJECT_ID, value);
  }

  public static void setGcsServiceAccountJson(Configuration conf, String value) {
    conf.set(GCS_SERVICE_ACCOUNT_JSON, value);
  }

  public static void setGcsFsClassFsClass(Configuration conf) {
    conf.set(GCS_FS_CLASS, S3AFileSystem.class.getName());
  }


  @Override
  protected MetadataInputSplit getInputSplit() {
    return new GCSMetadataInputSplit();
  }

  @Override
  protected FileMetadata getFileMetadata(FileStatus fileStatus, String sourcePath, Configuration conf)
    throws IOException {
    switch (fileStatus.getPath().toUri().getScheme()) {
      case "gcs":
        return new GCSFileMetadata(fileStatus, sourcePath, conf.get(GCS_PROJECT_ID), conf.get(GCS_SERVICE_ACCOUNT_JSON));
      default:
        throw new IOException("Scheme must be gcs.");
    }
  }
}
