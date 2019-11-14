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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.file.ingest.AbstractFileCopySink;
import io.cdap.plugin.file.ingest.AbstractFileCopySinkConfig;
import io.cdap.plugin.file.ingest.FileCopyOutputFormat;
//import org.apache.hadoop.fs.gcs.GoogleHadoopFileSystem;


/**
 * FileCopySink that writes to GCS.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("GCSFileCopySink")
@Description("Copies files from remote filesystem to S3 Filesystem")
public class GCSFileCopySink extends AbstractFileCopySink {

  private GCSFileCopySinkConfig config;

  public GCSFileCopySink(GCSFileCopySinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    super.prepareRun(context);
    context.addOutput(Output.of(config.referenceName, new GCSFileCopyOutputFormatProvider(config)));
  }

  /**
   * Adds necessary configuration resources and provides OutputFormat Class
   */
  public class GCSFileCopyOutputFormatProvider extends FileCopyOutputFormatProvider {
    public GCSFileCopyOutputFormatProvider(AbstractFileCopySinkConfig config) {
      super(config);
      GCSFileCopySinkConfig gcsConfig = (GCSFileCopySinkConfig) config;

      FileCopyOutputFormat.setFilesystemHostUri(conf, gcsConfig.filesystemURI);

      switch (config.getScheme()) {
        case "gcs" :
          conf.put(GCSMetadataInputFormat.GCS_PROJECT_ID, gcsConfig.gcsprojectid);
          conf.put(GCSMetadataInputFormat.GCS_SERVICE_ACCOUNT_JSON, gcsConfig.gcsserviceaccountjson);
         // conf.put(GCSMetadataInputFormat.GCS_FS_CLASS, GoogleHadoopFileSystem.class.getName());
          break;
        default:
          throw new IllegalArgumentException("Scheme must be either s3a or s3n.");
      }

    }
  }
}
