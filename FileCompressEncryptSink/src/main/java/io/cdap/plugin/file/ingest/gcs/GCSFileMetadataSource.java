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
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.file.ingest.AbstractFileMetadataSource;
import io.cdap.plugin.file.ingest.FileMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * FileCopySource plugin that pulls filemetadata from S3 Filesystem.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("GCSFileMetadataSource")
@Description("Reads file metadata from S3 bucket.")
public class GCSFileMetadataSource extends AbstractFileMetadataSource<GCSFileMetadata> {
  private GCSFileMetadataSourceConfig config;
  private static final Logger LOG = LoggerFactory.getLogger(GCSFileMetadataSource.class);

  public GCSFileMetadataSource(GCSFileMetadataSourceConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    List<Schema.Field> fieldList = new ArrayList<>(FileMetadata.DEFAULT_SCHEMA.getFields());
    fieldList.addAll(GCSFileMetadata.CREDENTIAL_SCHEMA.getFields());
    pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.recordOf("S3Schema", fieldList));
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    super.prepareRun(context);
    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    // initialize configuration
    setDefaultConf(conf);
    GCSMetadataInputFormat.setURI(conf, config.filesystemURI);
    String fsScheme = URI.create(config.filesystemURI).getScheme();
    switch (fsScheme) {
      case "gcs":
        GCSMetadataInputFormat.setGcsProjectId(conf, config.gcsprojectid);
        GCSMetadataInputFormat.setGcsServiceAccountJson(conf, config.gcsserviceaccountjson);
        GCSMetadataInputFormat.setGcsFsClassFsClass(conf);
        break;
      default:
        throw new IllegalArgumentException("Scheme must be either s3a or s3n.");
    }

    context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(GCSMetadataInputFormat.class, conf)));
  }

  @Override
  public void transform(KeyValue<NullWritable, GCSFileMetadata> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(input.getValue().toRecord());
  }

  /**
   * Configurations required for connecting to S3Filesystem.
   */
  public class GCSFileMetadataSourceConfig extends AbstractFileMetadataSourceConfig {

    // configurations for S3
    @Macro
    @Description("The URI of the filesystem")
    public String filesystemURI;

    @Macro
    @Description("Your GCS Project ID")
    public String gcsprojectid;

    @Macro
    @Description("Your GCS Service Account JSON File")
    public String gcsserviceaccountjson;

    public GCSFileMetadataSourceConfig(String name, String sourcePaths, Integer maxSplitSize,
                                      String filesystemURI, String gcsprojectid,
                                      String gcsserviceaccountjson) {
      super(name, sourcePaths, maxSplitSize);
      this.filesystemURI = filesystemURI;
      this.gcsprojectid = gcsprojectid;
      this.gcsserviceaccountjson = gcsserviceaccountjson;
    }

    @Override
    public void validate() {
      super.validate();
      if (!this.containsMacro("filesystemURI")) {
        URI fsUri = URI.create(filesystemURI);
        if (!fsUri.getScheme().equals("s3a") && !fsUri.getScheme().equals("s3n")) {
          throw new IllegalArgumentException("URI scheme for S3 source must be s3a or s3n");
        }
      }
    }
  }
}
