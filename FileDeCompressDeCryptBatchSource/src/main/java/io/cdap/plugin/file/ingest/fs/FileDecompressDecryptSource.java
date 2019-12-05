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

package io.cdap.plugin.file.ingest.fs;

import io.cdap.cdap.api.annotation.Description;
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
import io.cdap.plugin.file.ingest.FileInputFormat;
import io.cdap.plugin.file.ingest.config.FileDecompressDecryptSourceConfig;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.net.URI;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

/** FileCopySource plugin that pulls filemetadata from local filesystem or local HDFS. */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("FileDecompressDecryptSource")
@Description("Reads file metadata from local filesystem or local HDFS.")
public class FileDecompressDecryptSource
    extends BatchSource<NullWritable, CSVRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(FileDecompressDecryptSource.class);
  private FileDecompressDecryptSourceConfig config;

  public FileDecompressDecryptSource(FileDecompressDecryptSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    conf.setBoolean("decrypt", config.decryptFile());
    conf.setBoolean("decompress", config.decompressFile());
    conf.set("privateKeyFilePath", config.privateKeyFilePath);
    conf.set("password", config.password);

    // initialize configurations
    setDefaultConf(conf);
    switch (config.scheme) {
      case "file":
        FileInputFormat.setURI(conf, new URI(config.scheme, null, Path.SEPARATOR, null).toString());
        break;
      case "hdfs":
        break;
      default:
        throw new IllegalArgumentException("Scheme must be either file or hdfs.");
    }

    context.setInput(
        Input.of(config.referenceName, new SourceInputFormatProvider(FileInputFormat.class, conf)));
  }

  /**
   * Converts the input FileMetaData to a StructuredRecord and emits it.
   *
   * @param input The input FileMetaData.
   * @param emitter Emits StructuredRecord that contains FileMetaData.
   */
  @Override
  public void transform(
      KeyValue<NullWritable, CSVRecord> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(toRecord(input.getValue()));
  }
  /** Converts to a StructuredRecord */
  public StructuredRecord toRecord(CSVRecord csvRecord) {
    StructuredRecord structuredRecord = null;
    // merge default schema and credential schema to create output schema
    try {
      Schema outputSchema;
      List<Schema.Field> fieldList = config.getSchema().getFields();
      outputSchema = Schema.recordOf("metadata", fieldList);

      StructuredRecord.Builder outputBuilder = StructuredRecord.builder(outputSchema);

      fieldList.forEach(
          field -> {
            String schema = field.getSchema().getType().name();
            if (field.getSchema().getLogicalType() != null) {
              schema = field.getSchema().getLogicalType().name();
            }
            String value = csvRecord.get(field.getName());
            if (schema.equalsIgnoreCase("INT")) {
              outputBuilder.set(field.getName(), Integer.valueOf(value));
            }
            if (schema.equalsIgnoreCase("LONG")) {
              outputBuilder.set(field.getName(), Integer.valueOf(value));
            }
            if (schema.equalsIgnoreCase("DATE")) {
              outputBuilder.set(field.getName(), Date.valueOf(value));
            }
            if (schema.equalsIgnoreCase("STRING")) {
              outputBuilder.set(field.getName(), value);
            }
            if (schema.equalsIgnoreCase("BOOLEAN")) {
              outputBuilder.set(field.getName(), Boolean.valueOf(value));
            }
            if (schema.equalsIgnoreCase("BYTES")) {
              outputBuilder.set(field.getName(), value.getBytes());
            }
            if (schema.equalsIgnoreCase("DOUBLE")) {
              outputBuilder.set(field.getName(), Double.valueOf(value));
            }
            if (schema.equalsIgnoreCase("DECIMAL")) {
              outputBuilder.setDecimal(field.getName(), new BigDecimal(value));
            }
            if (schema.equalsIgnoreCase("FLOAT")) {
              outputBuilder.set(field.getName(), Float.valueOf(value));
            }
            if (schema.equalsIgnoreCase("TIME_MICROS")) {
              outputBuilder.set(field.getName(), Time.valueOf(value));
            }
            if (schema.equalsIgnoreCase("TIMESTAMP_MICROS")) {
              outputBuilder.set(field.getName(), Timestamp.valueOf(value));
            }
          });
      structuredRecord = outputBuilder.build();
    } catch (Exception e) {
      LOG.error(" Error parsing CSV data {} ", e);
    }

    return structuredRecord;
  }

  protected void setDefaultConf(Configuration conf) {
    FileInputFormat.setSourcePaths(conf, config.sourcePaths);
    FileInputFormat.setMaxSplitSize(conf, config.maxSplitSize);
    FileInputFormat.setRecursiveCopy(conf, config.recursiveCopy.toString());
  }
}
