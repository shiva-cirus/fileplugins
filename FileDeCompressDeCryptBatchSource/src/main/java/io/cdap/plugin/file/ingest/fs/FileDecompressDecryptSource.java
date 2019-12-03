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

import com.google.common.base.Strings;
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
import io.cdap.plugin.file.ingest.AbstractFileDecompressDecryptSource;
import io.cdap.plugin.file.ingest.FileInputFormat;
import io.cdap.plugin.file.ingest.FileMetaData;
import io.cdap.plugin.file.ingest.util.FileUtil;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.bouncycastle.openpgp.PGPException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.*;
import java.net.URI;
import java.security.NoSuchProviderException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

/** FileCopySource plugin that pulls filemetadata from local filesystem or local HDFS. */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("FileDecompressDecryptSource")
@Description("Reads file metadata from local filesystem or local HDFS.")
public class FileDecompressDecryptSource extends AbstractFileDecompressDecryptSource<FileMetaData> {
  private static final Logger LOG = LoggerFactory.getLogger(FileDecompressDecryptSource.class);
  private FileMetadataSourceConfig config;

  public FileDecompressDecryptSource(FileMetadataSourceConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    List<Schema.Field> fieldList = config.getSchema().getFields();
    pipelineConfigurer
        .getStageConfigurer()
        .setOutputSchema(Schema.recordOf("fileSchema", fieldList));
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    super.prepareRun(context);
    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

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
      KeyValue<NullWritable, FileMetaData> input, Emitter<StructuredRecord> emitter) {
    String filePath = input.getValue().getFullPath();
    String privateKeyFilePath = config.privateKeyFilePath;
    char[] privateKeyPassword = config.password.toCharArray(); // "passphrase";
    Boolean decrypt = config.decrypt;
    Boolean decompress = config.decompress;
    InputStream inputFileStream = null;
    try {
      if (decrypt) {
        if (decompress) {
          inputFileStream =
              FileUtil.decryptAndDecompress(filePath, privateKeyFilePath, privateKeyPassword);
        } else {
          inputFileStream = FileUtil.decrypt(filePath, privateKeyFilePath, privateKeyPassword);
        }
      } else {
        inputFileStream = new FileInputStream(filePath);
      }

      InputStreamReader isReader = new InputStreamReader(inputFileStream);
      // Creating a BufferedReader object
      BufferedReader reader = new BufferedReader(isReader);

      CSVParser csvParser =
          new CSVParser(
              reader,
              CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim());
      for (CSVRecord csvRecord : csvParser) {
        emitter.emit(toRecord(csvRecord));
      }

      inputFileStream.close();
      isReader.close();
      reader.close();

    } catch (IOException e) {
      e.printStackTrace();
    } catch (NoSuchProviderException e) {
      e.printStackTrace();
    } catch (PGPException e) {
      e.printStackTrace();
    }
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
            String value = csvRecord.get(field.getName());
            if (schema.equalsIgnoreCase("INT")) {
              outputBuilder.set(field.getName(), Integer.valueOf(value));
            }
            if (schema.equalsIgnoreCase("LONG")) {
              outputBuilder.set(field.getName(), Integer.valueOf(value));
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
              outputBuilder.set(field.getName(), Double.valueOf(value));
            }
            if (schema.equalsIgnoreCase("FLOAT")) {
              outputBuilder.set(field.getName(), Float.valueOf(value));
            }
            if (schema.equalsIgnoreCase("TIME")) {
              outputBuilder.set(field.getName(), Time.valueOf(value));
            }
            if (schema.equalsIgnoreCase("TIMESTAMP")) {
              outputBuilder.set(field.getName(), Timestamp.valueOf(value));
            }
            if (schema.equalsIgnoreCase("TIMESTAMP")) {
              outputBuilder.set(field.getName(), Timestamp.valueOf(value));
            }
          });
      structuredRecord = outputBuilder.build();
    } catch (Exception e) {
      LOG.error(" Error parsing CSV data {} ", e);
    }

    return structuredRecord;
  }
  /** Configurations required for connecting to local filesystems. */
  public class FileMetadataSourceConfig extends AbstractFileMetadataSourceConfig {

    public static final String NAME_SCHEMA = "schema";

    @Description("Scheme of the source filesystem.")
    public String scheme;

    @Macro
    @Description("Private key File Path.")
    public String privateKeyFilePath;

    @Macro
    @Description("Password of the private key")
    public String password;

    @Description("Decompression Format")
    public String decompressionFormat;

    @Description("Decryption Algorithm ")
    public String decryptionAlgorithm;

    @Description("File  Format ")
    public String format;

    @Description("File to be Decrypted or not")
    public Boolean decrypt;

    @Description("File to be decompress or not")
    public Boolean decompress;

    @Nullable
    @Description(
        "Output schema for the source. Formats like 'avro' and 'parquet' require a schema in order to "
            + "read the data.")
    private String schema;

    @Nullable
    public Schema getSchema() {
      try {
        return containsMacro(NAME_SCHEMA) || Strings.isNullOrEmpty(schema)
            ? null
            : Schema.parseJson(schema);
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid schema: " + e.getMessage(), e);
      }
    }

    public FileMetadataSourceConfig(
        String name, String sourcePaths, Integer maxSplitSize, String scheme) {
      super(name, sourcePaths, maxSplitSize);
      this.scheme = scheme;
      this.privateKeyFilePath = privateKeyFilePath;
      this.password = password;
      this.decrypt = decrypt;
      this.decompress = decompress;
    }
  }
}
