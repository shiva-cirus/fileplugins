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
import io.cdap.plugin.file.ingest.AbstractFileListSource;
import io.cdap.plugin.file.ingest.FileListData;
import io.cdap.plugin.file.ingest.FileListInputFormat;
import io.cdap.plugin.file.ingest.util.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.bouncycastle.openpgp.PGPException;

import java.io.*;
import java.net.URI;
import java.security.NoSuchProviderException;
import java.util.ArrayList;
import java.util.List;

/** FileCopySource plugin that pulls filemetadata from local filesystem or local HDFS. */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("FileDeCompressDeCryptSource")
@Description("Reads file metadata from local filesystem or local HDFS.")
public class FileDeCompressDeCryptSource extends AbstractFileListSource<FileListData> {

  private FileMetadataSourceConfig config;

  public FileDeCompressDeCryptSource(FileMetadataSourceConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    List<Schema.Field> fieldList = new ArrayList<>(FileListData.DEFAULT_SCHEMA.getFields());
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
        FileListInputFormat.setURI(
            conf, new URI(config.scheme, null, Path.SEPARATOR, null).toString());
        break;
      case "hdfs":
        break;
      default:
        throw new IllegalArgumentException("Scheme must be either file or hdfs.");
    }

    context.setInput(
        Input.of(
            config.referenceName, new SourceInputFormatProvider(FileListInputFormat.class, conf)));
  }

  /**
   * Converts the input FileListData to a StructuredRecord and emits it.
   *
   * @param input The input FileListData.
   * @param emitter Emits StructuredRecord that contains FileListData.
   */
  @Override
  public void transform(
      KeyValue<NullWritable, FileListData> input, Emitter<StructuredRecord> emitter) {
    String filePath = input.getValue().getFullPath();
    String keyFileName = config.privateKeyFilePath;
    char[] privateKeyPassword = config.password.toCharArray(); // "passphrase";
    Boolean decrypt = config.decrypt;
    Boolean decompress = config.decompress;
    InputStream inputFileStream = null;
    try {
      if (decrypt) {
        if (decompress) {
          inputFileStream =
              FileUtil.decryptAndDecompress(filePath, keyFileName, privateKeyPassword);
        } else {
          inputFileStream = FileUtil.decrypt(filePath, keyFileName, privateKeyPassword);
        }
      } else {
        inputFileStream = new FileInputStream(filePath);
      }

      InputStreamReader isReader = new InputStreamReader(inputFileStream);
      // Creating a BufferedReader object
      BufferedReader reader = new BufferedReader(isReader);

      String line = reader.readLine();

      while (line != null) {
        // read next line
        emitter.emit(toRecord(line));
        line = reader.readLine();
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
  public StructuredRecord toRecord(String line) {

    // merge default schema and credential schema to create output schema
    Schema outputSchema;
    List<Schema.Field> fieldList = new ArrayList<>(FileListData.DEFAULT_SCHEMA.getFields());
    outputSchema = Schema.recordOf("metadata", fieldList);

    StructuredRecord.Builder outputBuilder =
        StructuredRecord.builder(outputSchema).set(FileListData.BODY, line);

    return outputBuilder.build();
  }
  /** Configurations required for connecting to local filesystems. */
  public class FileMetadataSourceConfig extends AbstractFileMetadataSourceConfig {

    @Description("Scheme of the source filesystem.")
    public String scheme;

    @Description("Private key File Path.")
    public String privateKeyFilePath;

    @Description("Password of the private key")
    public String password;

    @Description("File to be Decypted or not")
    public Boolean decrypt;

    @Description("File to be decompress or not")
    public Boolean decompress;

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
