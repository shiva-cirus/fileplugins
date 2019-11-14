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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.file.ingest.FileMetadata;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Filemetadata specific for S3. Defines credentials that are required for
 * connecting to S3.
 */
public class GCSFileMetadata extends FileMetadata {

  public static final String PROJECTID = "projectid";
  public static final String SERVICEACCOUNTJSON = "serviceaccountjson";
  public static final Schema CREDENTIAL_SCHEMA = Schema.recordOf(
    "metadata",
    Schema.Field.of(PROJECTID, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(SERVICEACCOUNTJSON, Schema.of(Schema.Type.STRING))
  );

  private final String projectid;
  private final String serviceaccountjson;

  private static final Logger LOG = LoggerFactory.getLogger(GCSFileMetadata.class);

  public GCSFileMetadata(FileStatus fileStatus, String sourcePath,
                         String projectid, String serviceaccountjson) throws IOException {
    super(fileStatus, sourcePath);
    this.projectid = projectid;
    this.serviceaccountjson = serviceaccountjson;
  }

  public GCSFileMetadata(StructuredRecord record) {
    super(record);
    this.projectid = record.get(PROJECTID);
    this.serviceaccountjson = record.get(SERVICEACCOUNTJSON);
  }

  public GCSFileMetadata(DataInput input) throws IOException {
    super(input);
    this.projectid = input.readUTF();
    this.serviceaccountjson = input.readUTF();
  }

  public String getAccessKeyId() {
    return projectid;
  }

  public String getSecretKeyId() {
    return serviceaccountjson;
  }

  @Override
  protected Schema getCredentialSchema() {
    return CREDENTIAL_SCHEMA;
  }

  @Override
  protected void addCredentialsToRecordBuilder(StructuredRecord.Builder builder) {
    builder
      .set(PROJECTID, projectid)
      .set(SERVICEACCOUNTJSON, serviceaccountjson);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    super.write(dataOutput);
    dataOutput.writeUTF(projectid);
    dataOutput.writeUTF(serviceaccountjson);
  }
}
