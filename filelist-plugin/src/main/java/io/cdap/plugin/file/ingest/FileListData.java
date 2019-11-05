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

package io.cdap.plugin.file.ingest;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract class that contains file metadata fields. Extend from this class to add credentials
 * specific to different filesystems.
 */
public class FileListData implements Comparable<FileListData> {

  public static final String FILE_NAME = "fileName";
  public static final String FILE_SIZE = "fileSize";
  public static final String FULL_PATH = "fullPath";
  public static final String IS_DIR = "isDir";

  // The default schema that will be used to convert this object to a StructuredRecord.
  public static final Schema DEFAULT_SCHEMA =
      Schema.recordOf(
          "metadata",
          Schema.Field.of(FILE_NAME, Schema.of(Schema.Type.STRING)),
          Schema.Field.of(FULL_PATH, Schema.of(Schema.Type.STRING)));

  // contains only the name of the file
  private final String fileName;

  // full path of the file in the source filesystem
  private final String fullPath;

  // file size
  private final long fileSize;

  // whether or not the file is a directory
  private final boolean isDir;

  /**
   * Constructs a FileListData instance given a FileStatus and source path. Override this method to
   * add additional credential fields to the instance.
   *
   * @param fileStatus The FileStatus object that contains raw file metadata for this object.
   * @param sourcePath The user specified path that was used to obtain this file.
   * @throws IOException
   */
  public FileListData(FileStatus fileStatus, String sourcePath) throws IOException {
    fileName = fileStatus.getPath().getName();
    fullPath = fileStatus.getPath().toUri().getPath();
    isDir = fileStatus.isDirectory();
    fileSize = fileStatus.getLen();
  }

  /**
   * Use this constructor to construct a FileListData from a StructuredRecord. Override this method
   * if additional credentials are contained in the structured record.
   *
   * @param record The StructuredRecord instance to convert from.
   */
  public FileListData(StructuredRecord record) {
    this.fileName = record.get(FILE_NAME);
    this.fullPath = record.get(FULL_PATH);
    this.fileSize = record.get(FILE_SIZE);
    this.isDir = record.get(IS_DIR);
  }

  /**
   * Use this constructor to deserialize from an input stream.
   *
   * @param dataInput The input stream to deserialize from.
   */
  public FileListData(DataInput dataInput) throws IOException {
    this.fileName = dataInput.readUTF();
    this.fullPath = dataInput.readUTF();
    this.fileSize = dataInput.readLong();
    this.isDir = dataInput.readBoolean();
  }

  public String getFullPath() {
    return fullPath;
  }

  public String getFileName() {
    return fileName;
  }

  public long getFileSize() {
    return fileSize;
  }



  public boolean isDir() {
    return isDir;
  }


  /** Converts to a StructuredRecord */
  public StructuredRecord toRecord() {
    // initialize credential schema
    List<Schema.Field> credentialSchemaList;
    if (getCredentialSchema() == null) {
      credentialSchemaList = new ArrayList<>();
    } else {
      credentialSchemaList = getCredentialSchema().getFields();
    }

    // merge default schema and credential schema to create output schema
    Schema outputSchema;
    List<Schema.Field> fieldList = new ArrayList<>(DEFAULT_SCHEMA.getFields());
    fieldList.addAll(credentialSchemaList);
    outputSchema = Schema.recordOf("metadata", fieldList);

    StructuredRecord.Builder outputBuilder =
        StructuredRecord.builder(outputSchema)
            .set(FILE_NAME, fileName)
            .set(FULL_PATH, fullPath);
    addCredentialsToRecordBuilder(outputBuilder);

    return outputBuilder.build();
  }

  /**
   * Compares the size of two files
   *
   * @param o The other file to compare to
   * @return 1 if this instance is larger than the other file. 0 if this instance has the same size
   *     as the other file. -1 if this instance is smaller than the other file.
   */
  @Override
  public int compareTo(FileListData o) {
    return Long.compare(fileSize, o.getFileSize());
  }

  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(getFileName());
    dataOutput.writeUTF(getFullPath());
    dataOutput.writeLong(getFileSize());
    dataOutput.writeBoolean(isDir());
  }

  /**
   * Override this in extended class to return credential schema for different filesystems.
   *
   * @return Credential schema for different filesystems
   */
  protected Schema getCredentialSchema() {
    return null;
  }

  /**
   * Override this in extended class to add credential information to StructuredRecord.
   *
   * @param builder
   */
  protected void addCredentialsToRecordBuilder(StructuredRecord.Builder builder) {
    // no op
  }
}
