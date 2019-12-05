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

/**
 * Abstract class that contains file metadata fields. Extend from this class to add credentials
 * specific to different filesystems.
 */
public class FileMetaData implements Comparable<FileMetaData> {

  public static final String FILE_SIZE = "fileSize";
  public static final String FULL_PATH = "fullPath";
  public static final String IS_DIR = "isDir";
  public static final String RELATIVE_PATH = "relativePath";
  public static final String HOST_URI = "hostURI";

  // full path of the file in the source filesystem
  private final String fullPath;

  // file size
  private final long fileSize;

  // whether or not the file is a directory
  private final boolean isDir;

  /*
   * The relavite path is constructed by deleting the portion of the source
   * path that comes before the last path separator ("/") from the full path.
   * It is assumed here that the source path is always a prefix of the full
   * path.
   *
   * For example, given full path http://example.com/foo/bar/baz/index.html
   *              and source path /foo/bar
   *              the relative path will be bar/baz/index.html
   */
  private final String relativePath;

  /*
   * URI for the Filesystem
   * For instance, the hostURI for http://abc.def.ghi/new/index.html is http://abc.def.ghi
   */
  private final String hostURI;

  public String getRelativePath() {
    return relativePath;
  }

  public String getHostURI() {
    return hostURI;
  }

  /**
   * Constructs a FileMetaData instance given a FileStatus and source path. Override this method to
   * add additional credential fields to the instance.
   *
   * @param fileStatus The FileStatus object that contains raw file metadata for this object.
   * @param sourcePath The user specified path that was used to obtain this file.
   * @throws IOException
   */
  public FileMetaData(FileStatus fileStatus, String sourcePath) throws IOException {
    fullPath = fileStatus.getPath().toUri().getPath();
    isDir = fileStatus.isDirectory();
    fileSize = fileStatus.getLen();
    // check if sourcePath is a valid prefix of fullPath
    if (fullPath.startsWith(sourcePath)) {
      relativePath = fullPath.substring(sourcePath.lastIndexOf(Path.SEPARATOR) + 1);
    } else {
      throw new IOException("sourcePath should be a valid prefix of fullPath");
    }

    // construct host URI given the full path from filestatus
    try {
      hostURI =
          new URI(
                  fileStatus.getPath().toUri().getScheme(),
                  fileStatus.getPath().toUri().getHost(),
                  Path.SEPARATOR,
                  null)
              .toString();
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  /**
   * Use this constructor to construct a FileMetaData from a StructuredRecord. Override this method
   * if additional credentials are contained in the structured record.
   *
   * @param record The StructuredRecord instance to convert from.
   */
  public FileMetaData(StructuredRecord record) {
    this.fullPath = record.get(FULL_PATH);
    this.fileSize = record.get(FILE_SIZE);
    this.isDir = record.get(IS_DIR);
    this.relativePath = record.get(RELATIVE_PATH);
    this.hostURI = record.get(HOST_URI);
  }

  /**
   * Use this constructor to deserialize from an input stream.
   *
   * @param dataInput The input stream to deserialize from.
   */
  public FileMetaData(DataInput dataInput) throws IOException {
    this.fullPath = dataInput.readUTF();
    this.fileSize = dataInput.readLong();
    this.isDir = dataInput.readBoolean();
    this.relativePath = dataInput.readUTF();
    this.hostURI = dataInput.readUTF();
  }

  public String getFullPath() {
    return fullPath;
  }

  public boolean isDir() {
    return isDir;
  }

  public long getFileSize() {
    return fileSize;
  }
  /**
   * Compares the size of two files
   *
   * @param o The other file to compare to
   * @return 1 if this instance is larger than the other file. 0 if this instance has the same size
   *     as the other file. -1 if this instance is smaller than the other file.
   */
  @Override
  public int compareTo(FileMetaData o) {
    return Long.compare(fileSize, o.getFileSize());
  }

  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(getFullPath());
    dataOutput.writeLong(getFileSize());
    dataOutput.writeBoolean(isDir());
    dataOutput.writeUTF(getRelativePath());
    dataOutput.writeUTF(getHostURI());
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
