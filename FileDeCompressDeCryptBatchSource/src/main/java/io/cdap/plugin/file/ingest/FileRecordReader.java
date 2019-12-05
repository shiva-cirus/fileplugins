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

import io.cdap.plugin.file.ingest.util.FileUtil;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bouncycastle.openpgp.PGPException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.NoSuchProviderException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/** Returns key that contains file path. Returns value that contains file metadata. */
public class FileRecordReader extends RecordReader<Long, CSVRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(FileRecordReader.class);
  protected FileInputSplit split;
  private long rowIdx;
  private InputStream inputFileStream;
  private InputStreamReader isReader;
  private BufferedReader reader;

  // Specifies all the rows of an Excel spreadsheet - An iterator over all the rows.
  private Iterator<CSVRecord> rows;

  public FileRecordReader() {
    super();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    rowIdx++;
    return rows.hasNext();
  }

  @Override
  public Long getCurrentKey() throws IOException, InterruptedException {
    return rowIdx;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return rowIdx / rowIdx;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    this.split = (FileInputSplit) inputSplit;
    Configuration configuration = taskAttemptContext.getConfiguration();
    String filePath = this.split.getFileMetaDataList().get(0).getFullPath();
    String hostURI = this.split.getFileMetaDataList().get(0).getHostURI();
    String privateKeyFilePath = configuration.get("privateKeyFilePath");
    char[] privateKeyPassword = configuration.get("password").toCharArray(); // "passphrase";
    Boolean decrypt = configuration.getBoolean("decrypt", false);
    Boolean decompress = configuration.getBoolean("decompress", false);
    try {
      if (decrypt) {
        if (decompress) {
          inputFileStream =
              FileUtil.decryptAndDecompress(filePath, privateKeyFilePath, privateKeyPassword,hostURI,configuration);
        } else {
          inputFileStream = FileUtil.decrypt(filePath, privateKeyFilePath, privateKeyPassword,hostURI,configuration);
        }
      } else {
        if (decompress) {
          ZipFile zf = new ZipFile(filePath);
          Enumeration entries = zf.entries();
          ZipEntry ze = (ZipEntry) entries.nextElement();
          inputFileStream = zf.getInputStream(ze);
        } else {
          inputFileStream = FileUtil.getFSInputStream(filePath,hostURI,configuration);
        }
      }

      isReader = new InputStreamReader(inputFileStream);
      // Creating a BufferedReader object
      reader = new BufferedReader(isReader);

      CSVParser csvParser =
          new CSVParser(
              reader,
              CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim());

      rows = csvParser.iterator();
      rowIdx = 0;

    } catch (IOException e) {
      e.printStackTrace();
    } catch (NoSuchProviderException e) {
      e.printStackTrace();
    } catch (PGPException e) {
      e.printStackTrace();
    }
  }



  @Override
  public CSVRecord getCurrentValue() throws IOException, InterruptedException {
    return rows.next();
  }

  @Override
  public void close() throws IOException {
    if (inputFileStream != null) {
      inputFileStream.close();
    }
    if (isReader != null) {
      isReader.close();
    }
    if (reader != null) {
      reader.close();
    }
  }
}
