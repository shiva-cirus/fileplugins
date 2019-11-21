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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract class that implements information for InputSplit.
 * Contains a list of fileMetadata that is assigned to the specific split.
 */
public class FileListInputSplit extends InputSplit implements Writable, Comparable {
    private static final Logger LOG = LoggerFactory.getLogger(FileListInputSplit.class);
    private List<FileListData> fileMetaDataList;
    private long totalBytes;

    public FileListInputSplit() {
        this.fileMetaDataList = new ArrayList<>();
        this.totalBytes = 0;
    }

    public List<FileListData> getFileMetaDataList() {
        return this.fileMetaDataList;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        try {
            // write number of files
            dataOutput.writeLong(this.getLength());

            for (FileListData fileMetaData : fileMetaDataList) {
                // convert each filestatus (serializable) to byte array
                fileMetaData.write(dataOutput);
            }

        } catch (InterruptedException interruptedException) {
            throw new IOException("Failed to get length for InputSplit");
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        // read number of files
        long numObjects = dataInput.readLong();

        fileMetaDataList = new ArrayList<>();
        for (long i = 0; i < numObjects; i++) {
            FileListData metadata = readFileMetaData(dataInput);
            addFileMetadata(metadata);
        }
    }

    /**
     * @return the number of files in this split
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public long getLength() throws IOException, InterruptedException {
        return fileMetaDataList.size();
    }

    /**
     * @return the total number of file bytes in this split
     */
    public long getTotalBytes() {
        return this.totalBytes;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    /**
     * Adds a new file to the split.
     *
     * @param fileMetaData The file to be added.
     */
    public void addFileMetadata(FileListData fileMetaData) {
        fileMetaDataList.add(fileMetaData);
        totalBytes += fileMetaData.getFileSize();
    }

    /**
     * Compares the total number of bytes contained in the split
     */
    @Override
    public int compareTo(Object o) {
        return Long.compare(getTotalBytes(), ((FileListInputSplit) o).getTotalBytes());
    }

    /**
     * This function deserializes FileListData from an input stream. Override this function if the metadata class
     * specific to the filesystem has its own deserialization method.
     *
     * @param dataInput The input stream we wish to deserialize from.
     * @return Deserialized FileListData.
     * @throws IOException
     */
    protected FileListData readFileMetaData(DataInput dataInput) throws IOException {
        return new FileListData(dataInput);
    }
}

