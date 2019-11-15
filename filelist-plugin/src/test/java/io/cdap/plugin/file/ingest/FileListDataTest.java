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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class FileListDataTest {
    @Test
    public void testRelativePathParsing() throws IOException {
        FileStatus fileStatus = new FileStatus();
        fileStatus.setPath(new Path("hdfs://12.34.56.78/source/path/directory/123.txt"));

        // Copy a file that is part of a whole directory copy
        String sourcePath = "/source/path/directory";
        FileListData metadata = new FileListData(fileStatus, sourcePath);
        Assert.assertEquals("123.txt", metadata.getFileName());
        Assert.assertEquals("/source/path/directory/123.txt", metadata.getFullPath());

        // Copy a file that is part of a whole directory copy without including the directory
        sourcePath = "/source/path/";
        metadata = new FileListData(fileStatus, sourcePath);
        Assert.assertEquals("123.txt", metadata.getFileName());
        Assert.assertEquals("/source/path/directory/123.txt", metadata.getFullPath());

        fileStatus.setPath(new Path("hdfs://12.34.56.78/"));
        sourcePath = "/";
        metadata = new FileListData(fileStatus, sourcePath);

        fileStatus.setPath(new Path("hdfs://12.34.56.78/abc.txt"));
        sourcePath = "/";
        metadata = new FileListData(fileStatus, sourcePath);
    }

    @Test
    public void testCompare() throws IOException {
        final FileStatus statusA = new FileStatus(1, false, 0, 0, 0, new Path("s3a://hello.com/abc/fileA"));
        final FileStatus statusB = new FileStatus(3, false, 0, 0, 0, new Path("s3a://hello.com/abc/fileB"));
        final FileStatus statusC = new FileStatus(3, false, 0, 0, 0, new Path("s3a://hello.com/abc/fileC"));
        final String basePath = "/abc";

        // generate 3 files with different file sizes
        FileListData file1 = new FileListData(statusA, basePath);

        FileListData file2 = new FileListData(statusB, basePath);

        FileListData file3 = new FileListData(statusC, basePath);

        Assert.assertEquals(-1, file1.compareTo(file2));
        Assert.assertEquals(0, file3.compareTo(file2));
        Assert.assertEquals(1, file3.compareTo(file1));
    }
}