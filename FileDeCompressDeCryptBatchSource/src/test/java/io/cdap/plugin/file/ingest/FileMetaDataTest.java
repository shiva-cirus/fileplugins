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

public class FileMetaDataTest {
    @Test
    public void testRelativePathParsing() throws IOException {
        FileStatus fileStatus = new FileStatus();
        fileStatus.setPath(new Path("hdfs://12.34.56.78/source/path/directory/123.txt"));

        // Copy a file that is part of a whole directory copy
        String sourcePath = "/source/path/directory";
        FileMetaData metadata = new FileMetaData(fileStatus, sourcePath);
        Assert.assertEquals("/source/path/directory/123.txt", metadata.getFullPath());

        // Copy a file that is part of a whole directory copy without including the directory
        sourcePath = "/source/path/";
        metadata = new FileMetaData(fileStatus, sourcePath);
        Assert.assertEquals("/source/path/directory/123.txt", metadata.getFullPath());

        fileStatus.setPath(new Path("hdfs://12.34.56.78/"));
        sourcePath = "/";
        metadata = new FileMetaData(fileStatus, sourcePath);

        fileStatus.setPath(new Path("hdfs://12.34.56.78/abc.txt"));
        sourcePath = "/";
        metadata = new FileMetaData(fileStatus, sourcePath);
    }


}