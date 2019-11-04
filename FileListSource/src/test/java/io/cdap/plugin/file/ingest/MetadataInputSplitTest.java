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

public class MetadataInputSplitTest {

    @Test
    public void testCompare() throws IOException {
        MetadataInputSplit metadataInputSplita = new MetadataInputSplit();
        MetadataInputSplit metadataInputSplitb = new MetadataInputSplit();
        MetadataInputSplit metadataInputSplitc = new MetadataInputSplit();

        final FileStatus statusA = new FileStatus(1, false, 0, 0, 0, new Path("hdfs://hello.com/abc/fileA"));
        final FileStatus statusB = new FileStatus(2, false, 0, 0, 0, new Path("hdfs://hello.com/abc/fileB"));
        final FileStatus statusC = new FileStatus(3, false, 0, 0, 0, new Path("hdfs://hello.com/abc/fileC"));
        final String basePath = "/abc";

        // generate 3 files with different file sizes
        FileMetadata file1 = new FileMetadata(statusA, basePath);

        FileMetadata file2 = new FileMetadata(statusB, basePath);

        FileMetadata file3 = new FileMetadata(statusC, basePath);

        // a has 3 bytes
        metadataInputSplita.addFileMetadata(file1);
        metadataInputSplita.addFileMetadata(file2);

        // b has 3 bytes too
        metadataInputSplitb.addFileMetadata(file3);

        // c only has 1 byte
        metadataInputSplitc.addFileMetadata(file1);

        Assert.assertEquals(0, metadataInputSplita.compareTo(metadataInputSplitb));
        Assert.assertEquals(1, metadataInputSplita.compareTo(metadataInputSplitc));
        Assert.assertEquals(-1, metadataInputSplitc.compareTo(metadataInputSplita));
    }
}
