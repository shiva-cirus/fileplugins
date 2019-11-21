package io.cdap.plugin.file.ingest.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @author Vikas K  Created On 14/11/19
 **/
public class FileMetaData {
    Path path;
    FileSystem fileSystem;

    private FileMetaData() {
    }

    public FileMetaData(String filePath, Configuration conf) {
        this.path = new Path(filePath);

        try {
            this.fileSystem = path.getFileSystem(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Path getPath() {
        return path;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public long getLastModifiedTime() {
        try {
            return fileSystem.getFileStatus(path).getModificationTime();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
