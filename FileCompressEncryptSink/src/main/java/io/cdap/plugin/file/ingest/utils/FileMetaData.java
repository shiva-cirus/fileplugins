package io.cdap.plugin.file.ingest.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * @author Vikas K  Created On 14/11/19
 **/
public class FileMetaData {
    private static final Logger LOG = LoggerFactory.getLogger(FileMetaData.class);
    Path path;
    FileSystem fileSystem;
    String filePath;
    String uri;

    private FileMetaData() {
    }

    public FileMetaData(String filePath, String uri, Configuration conf) {
        this.filePath = filePath;
        this.uri = uri;
        if (uri.startsWith("hdfs")) {
            String strPath = uri + filePath;
            LOG.debug("FileMetaData::hdfs:: {}", strPath);
            this.path = new Path(strPath);
            LOG.debug("FileMetaData::hdfs:: path.toString() = {}", getPath().toString());
            try {
                this.fileSystem = FileSystem.get(URI.create(uri), conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            String strPath = uri + '/' + filePath;
            LOG.debug("FileMetaData::else:: {}", strPath);
            this.path = new Path(strPath);
            LOG.debug("FileMetaData::else:: path.toString() = {}", getPath().toString());
            try {
                this.fileSystem = path.getFileSystem(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
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
            return fileSystem.getFileStatus(new Path(filePath)).getModificationTime();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
