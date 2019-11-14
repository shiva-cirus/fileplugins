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

package io.cdap.plugin.file.ingest.batchsink;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Class for the OutputFormat that FileCopySink uses.
 */
public class FileCopyOutputFormat extends OutputFormat {

    public static final String NAME_FILECOMPRESSION = "file.compression";
    public static final String NAME_FILEENCRYPTION = "file.encryption";
    public static final String NAME_GCS_PROJECTID = "gcs.projectid";
    public static final String NAME_GCS_SERVICEACCOUNTJSON = "gcs.service.account";
    public static final String NAME_GCS_BUCKET = "gcs.bucket";
    public static final String NAME_GCS_DESTPATH = "gcs.bucket.path";
    public static final String NAME_GCS_DESTPATH_SUFFIX = "gcs.bucket.path.suffix";
    public static final String NAME_PGP_PUBKEY = "file.pgp.pub.key";



    public static final String FS_SCHEME = "filesystem.scheme";

    private static final Logger LOG = LoggerFactory.getLogger(FileCopyOutputFormat.class);




    public static void setCompression(Map<String, String> conf,  String value) {
        conf.put(NAME_FILECOMPRESSION, value);
    }


    public static void setEncryption(Map<String, String> conf,  String value) {
        conf.put(NAME_FILEENCRYPTION, value);
    }

    public static void setGCSProjectID(Map<String, String> conf,  String value) {
        conf.put(NAME_GCS_PROJECTID, value);
    }


    public static void setGCSServiceAccount(Map<String, String> conf,  String value) {
        conf.put(NAME_GCS_SERVICEACCOUNTJSON, value);
    }



    public static void setGCSBucket(Map<String, String> conf,  String value) {
        conf.put(NAME_GCS_BUCKET, value);
    }

    public static void setGCSDestPath(Map<String, String> conf,  String value) {
        conf.put(NAME_GCS_DESTPATH, value);
    }



    public static void setGCSDestPathSuffix(Map<String, String> conf,  String value) {
        conf.put(NAME_GCS_DESTPATH_SUFFIX, value);
    }

    public static void setPGPPubKey(Map<String, String> conf,  String value) {
        conf.put(NAME_PGP_PUBKEY, value);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        // check if base path is set
        // if (jobContext.getConfiguration().get(BASE_PATH, null) == null) {
        //   throw new IOException("Base path not set.");
        //  }
    }



    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) {
        // TODO: implement an OutputCommitter for file copying jobs, or investigate whether we can use FileOutputCommitter
        return new OutputCommitter() {
            @Override
            public void setupJob(JobContext jobContext) throws IOException {
                // no op
            }

            @Override
            public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
                // no op
            }

            @Override
            public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
                return false;
            }

            @Override
            public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
                // no op
            }

            @Override
            public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
                // no op
            }
        };

    }



    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Configuration conf = taskAttemptContext.getConfiguration();
        return new FileCopyRecordWriter(conf);
    }
}
