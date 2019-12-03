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

package io.cdap.plugin.batchsink;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Class for the OutputFormat that FileCopySink uses.
 */
public class FileAnonymizedOutputFormat extends OutputFormat {
    public static final String NAME_GCS_PROJECTID = "gcs.projectid";
    public static final String NAME_GCS_SERVICEACCOUNTJSON = "gcs.service.account";
    public static final String NAME_GCS_BUCKET = "gcs.bucket";
    public static final String NAME_GCS_DESTPATH = "gcs.bucket.path";
    public static final String NAME_GCS_DESTPATH_SUFFIX = "gcs.bucket.path.suffix";
    public static final String NAME_BUFFER_SIZE = "buffer.size";
    public static final String NAME_POLICY_URL = "policy.url";
    public static final String NAME_IDENTITY = "identity";
    public static final String NAME_SHARED_SECRET = "shared.secret";
    public static final String NAME_TRUST_STORE_PATH = "trust.store.path";
    public static final String NAME_CACHE_PATH = "cache.path";
    public static final String NAME_FILE_FORMAT = "file.format";
    public static final String NAME_IGNORE_HEADER = "ignore.header";
    public static final String NAME_FIELD_LIST = "field.list";
    public static final String NAME_PROXY = "proxy";
    public static final String NAME_PROXY_TYPE = "proxytype";

    private static final Logger LOG = LoggerFactory.getLogger(FileAnonymizedOutputFormat.class);

    public static void setGCSProjectID(Map<String, String> conf, String value) {
        conf.put(NAME_GCS_PROJECTID, value);
    }

    public static void setGCSServiceAccount(Map<String, String> conf, String value) {
        conf.put(NAME_GCS_SERVICEACCOUNTJSON, value);
    }

    public static void setGCSBucket(Map<String, String> conf, String value) {
        conf.put(NAME_GCS_BUCKET, value);
    }

    public static void setGCSDestPath(Map<String, String> conf, String value) {
        conf.put(NAME_GCS_DESTPATH, value);
    }

    public static void setGCSDestPathSuffix(Map<String, String> conf, String value) {
        conf.put(NAME_GCS_DESTPATH_SUFFIX, value == null ? "" : value);
    }

    public static void setBufferSize(Map<String, String> conf, String value) {
        conf.put(NAME_BUFFER_SIZE, value == null ? "" : value);
    }

    public static void setPolicyUrl(Map<String, String> conf, String value) {
        conf.put(NAME_POLICY_URL, value);
    }

    public static void setIdentity(Map<String, String> conf, String value) {
        conf.put(NAME_IDENTITY, value);
    }

    public static void setSharedSecret(Map<String, String> conf, String value) {
        conf.put(NAME_SHARED_SECRET, value);
    }

    public static void setTrustStorePath(Map<String, String> conf, String value) {
        conf.put(NAME_TRUST_STORE_PATH, value);
    }

    public static void setCachePath(Map<String, String> conf, String value) {
        conf.put(NAME_CACHE_PATH, value);
    }

    public static void setFormat(Map<String, String> conf, String value) {
        conf.put(NAME_FILE_FORMAT, value);
    }

    public static void setIgnoreHeader(Map<String, String> conf, String value) {
        conf.put(NAME_IGNORE_HEADER, value);
    }

    public static void setFieldList(Map<String, String> conf, String value) {
        conf.put(NAME_FIELD_LIST, value == null ? "" : value);
    }

    public static void setProxy(Map<String, String> conf, String value) {
        conf.put(NAME_PROXY, value == null ? "" : value);
    }

    public static void setProxyType(Map<String, String> conf, String value) {
        conf.put(NAME_PROXY_TYPE, value == null ? "" : value);
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
        return new FileAnonymizedRecordWriter(conf);
    }
}
