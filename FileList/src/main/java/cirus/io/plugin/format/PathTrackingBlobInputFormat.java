/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package cirus.io.plugin.format;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

/** Blob input format */
public class PathTrackingBlobInputFormat extends PathTrackingInputFormat {

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    ClassLoader cl = job.getConfiguration().getClassLoader();
    job.getConfiguration().setClassLoader(getClass().getClassLoader());
    try {
      return super.getSplits(job);
    } finally {
      job.getConfiguration().setClassLoader(cl);
    }
  }

  @Override
  protected RecordReader<String, StructuredRecord.Builder> createRecordReader(
      FileSplit split,
      TaskAttemptContext context,
      @Nullable String pathField,
      @Nullable Schema schema) {
    if (split.getLength() > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Blob format cannot be used with files larger than 2GB");
    }
    return new RecordReader<String, StructuredRecord.Builder>() {
      boolean hasNext;
      String val;

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) {
        hasNext = true;
        val = null;
      }

      @Override
      public boolean nextKeyValue() throws IOException {
        if (!hasNext) {
          return false;
        }
        hasNext = false;
        if (split.getLength() == 0) {
          return false;
        }

        Path path = split.getPath();
        val = path.toUri().getPath();

        return true;
      }

      @Override
      public String getCurrentKey() {
        return val;
      }

      @Override
      public StructuredRecord.Builder getCurrentValue() {
        String fieldName = schema.getFields().iterator().next().getName();
        return StructuredRecord.builder(schema).set(fieldName, val);
      }

      @Override
      public float getProgress() {
        return 0.0f;
      }

      @Override
      public void close() {
        // no-op
      }
    };
  }
}
