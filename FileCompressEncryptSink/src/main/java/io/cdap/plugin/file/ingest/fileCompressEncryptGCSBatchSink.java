package io.cdap.plugin.file.ingest;


import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;


@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(fileCompressEncryptGCSBatchSink.NAME)
@Description("Writes to a FileSet in text format.")
public class fileCompressEncryptGCSBatchSink extends BatchSink<StructuredRecord, NullWritable, FileMetadata> {

    private static final Logger LOG = LoggerFactory.getLogger(fileCompressEncryptGCSBatchSink.class);
    public static final String NAME = "fileCompressEncryptGCSBatchSink";
    private final fileCompressEncryptGCSBatchSinkConfig config;

    public fileCompressEncryptGCSBatchSink(fileCompressEncryptGCSBatchSinkConfig config) {
        this.config = config;
    }

    // configurePipeline is called exactly once when the pipeline is being created.
    // Any static configuration should be performed here.
    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        super.configurePipeline(pipelineConfigurer);
    }

    // prepareRun is called before every pipeline run, and is used to configure what the input should be,
    // as well as any arguments the input should use. It is called by the client that is submitting the batch job.
    @Override
    public void prepareRun(BatchSinkContext context) throws Exception {
    }

    // onRunFinish is called at the end of the pipeline run by the client that submitted the batch job.
    @Override
    public void onRunFinish(boolean succeeded, BatchSinkContext context) {
        // perform any actions that should happen at the end of the run.
    }

    // initialize is called by each job executor before any call to transform is made.
    // This occurs at the start of the batch job run, after the job has been successfully submitted.
    // For example, if mapreduce is the execution engine, each mapper will call initialize at the start of the program.
    @Override
    public void initialize(BatchRuntimeContext context) throws Exception {
        super.initialize(context);
    }

    // destroy is called by each job executor at the end of its life.
    // For example, if mapreduce is the execution engine, each mapper will call destroy at the end of the program.
    @Override
    public void destroy() {
        // clean up any resources created by initialize
    }

    @Override
    public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, FileMetadata>> emitter) throws Exception {
        FileMetadata output;
        output = new FileMetadata(input);
        emitter.emit(new KeyValue<NullWritable, FileMetadata>(null, output));
    }


    /**
     * Adds necessary configuration resources and provides OutputFormat Class
     */
    public class FileCopyOutputFormatProvider implements OutputFormatProvider {
        protected final Map<String, String> conf;

        public FileCopyOutputFormatProvider(fileCompressEncryptGCSBatchSinkConfig config) {
            this.conf = new HashMap<>();
          //  FileCopyOutputFormat.setBasePath(conf, config.);
           // FileCopyOutputFormat.setFilesystemScheme(conf, config.getScheme());
        }

        @Override
        public Map<String, String> getOutputFormatConfiguration() {
            return conf;
        }

        @Override
        public String getOutputFormatClassName() {
            return FileCopyOutputFormat.class.getName();
        }
    }




}

