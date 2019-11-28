package io.cdap.plugin.batchsink;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.batchsink.common.FileListData;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(FileAnonymizerBatchSink.NAME)
@Description("File Compress / Encrypt and Persist to GCS.")
public class FileAnonymizerBatchSink extends BatchSink<StructuredRecord, NullWritable, FileListData> {
    public static final String NAME = "FileAnonymizerBatchSink";

    private static final Logger LOG = LoggerFactory.getLogger(FileAnonymizerBatchSink.class);

    private final FileAnonymizerBatchSinkConfig config;

    public FileAnonymizerBatchSink(FileAnonymizerBatchSinkConfig config) {
        this.config = config;
    }

    // configurePipeline is called exactly once when the pipeline is being created.
    // Any static configuration should be performed here.
    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        super.configurePipeline(pipelineConfigurer);
        LOG.info("In configurePipeline");
    }

    // prepareRun is called before every pipeline run, and is used to configure what the input should be,
    // as well as any arguments the input should use. It is called by the client that is submitting the batch job.
    @Override
    public void prepareRun(BatchSinkContext context) throws Exception {
        LOG.info("In prepareRun");
        context.addOutput(Output.of("FileCopyOutputFormatProvider", new FileCopyOutputFormatProvider(config)));

        //validation for suffix
        if (StringUtils.isNotEmpty(config.getSuffix())) {
            try {
                DateTimeFormatter.ofPattern(config.getSuffix());
            } catch (Exception e) {
                throw new IllegalArgumentException(String.format("Suffix has a invalid date format for %s plugin. Please correct the same.", NAME));
            }
        }

        //validation for bufferSize
        if (!NumberUtils.isCreatable(config.getBufferSize())) {
            throw new IllegalArgumentException(String.format("Buffer size must be a numeric value for %s plugin. Please provide the same.", NAME));
        }

        //TODO: add validations for fieldList

        LOG.info("prepareRun completed");
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
        LOG.info("In initialize");
    }

    // destroy is called by each job executor at the end of its life.
    // For example, if mapreduce is the execution engine, each mapper will call destroy at the end of the program.
    @Override
    public void destroy() {
        // clean up any resources created by initialize
    }

    @Override
    public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, FileListData>> emitter) throws Exception {
        LOG.info("In transform");
        FileListData output = new FileListData(input);
        emitter.emit(new KeyValue<NullWritable, FileListData>(null, output));
        LOG.info("transform completed");
    }

    /**
     * Adds necessary configuration resources and provides OutputFormat Class
     */
    public class FileCopyOutputFormatProvider implements OutputFormatProvider {
        protected final Map<String, String> conf;

        public FileCopyOutputFormatProvider(FileAnonymizerBatchSinkConfig config) {
            this.conf = new HashMap<>();

            FileAnonymizedOutputFormat.setGCSBucket(conf, config.getBucket());
            FileAnonymizedOutputFormat.setGCSDestPath(conf, config.getDestinationPath());
            FileAnonymizedOutputFormat.setGCSDestPathSuffix(conf, config.getSuffix());
            FileAnonymizedOutputFormat.setGCSProjectID(conf, config.getProject());
            FileAnonymizedOutputFormat.setGCSServiceAccount(conf, config.getServiceFilePath());
            FileAnonymizedOutputFormat.setBufferSize(conf, config.getBufferSize());
            FileAnonymizedOutputFormat.setPolicyUrl(conf, config.getPolicyUrl());
            FileAnonymizedOutputFormat.setIdentity(conf, config.getIdentity());
            FileAnonymizedOutputFormat.setSharedSecret(conf, config.getSharedSecret());
            FileAnonymizedOutputFormat.setTrustStorePath(conf, config.getTrustStorePath());
            FileAnonymizedOutputFormat.setCachePath(conf, config.getCachePath());
            FileAnonymizedOutputFormat.setFormat(conf, config.getFormat());
            FileAnonymizedOutputFormat.setIgnoreHeader(conf, config.getIgnoreHeader());
            FileAnonymizedOutputFormat.setFieldList(conf, config.getFieldList());
        }

        @Override
        public Map<String, String> getOutputFormatConfiguration() {
            return conf;
        }

        @Override
        public String getOutputFormatClassName() {
            return FileAnonymizedOutputFormat.class.getName();
        }
    }
}
