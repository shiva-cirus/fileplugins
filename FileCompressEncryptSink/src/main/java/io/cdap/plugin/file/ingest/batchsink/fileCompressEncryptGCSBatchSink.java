package io.cdap.plugin.file.ingest.batchsink;

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
import io.cdap.plugin.file.ingest.common.FileListData;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(fileCompressEncryptGCSBatchSink.NAME)
@Description("File Compress / Encrypt and Persist to GCS.")
public class fileCompressEncryptGCSBatchSink extends BatchSink<StructuredRecord, NullWritable, FileListData> {
    public static final String NAME = "fileCompressEncryptGCSBatchSink";

    private static final Logger LOG = LoggerFactory.getLogger(fileCompressEncryptGCSBatchSink.class);

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
        context.addOutput(Output.of("FileCopyOutputFormatProvider", new FileCopyOutputFormatProvider(config)));

        if (config.encryptFile() && StringUtils.isEmpty(config.getPublicKeyPath())) {
            throw new IllegalArgumentException(String.format("Encryption enabled and PGP Public Key path is missing for %s plugin. Please provide the same.", NAME));
        }
        if (StringUtils.isNotEmpty(config.getSuffix())) {
            try {
                DateTimeFormatter.ofPattern(config.getSuffix());
            } catch (Exception e) {
                throw new IllegalArgumentException(String.format("Suffix has a invalid date format for %s plugin. Please correct the same.", NAME));
            }
        }
        if (!NumberUtils.isCreatable(config.getBufferSize())) {
            throw new IllegalArgumentException(String.format("Buffer size must be a numeric value for %s plugin. Please provide the same.", NAME));
        }

        if (config.useProxy() && StringUtils.isEmpty(config.getProxy())) {
            throw new IllegalArgumentException(String.format("Proxy host and port is required.", NAME));
        }


        if (StringUtils.isNotEmpty(config.getProxy())) {
            String[] proxyComponents = StringUtils.splitByWholeSeparatorPreserveAllTokens(config.getProxy(), ":");
            if (proxyComponents.length != 2) {
                throw new IllegalArgumentException(String.format("Invalid proxy value for %s plugin. It must be in \"host:port\" format. Please provide the same.", NAME));
            }
            int port = NumberUtils.toInt(proxyComponents[1], 0);
            if (port == 0) {
                throw new IllegalArgumentException(String.format("Invalid proxy port value for %s plugin. Please correct the same.", NAME));
            }
        }
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
    public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, FileListData>> emitter) throws Exception {
        FileListData output = new FileListData(input);
        emitter.emit(new KeyValue<NullWritable, FileListData>(null, output));
    }

    /**
     * Adds necessary configuration resources and provides OutputFormat Class
     */
    public class FileCopyOutputFormatProvider implements OutputFormatProvider {
        protected final Map<String, String> conf;

        public FileCopyOutputFormatProvider(fileCompressEncryptGCSBatchSinkConfig config) {
            this.conf = new HashMap<>();
            FileCopyOutputFormat.setCompression(conf, config.getCompressor());
            FileCopyOutputFormat.setEncryption(conf, config.getEncryption());
            FileCopyOutputFormat.setGCSBucket(conf, config.getBucket());
            FileCopyOutputFormat.setGCSDestPath(conf, config.getDestPath());
            FileCopyOutputFormat.setGCSDestPathSuffix(conf, config.getSuffix());
            FileCopyOutputFormat.setPGPPubKey(conf, config.getPublicKeyPath());
            FileCopyOutputFormat.setGCSProjectID(conf, config.getProject());
            FileCopyOutputFormat.setGCSServiceAccount(conf, config.getServiceAccountFilePath());
            FileCopyOutputFormat.setBufferSize(conf, config.getBufferSize());
            FileCopyOutputFormat.setProxy(conf, config.getProxy());
            FileCopyOutputFormat.setProxyType(conf, config.getProxyType());
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
