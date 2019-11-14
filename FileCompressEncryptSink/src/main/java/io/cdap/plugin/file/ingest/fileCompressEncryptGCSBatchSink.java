package io.cdap.plugin.file.ingest;


import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.FileSetArguments;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.file.ingest.encryption.FileCompressEncrypt;
import io.cdap.plugin.file.ingest.encryption.PGPExampleUtil;
import io.cdap.plugin.file.ingest.utils.GCSPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
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

/*

    private Storage getGoogleStorage() {
        Credentials credentials = null;
        try {
            credentials = GoogleCredentials.fromStream(new FileInputStream(config.getServiceAccountFilePath()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Storage storage = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId(config.project)
                .build()
                .getService();

        return storage;

    }

    private Bucket getBucket() {
        String bucketName = config.getBucket();
        Bucket bucket = storage.get(bucketName);
        if (bucket == null) {
            System.out.println("Creating new bucket.");
            bucket = storage.create(BucketInfo.of(bucketName));
        }
        return bucket;
    }

*/
    @Override
    public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, FileMetadata>> emitter) throws Exception {
        FileMetadata output;
        String fsScheme = URI.create((String) input.get(FileMetadata.HOST_URI)).getScheme();
        switch (fsScheme) {
            case "file":
            case "hdfs":
                output = new FileMetadata(input);
                break;
            default:
                throw new IllegalArgumentException(fsScheme + "is not supported.");
        }
        emitter.emit(new KeyValue<NullWritable, FileMetadata>(null, output));
    }


    /**
     * Adds necessary configuration resources and provides OutputFormat Class
     */
    public class FileCopyOutputFormatProvider implements OutputFormatProvider {
        protected final Map<String, String> conf;

        public FileCopyOutputFormatProvider(AbstractFileCopySinkConfig config) {
            this.conf = new HashMap<>();
            FileCopyOutputFormat.setBasePath(conf, config.basePath);
            FileCopyOutputFormat.setEnableOverwrite(conf, config.enableOverwrite.toString());
            FileCopyOutputFormat.setPreserveFileOwner(conf, config.preserveFileOwner.toString());
            FileCopyOutputFormat.setFilesystemScheme(conf, config.getScheme());

            if (config.bufferSize != null) {
                // bufferSize is in megabytes
                FileCopyOutputFormat.setBufferSize(conf, String.valueOf(config.bufferSize << 20));
            } else {
                FileCopyOutputFormat.setBufferSize(conf, String.valueOf(FileCopyRecordWriter.DEFAULT_BUFFER_SIZE));
            }
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

