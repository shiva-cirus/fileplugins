package v2.io.cdap.plugin.file.ingest;


import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.SourceInputFormatProvider;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.file.ingest.FileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(FileDecompressDecryptSource.NAME)
@Description("Reads a Encrypted / Compressed File of CSV Data.")
public class FileDecompressDecryptSource extends BatchSource<NullWritable, CSVRecord, StructuredRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(FileDecompressDecryptSource.class);
    public static final String NAME = "FileDecompressDecryptSource";
    private final FileDecompressDecryptSourceConfig config;

    public FileDecompressDecryptSource(FileDecompressDecryptSourceConfig config) {
        this.config = config;
    }


    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
        super.configurePipeline(pipelineConfigurer);
        List<Schema.Field> fieldList = config.getSchema().getFields();
        pipelineConfigurer
                .getStageConfigurer()
                .setOutputSchema(Schema.recordOf("fileSchema", fieldList));
    }


    @Override
    public void prepareRun(BatchSourceContext context) throws Exception {


        if (config.decryptFile() && StringUtils.isEmpty(config.privateKeyFilePath)) {
            throw new IllegalArgumentException(String.format("Decryption is  enabled and PGP Private Key path is missing for %s plugin. Please provide the same.", NAME));
        }

        if (config.decryptFile() && StringUtils.isNotEmpty(config.privateKeyFilePath) && StringUtils.isEmpty(config.password)) {
            throw new IllegalArgumentException(String.format("Provide password for Private Key File.", NAME));
        }

        Job job = JobUtils.createInstance();
        Configuration conf = job.getConfiguration();
        conf.setBoolean("decrypt", config.decryptFile());
        conf.setBoolean("decompress", config.decompressFile());
        conf.set("privateKeyFilePath", config.privateKeyFilePath);
        conf.set("password", config.password);

        switch (config.scheme) {
            case "file":
                FileInputFormat.setURI(conf, new URI(config.scheme, null, Path.SEPARATOR, null).toString());
                break;
            case "hdfs":
                break;
            default:
                throw new IllegalArgumentException("Scheme must be either file or hdfs.");
        }

        context.setInput(
                Input.of("FileDecompressDecryptInputFormatProvider", new SourceInputFormatProvider(FileInputFormat.class, conf)));



    }

    @Override
    public void initialize(BatchRuntimeContext context) throws Exception {
        super.initialize(context);
    }


    @Override
    public void transform(
            KeyValue<NullWritable, CSVRecord> input, Emitter<StructuredRecord> emitter) {
        emitter.emit(toRecord(input.getValue()));
    }

    /** Converts to a StructuredRecord */
    public StructuredRecord toRecord(CSVRecord csvRecord) {
        StructuredRecord structuredRecord = null;
        // merge default schema and credential schema to create output schema
        try {
            Schema outputSchema;
            List<Schema.Field> fieldList = config.getSchema().getFields();
            outputSchema = Schema.recordOf("metadata", fieldList);

            StructuredRecord.Builder outputBuilder = StructuredRecord.builder(outputSchema);

            fieldList.forEach(
                    field -> {
                        String schema = field.getSchema().getType().name();
                        if (field.getSchema().getLogicalType() != null) {
                            schema = field.getSchema().getLogicalType().name();
                        }
                        String value = csvRecord.get(field.getName());
                        if (schema.equalsIgnoreCase("INT")) {
                            outputBuilder.set(field.getName(), Integer.valueOf(value));
                        }
                        if (schema.equalsIgnoreCase("LONG")) {
                            outputBuilder.set(field.getName(), Integer.valueOf(value));
                        }
                        if (schema.equalsIgnoreCase("DATE")) {
                            outputBuilder.set(field.getName(), Date.valueOf(value));
                        }
                        if (schema.equalsIgnoreCase("STRING")) {
                            outputBuilder.set(field.getName(), value);
                        }
                        if (schema.equalsIgnoreCase("BOOLEAN")) {
                            outputBuilder.set(field.getName(), Boolean.valueOf(value));
                        }
                        if (schema.equalsIgnoreCase("BYTES")) {
                            outputBuilder.set(field.getName(), value.getBytes());
                        }
                        if (schema.equalsIgnoreCase("DOUBLE")) {
                            outputBuilder.set(field.getName(), Double.valueOf(value));
                        }
                        if (schema.equalsIgnoreCase("DECIMAL")) {
                            outputBuilder.set(field.getName(), Double.valueOf(value));
                        }
                        if (schema.equalsIgnoreCase("FLOAT")) {
                            outputBuilder.set(field.getName(), Float.valueOf(value));
                        }
                        if (schema.equalsIgnoreCase("TIME")) {
                            outputBuilder.set(field.getName(), Time.valueOf(value));
                        }
                        if (schema.equalsIgnoreCase("TIMESTAMP")) {
                            outputBuilder.set(field.getName(), Timestamp.valueOf(value));
                        }
                    });
            structuredRecord = outputBuilder.build();
        } catch (Exception e) {
            LOG.error(" Error parsing CSV data {} ", e);
        }

        return structuredRecord;
    }


}
