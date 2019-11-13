/*
 * Copyright © 2015 Cask Data, Inc.
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
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.plugin.file.ingest.encryption.FileCompressEncrypt;
import io.cdap.plugin.file.ingest.encryption.PGPExampleUtil;
import io.cdap.plugin.file.ingest.utils.FileEncryptTest;
import io.cdap.plugin.file.ingest.utils.GCSPath;
import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.JavaRDD;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Compresses the configured fields using the algorithms specified.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name(CompressorEncryptorSink.NAME)
@Description("Compresses configured fields using the algorithms specified.")
public final class CompressorEncryptorSink extends SparkSink<StructuredRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(CompressorEncryptorSink.class);

    private Config config = null;

    public static final String NAME = "CompressorEncryptorSink";

    // Output Schema associated with transform output.
    private Schema outSchema;

    // Output Field name to type map
    private Map<String, Schema.Type> outSchemaMap = new HashMap<>();

    private final Map<String, CompressorType> compMap = new HashMap<>();

    private static Storage storage;


    // This is used only for tests, otherwise this is being injected by the ingestion framework.
    public CompressorEncryptorSink(Config config) {
        this.config = config;
    }

    private void parseConfiguration(String config) throws IllegalArgumentException {
        String[] mappings = config.split(",");
        for (String mapping : mappings) {
            String[] params = mapping.split(":");

            // If format is not right, then we throw an exception.
            if (params.length < 2) {
                throw new IllegalArgumentException("Configuration " + mapping + " is in-correctly formed. " +
                        "Format should be <fieldname>:<compressor-type>");
            }

            String field = params[0];
            String type = params[1].toUpperCase();
            CompressorType cType = CompressorType.valueOf(type);

            if (compMap.containsKey(field)) {
                throw new IllegalArgumentException("Field " + field + " already has compressor set. Check the mapping.");
            } else {
                compMap.put(field, cType);
            }
        }
    }


    @Override
    public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
        super.configurePipeline(pipelineConfigurer);

        Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
        if (inputSchema != null) {
            /*WordCount wordCount = new WordCount(config.field);
            wordCount.validateSchema(inputSchema);*/
        }
        pipelineConfigurer.createDataset(config.tableName, KeyValueTable.class, DatasetProperties.EMPTY);

    }


    private static byte[] gzip(byte[] input) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip = null;
        try {
            gzip = new GZIPOutputStream(out);
            gzip.write(input, 0, input.length);
        } catch (IOException e) {
            // These are all in memory operations, so this should not happen.
            // But, if it happens then we just return null. Logging anything
            // here can be noise.
            return null;
        } finally {
            if (gzip != null) {
                try {
                    gzip.close();
                } catch (IOException e) {
                    return null;
                }
            }
        }

        return out.toByteArray();
    }

    private byte[] zip(byte[] input) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ZipOutputStream zos = new ZipOutputStream(out);
        try {
            zos.setLevel(9);
            zos.putNextEntry(new ZipEntry("c"));
            zos.write(input, 0, input.length);
            zos.finish();
        } catch (IOException e) {
            return null;
        } finally {
            try {
                if (zos != null) {
                    zos.close();
                }
            } catch (IOException e) {
                return null;
            }
        }

        return out.toByteArray();
    }


    @Override
    public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> javaRDD) throws IOException, PGPException {
        LOG.info("Schema and filds are: " + config.compressor);
        String fieldName = null;
        if (config.compressor != null) {
            String[] split = config.compressor.split(":");
            fieldName = split[0];
        }

        System.out.println("fieldName >>>>>>>>>" + fieldName);

        PGPPublicKey encKey = null;

        encKey = PGPExampleUtil.readPublicKey(config.publicKeyPath);

        storage = getGoogleStorage();
        Bucket bucket = getBucket();

        String finalFieldName = fieldName;
        PGPPublicKey finalEncKey = encKey;

        List<StructuredRecord> collect = javaRDD.collect();

        collect.forEach(st -> {

            String fileName = st.get(finalFieldName);
            String outFileName = null;
            File file = new File(fileName);
            if (file != null) {
                outFileName = file.getName() + ".enc";
            }

            LOG.info("file Name >>> " + fileName);

            BlobId blobId = BlobId.of(bucket.getName(), outFileName);

            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/pgp-encrypted").build();


            InputStream inputStream = null;

            try {
                inputStream = FileCompressEncrypt.gcsWriter(fileName, finalEncKey);
            } catch (IOException e) {
                e.printStackTrace();
            }

            byte[] buffer = new byte[1 << 16];
            try {
                try (WriteChannel writer =
                             storage.writer(blobInfo)) {
                    int limit;
                    while ((limit = inputStream.read(buffer)) >= 0) {
                        System.out.println("upload file " + limit);
                        writer.write(ByteBuffer.wrap(buffer, 0, limit));
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }


        });

    }


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

    private static InputStream gzipInputStream(InputStream inputStream) throws IOException {
        PipedInputStream inPipe = new PipedInputStream();
        PipedOutputStream outPipe = new PipedOutputStream(inPipe);
        new Thread(
                () -> {
                    try (OutputStream outZip = new GZIPOutputStream(outPipe)) {
                        IOUtils.copy(inputStream, outZip);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
        ).start();
        return inPipe;
    }

    private static InputStream encryptInputStream(InputStream inputStream) throws IOException {
        PipedInputStream inPipe = new PipedInputStream();
        PipedOutputStream outPipe = new PipedOutputStream(inPipe);
        DataOutputStream dataOutputStream = new DataOutputStream(outPipe);
        new Thread(
                () -> {
                    FileEncryptTest outZip = new FileEncryptTest(outPipe);
                    try {
                        IOUtils.copy(inputStream, dataOutputStream);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
        ).start();
        return inPipe;
    }

    private static void uploadToStorage(InputStream fileInputStream, BlobInfo blobInfo)
            throws IOException {

        // For big files:
        // When content is not available or large (1MB or more) it is recommended to write it in chunks
        // via the blob's channel writer.
        try (WriteChannel writer = storage.writer(blobInfo)) {

            byte[] buffer = new byte[10_240];
            try (InputStream input = fileInputStream) {
                int limit;
                while ((limit = input.read(buffer)) >= 0) {
                    writer.write(ByteBuffer.wrap(buffer, 0, limit));
                }
            }
        }
    }

    private FileOutputStream compressEncript(FileInputStream fis, CompressorType type, String publicKey) throws FileNotFoundException {
        return new FileOutputStream("");
    }

    /*private void getFileStorage(SparkExecutionPluginContext context) {
        String cmekKey = context.getArguments().get(GCPUtils.CMEK_KEY);
        Credentials credentials = null;
        try {
            credentials = config.getServiceAccountFilePath() == null ?
                    null : GCPUtils.loadServiceAccountCredentials(config.getServiceAccountFilePath());
        } catch (IOException e) {
            e.printStackTrace();
        }
        Storage storage = GCPUtils.getStorage(config.getProject(), credentials);
        if (storage.get(config.getBucket()) == null) {
            GCPUtils.createBucket(storage, config.getBucket(), config.getLocation(), cmekKey);
        }

    }

    private Tuple<Storage, BlobInfo> googleCloudSinkStorage(SparkExecutionPluginContext context, String filePath) {
        String cmekKey = context.getArguments().get(GCPUtils.CMEK_KEY);
        Credentials credentials = null;
        try {
            credentials = config.getServiceAccountFilePath() == null ?
                    null : GCPUtils.loadServiceAccountCredentials(config.getServiceAccountFilePath());
        } catch (IOException e) {
            e.printStackTrace();
        }
        Storage storage = GCPUtils.getStorage(config.getProject(), credentials);
        if (storage.get(config.getBucket()) == null) {
            GCPUtils.createBucket(storage, config.getBucket(), config.getLocation(), cmekKey);
        }
        GCSFIleUpload gcsfIleUpload = new GCSFIleUpload();
        Tuple<Path, BlobInfo> parse = null;
        try {
            parse = gcsfIleUpload.parse(filePath, config.getBucket(), config.getLocation());
            //gcsfIleUpload.run(storage, parse);
        } catch (IOException e) {
            e.printStackTrace();
        }


        Tuple<Storage, BlobInfo> tuple = Tuple.of(storage, parse.y());
        return tuple;

    }*/


    @Override
    public void prepareRun(SparkPluginContext sparkPluginContext) throws Exception {
        //sparkPluginContext.getInputSchema();
        /*parseConfiguration(config.compressor);
        try {
            outSchema = Schema.parseJson(config.schema);
            List<Schema.Field> outFields = outSchema.getFields();
            for (Schema.Field field : outFields) {
                outSchemaMap.put(field.getName(), field.getSchema().getType());
            }

            for (String field : compMap.keySet()) {
                if (compMap.containsKey(field)) {
                    Schema.Type type = outSchemaMap.get(field);
                    if (type != Schema.Type.BYTES) {
                        throw new IllegalArgumentException("Field '" + field + "' is not of type BYTES. It's currently" +
                                "of type '" + type.toString() + "'.");
                    }
                }
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.");
        }*/
    }


    /**
     * Enum specifying the compressor type.
     */
    private enum CompressorType {
        SNAPPY("SNAPPY"),
        ZIP("ZIP"),
        GZIP("GZIP"),
        NONE("NONE");

        private String type;

        CompressorType(String type) {
            this.type = type;
        }

        String getType() {
            return type;
        }
    }

    private Bucket getBucket() {
        String bucketName = config.path;
        Bucket bucket = storage.get(bucketName);
        if (bucket == null) {
            System.out.println("Creating new bucket.");
            bucket = storage.create(BucketInfo.of(bucketName));
        }
        return bucket;
    }


    /**
     * Plugin configuration.
     */
    public static class Config extends PluginConfig {
        @Name("compressor")
        @Description("Specify the field and compression type combination. " +
                "Format is <field>:<compressor-type>[,<field>:<compressor-type>]*")
        public final String compressor;


        @Name(("tableName"))
        @Description("The name of the KeyValueTable to write to.")
        private String tableName;

        private static final String NAME_PATH = "path";
        private static final String NAME_SUFFIX = "suffix";
        private static final String NAME_FORMAT = "format";
        private static final String NAME_SCHEMA = "schema";
        private static final String NAME_DELIMITER = "delimiter";
        private static final String NAME_LOCATION = "location";
        public static final String NAME_PROJECT = "project";
        public static final String NAME_SERVICE_ACCOUNT_FILE_PATH = "serviceFilePath";
        public static final String AUTO_DETECT = "auto-detect";
        public static final String NAME_ENCRYPTION_PUBLIC_KEY_FILE_PATH = "publicKeyPath";


        private static final String SCHEME = "gs://";
        @Name(NAME_PATH)
        @Description("The path to write to. For example, gs://<bucket>/path/to/directory")
        @Macro
        private String path;

        @Name(NAME_SUFFIX)
        @Description("The time format for the output directory that will be appended to the path. " +
                "For example, the format 'yyyy-MM-dd-HH-mm' will result in a directory of the form '2015-01-01-20-42'. " +
                "If not specified, nothing will be appended to the path.")
        @Nullable
        @Macro
        private String suffix;

        @Name(NAME_FORMAT)
        @Description("The format to write in. The format must be one of 'json', 'avro', 'parquet', 'csv', 'tsv', "
                + "or 'delimited'.")
        protected String format;


        @Name(NAME_DELIMITER)
        @Description("The delimiter to use if the format is 'delimited'. The delimiter will be ignored if the format "
                + "is anything other than 'delimited'.")
        @Macro
        @Nullable
        private String delimiter;

        @Name(NAME_SCHEMA)
        @Description("The schema of the data to write. The 'avro' and 'parquet' formats require a schema but other "
                + "formats do not.")
        @Macro
        @Nullable
        private String schema;

        @Name(NAME_LOCATION)
        @Macro
        @Nullable
        @Description("The location where the gcs bucket will get created. " +
                "This value is ignored if the bucket already exists")
        protected String location;

        @Name(NAME_PROJECT)
        @Description("Google Cloud Project ID, which uniquely identifies a project. "
                + "It can be found on the Dashboard in the Google Cloud Platform Console.")
        @Macro
        @Nullable
        protected String project;

        @Name(NAME_SERVICE_ACCOUNT_FILE_PATH)
        @Description("Path on the local file system of the service account key used "
                + "for authorization. Can be set to 'auto-detect' when running on a Dataproc cluster. "
                + "When running on other clusters, the file must be present on every node in the cluster.")
        @Macro
        @Nullable
        protected String serviceFilePath;


        @Name(NAME_ENCRYPTION_PUBLIC_KEY_FILE_PATH)
        @Description("Path on the local file system of the public key used for encryption.")
        @Macro
        @Nullable
        protected String publicKeyPath;

        public String getBucket() {
            return GCSPath.from(path).getBucket();
        }

        /*@Override
        public String getPath() {
            GCSPath gcsPath = GCSPath.from(path);
            return SCHEME + gcsPath.getBucket() + gcsPath.getUri().getPath();
        }

        @Override
        public FileFormat getFormat() {
            return FileFormat.from(format, FileFormat::canWrite);
        }*/

        @Nullable
        public Schema getSchema() {
            if (containsMacro("schema") || Strings.isNullOrEmpty(schema)) {
                return null;
            }
            try {
                return Schema.parseJson(schema);
            } catch (IOException e) {
                throw new IllegalArgumentException("Invalid schema: " + e.getMessage(), e);
            }
        }

        @Nullable
        //@Override
        public String getSuffix() {
            return suffix;
        }

        @Nullable
        public String getDelimiter() {
            return delimiter;
        }

        @Nullable
        public String getLocation() {
            return location;
        }

        public Config(String compressor, String tableName) {
            this.compressor = compressor;
            this.tableName = tableName;
        }

        public Config(String compressor, String tableName, String path, @Nullable String suffix, String format, @Nullable String delimiter, @Nullable String schema, @Nullable String location) {
            this.compressor = compressor;
            this.tableName = tableName;
            this.path = path;
            this.suffix = suffix;
            this.format = format;
            this.delimiter = delimiter;
            this.schema = schema;
            this.location = location;
        }

        public String getProject() {
            String projectId = tryGetProject();
            if (projectId == null) {
                throw new IllegalArgumentException(
                        "Could not detect Google Cloud project id from the environment. Please specify a project id.");
            }
            return projectId;
        }

        @Nullable
        public String tryGetProject() {
            if (containsMacro(NAME_PROJECT) && Strings.isNullOrEmpty(project)) {
                return null;
            }
            String projectId = project;
            if (Strings.isNullOrEmpty(project) || AUTO_DETECT.equals(project)) {
                projectId = ServiceOptions.getDefaultProjectId();
            }
            return projectId;
        }

        @Nullable
        public String getServiceAccountFilePath() {
            if (containsMacro(NAME_SERVICE_ACCOUNT_FILE_PATH) || serviceFilePath == null ||
                    serviceFilePath.isEmpty() || AUTO_DETECT.equals(serviceFilePath)) {
                return null;
            }
            return serviceFilePath;
        }

        /**
         * Return true if the service account is set to auto-detect but it can't be fetched from the environment.
         * This shouldn't result in a deployment failure, as the credential could be detected at runtime if the pipeline
         * runs on dataproc. This should primarily be used to check whether certain validation logic should be skipped.
         *
         * @return true if the service account is set to auto-detect but it can't be fetched from the environment.
         */
        public boolean autoServiceAccountUnavailable() {
            if (getServiceAccountFilePath() == null) {
                try {
                    ServiceAccountCredentials.getApplicationDefault();
                } catch (IOException e) {
                    return true;
                }
            }
            return false;
        }

        public Config(String compressor, String tableName, String path, @Nullable String suffix, String format, @Nullable String delimiter, @Nullable String schema, @Nullable String location, @Nullable String project, @Nullable String serviceFilePath, @Nullable String publicKeyPath) {
            this.compressor = compressor;
            this.tableName = tableName;
            this.path = path;
            this.suffix = suffix;
            this.format = format;
            this.delimiter = delimiter;
            this.schema = schema;
            this.location = location;
            this.project = project;
            this.serviceFilePath = serviceFilePath;
            this.publicKeyPath = publicKeyPath;
        }
    }

}
