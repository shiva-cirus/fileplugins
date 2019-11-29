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

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.Credentials;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.TransportOptions;
import com.google.cloud.WriteChannel;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.*;
import com.voltage.securedata.enterprise.FPE;
import com.voltage.securedata.enterprise.LibraryContext;
import com.voltage.securedata.enterprise.VeException;
import io.cdap.plugin.batchsink.common.FieldInfo;
import io.cdap.plugin.batchsink.common.FileListData;
import io.cdap.plugin.batchsink.utils.FileMetaData;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The record writer that takes file metadata and streams data from source database
 * to destination database
 */
public class FileAnonymizedRecordWriter extends RecordWriter<NullWritable, FileListData> {
    private static final Logger LOG = LoggerFactory.getLogger(FileAnonymizedRecordWriter.class);

    static Configuration conf = null;

    static {
        conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }

    private final String bucketName;
    private final String project;
    private final String destinationPath;
    private final String suffix;
    private final String gcsServiceAccountJson;
    private Integer bufferSize;
    private Storage storage = null;
    private Bucket bucket = null;
    private String policyUrl;
    private String identity;
    private String sharedSecret;
    private String trustStorePath;
    private String cachePath;
    private String fileFormat;
    private boolean ignoreHeader;
    private String fieldList;
    private List<FieldInfo> fields;
    private final String proxy;
    private String proxyHost;
    private int proxyPort;
    private String proxyType;
    private final boolean useProxy;

    private LibraryContext library = null;
    private Map<String, FPE> mapFPE = new HashMap<>();

    /**
     * Construct a RecordWriter given user configurations.
     *
     * @param conf The configuration that contains required information to initialize the recordWriter.
     * @throws IOException
     */
    public FileAnonymizedRecordWriter(Configuration conf) throws IOException {
        LOG.info("Initializing of RecordWriter");

        bucketName = conf.get(FileAnonymizedOutputFormat.NAME_GCS_BUCKET, null);
        LOG.info("Bucket Name - " + bucketName);

        project = conf.get(FileAnonymizedOutputFormat.NAME_GCS_PROJECTID, null);
        LOG.info("GCS Project ID - " + project);

        gcsServiceAccountJson = conf.get(FileAnonymizedOutputFormat.NAME_GCS_SERVICEACCOUNTJSON, null);
        LOG.info("GCS Service Account-" + gcsServiceAccountJson);

        destinationPath = conf.get(FileAnonymizedOutputFormat.NAME_GCS_DESTPATH, null);
        LOG.info("Dest Path - " + destinationPath);

        suffix = conf.get(FileAnonymizedOutputFormat.NAME_GCS_DESTPATH_SUFFIX, null);
        LOG.info("Suffix - " + suffix);

        String size = conf.get(FileAnonymizedOutputFormat.NAME_BUFFER_SIZE, null);
        LOG.info("Buffer size - " + size);
        bufferSize = StringUtils.isNumeric(size) ? Integer.parseInt(size) : 1024;
        if (bufferSize <= 0) {
            bufferSize = 1024;
        }
        LOG.info("Buffer size applied - " + bufferSize);

        fileFormat = conf.get(FileAnonymizedOutputFormat.NAME_FILE_FORMAT, null);
        LOG.info("File Format - " + fileFormat);

        String ignoreHeaderValue = StringUtils.defaultIfEmpty(conf.get(FileAnonymizedOutputFormat.NAME_IGNORE_HEADER, null), "");
        ignoreHeader = ignoreHeaderValue.equalsIgnoreCase("Yes");
        LOG.info("Ignore Header - " + ignoreHeader);

        fieldList = conf.get(FileAnonymizedOutputFormat.NAME_FIELD_LIST, null);
        LOG.info("Field List - " + fieldList);

        parseFields(fieldList);

        proxy = conf.get(FileAnonymizedOutputFormat.NAME_PROXY, null);
        LOG.info("Proxy - " + proxy);

        if (conf.get(FileAnonymizedOutputFormat.NAME_PROXY_TYPE).equals("NONE")) {
            useProxy = false;
            LOG.info("Proxy is not set");
        } else {
            useProxy = true;
            LOG.info("Using Proxy.");
        }

        proxyType = conf.get(FileAnonymizedOutputFormat.NAME_PROXY_TYPE, null);
        LOG.info("Proxy Type - " + proxyType);

        // Create GCS Storage using the credentials
        storage = getGoogleStorage(gcsServiceAccountJson, project, proxy, proxyType, useProxy);
        LOG.info("Created GCS Storage");
        bucket = getBucket(storage, bucketName);
        LOG.info("Created GCS Bucket");

        try {
            LOG.debug("In initialize");
            //System.setProperty("java.library.path", "/opt/DA/javasimpleapi/lib");
            /*
            LOG.info("Print all default System Properties as seen by Plugin");
            Properties p = System.getProperties();
            Enumeration keys = p.keys();
            while (keys.hasMoreElements()) {
                String key = (String) keys.nextElement();
                String value = (String) p.get(key);
                LOG.info(key + " : " + value);
            }
            */

            //LOG.info("java.library.path = {}", "/opt/DA/javasimpleapi/lib");
            Field fieldSysPath = ClassLoader.class.getDeclaredField("sys_paths");
            fieldSysPath.setAccessible(true);
            fieldSysPath.set(null, null);

            // Load the JNI library
            System.loadLibrary("vibesimplejava");

            // Print the API version
            LOG.info("SimpleAPI version: " + LibraryContext.getVersion());

            // Sample configuration
            /*
            https://voltage-pp-0000.dataprotection.voltage.com/policy/clientPolicy.xml
            accounts22@dataprotection.voltage.com
            voltage123

            /opt/DA/javasimpleapi/trustStore
            /opt/DA/javasimpleapi/cache

            /opt/cdap-test-data/csv/

            midyear-courage-256620

            gs://shiva-cirus-test/disttest/

            /opt/cdap-test-data/midyear-courage-256620-0b0bc77ed7d8.json
            */

            policyUrl = conf.get(FileAnonymizedOutputFormat.NAME_POLICY_URL, null);
            LOG.info("Policy URL - " + policyUrl);

            identity = conf.get(FileAnonymizedOutputFormat.NAME_IDENTITY, null);
            LOG.info("Identity - " + identity);

            sharedSecret = conf.get(FileAnonymizedOutputFormat.NAME_SHARED_SECRET, null);
            LOG.info("Shared Secret - " + sharedSecret);

            trustStorePath = conf.get(FileAnonymizedOutputFormat.NAME_TRUST_STORE_PATH, null);
            LOG.info("Trust Store Path - " + trustStorePath);

            cachePath = conf.get(FileAnonymizedOutputFormat.NAME_CACHE_PATH, null);
            LOG.info("Cache Path - " + cachePath);

            LOG.info("Building LibraryContext...");

            // Create the context for crypto operations
            library = new LibraryContext.Builder()
                    .setPolicyURL(policyUrl)
                    .setFileCachePath(cachePath)
                    .setTrustStorePath(trustStorePath)
                    .build();

            //Build map of FPE objects for each format
            for (FieldInfo field : fields) {
                if (!field.isAnonymize()) {
                    continue;
                }
                String fieldFormat = field.getFormat();
                LOG.info("Building library.getFPEBuilder for {}...", fieldFormat);
                FPE fpe = mapFPE.get(fieldFormat);
                if (fpe == null) {
                    // Dev Guide Code Snippet: FPEBUILD; LC:5
                    // Protect and access the credit card number
                    fpe = library.getFPEBuilder(fieldFormat)
                            .setSharedSecret(sharedSecret)
                            .setIdentity(identity)
                            .build();
                    LOG.debug(fpe.getFullIdentity());
                    LOG.debug("Created library.getFPEBuilder for {}...", fieldFormat);
                    mapFPE.put(fieldFormat, fpe);
                }
            }
        } catch (VeException | NoSuchFieldException | IllegalAccessException ve) {
            LOG.error("Error loading library", ve);
            throw new IOException(ve);
        }
    }

    private void parseFields(String fieldList) {
        fields = new ArrayList<>();

        String[] list = StringUtils.splitPreserveAllTokens(fieldList, ",");
        for (String entry : list) {
            String[] attributes = StringUtils.splitPreserveAllTokens(entry, ":", 3);
            FieldInfo field = new FieldInfo(attributes[0], attributes[1].equalsIgnoreCase("Yes"), attributes[2]);
            fields.add(field);
        }

        LOG.debug("fields = {}", StringUtils.join(fields, ","));
    }

    private static FileMetaData getFileMetaData(String filePath, String uri) throws IOException {
        return new FileMetaData(uri + '/' + filePath, conf);
    }

    private void extractHostAndPortFromProxy() {
        if (StringUtils.isNotEmpty(proxy)) {
            String[] proxyComponents = StringUtils.splitByWholeSeparatorPreserveAllTokens(proxy, ":");
            if (proxyComponents.length > 0) {
                proxyHost = proxyComponents[0];
            }

            if (proxyComponents.length > 1) {
                proxyPort = NumberUtils.toInt(proxyComponents[1], 0);
            }
        }
    }

    private Storage getGoogleStorage(String serviceAccountJSON, String project, String proxy, String proxyType, boolean useProxy) {
        Credentials credentials = null;
        try {
            credentials = GoogleCredentials.fromStream(new FileInputStream(serviceAccountJSON));
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }

        StorageOptions.Builder builder = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId(project);

        if (useProxy) {
            extractHostAndPortFromProxy();

            LOG.info("Proxy Host - " + proxyHost);
            LOG.info("Proxy Port - " + proxyPort);

            HttpTransportFactory transportFactory = new HttpTransportFactory() {

                @Override
                public HttpTransport create() {

                    if ("SOCKS".equals(proxyType)) {
                        return new NetHttpTransport.Builder().setProxy(new Proxy(Proxy.Type.SOCKS, new InetSocketAddress(proxyHost, proxyPort))).build();
                    } else {
                        return new NetHttpTransport.Builder().setProxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort))).build();
                    }
                }
            };

            TransportOptions transportOptions = HttpTransportOptions.newBuilder().setHttpTransportFactory(transportFactory).build();
            builder.setTransportOptions(transportOptions);
        }

        return builder.build().getService();
    }

    private Bucket getBucket(Storage storage, String bucketName) {
        Bucket bucket = storage.get(bucketName);
        if (bucket == null) {
            LOG.info("Creating new bucket '{}'.", bucketName);
            bucket = storage.create(BucketInfo.of(bucketName));
        }
        return bucket;
    }

    /**
     * This method connects to the source filesystem and copies the file specified by the FileMetadata input to the
     * destination filesystem.
     *
     * @param key          Unused key.
     * @param fileListData Contains metadata for the file we wish to copy.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(NullWritable key, FileListData fileListData) throws IOException, InterruptedException {
        LOG.debug("In write");
        if (fileListData.getRelativePath().isEmpty()) {
            LOG.warn("fileListData.getRelativePath() is empty");
            return;
        }

        if (fileListData.getFullPath().isEmpty()) {
            LOG.warn("fileListData.getFullPath() is empty");
            return;
        }

        // construct file paths for source and destination
        String outFileName = destinationPath + fileListData.getRelativePath();
        String contentType = "text/csv";

        FileMetaData fileMetaData = null;
        String fullPath = fileListData.getFullPath();
        try {
            fileMetaData = getFileMetaData(fullPath, fileListData.getHostURI());
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }

        LOG.info("Output File Name " + outFileName);

        InputStream inputStream = null;

        try {
            inputStream = getAnonymizedStream(fileMetaData);
        } catch (Exception e) {
            LOG.error(String.format("Failed while anonymizing the file '%s'", fileMetaData.getPath().getName()), e);
            inputStream = null;
            throw new IOException(String.format("Failed while anonymizing the file '%s'", fileMetaData.getPath().getName()));
        }

        if (inputStream == null) {
            LOG.warn("File Anonymization failed for {} hence skipping GCS upload.", fileMetaData.getPath().getName());
            return;
        }

        BlobId blobId = BlobId.of(bucket.getName(), outFileName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(contentType).build();

        try {
            byte[] buffer = new byte[bufferSize];
            try (WriteChannel writer = storage.writer(blobInfo)) {
                int limit;
                while ((limit = inputStream.read(buffer)) >= 0) {
                    LOG.debug("upload file " + limit);
                    writer.write(ByteBuffer.wrap(buffer, 0, limit));
                }
            }
        } catch (IOException ie) {
            LOG.error(ie.getMessage(), ie);
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }

        LOG.debug("write completed");
    }

    private void buildAnonymizedContents(OutputStream outPipe, FileMetaData fileMetaData) throws IOException {
        LOG.debug("In buildAnonymizedContents");
        try (
                FSDataInputStream in = fileMetaData.getFileSystem().open(fileMetaData.getPath());
                InputStreamReader reader = new InputStreamReader(in);
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
        ) {
            List<CSVRecord> records = csvParser.getRecords();
            if (records.isEmpty()) {
                LOG.warn(String.format("The input file '%s' is empty", fileMetaData.getPath().getName()));
                throw new IOException(String.format("The input file '%s' is empty", fileMetaData.getPath().getName()));
            }

            LOG.debug("# of records in {} = {}", fileMetaData.getPath().getName(), records.size());

            CSVPrinter printer = new CSVPrinter(new OutputStreamWriter(outPipe), CSVFormat.DEFAULT);

            List<Object> fieldValues = new ArrayList<>();
            for (CSVRecord csvRecord : records) {
                LOG.debug("Processing record {}", csvRecord.getRecordNumber());
                if (csvRecord.size() < fields.size()) {
                    LOG.error(String.format("Invalid number of columns at line %d in '%s' file", csvRecord.getRecordNumber(), fileMetaData.getPath().getName()));
                    printer.close();
                    throw new IOException(String.format("Invalid number of columns at line %d in '%s' file", csvRecord.getRecordNumber(), fileMetaData.getPath().getName()));
                }

                fieldValues.clear();

                //Loop through all columns in the incoming file
                for (int columnIndex = 0; columnIndex < csvRecord.size(); columnIndex++) {
                    FieldInfo field = null;
                    if (columnIndex < fields.size()) {
                        field = fields.get(columnIndex);
                    }

                    String plainText = csvRecord.get(columnIndex);
                    LOG.debug("R{}:C{} = {}", csvRecord.getRecordNumber(), columnIndex, plainText);

                    //Skip the header row if ignoreHeader is true
                    if (ignoreHeader && csvRecord.getRecordNumber() == 1) {
                        LOG.info("Ignore Header row");
                        fieldValues.add(plainText);
                    } else {
                        //if field is marked for anonymization then call api
                        if (field != null && field.isAnonymize() && StringUtils.isNotEmpty(plainText)) {
                            LOG.debug("field != null && field.isAnonymize()");
                            String cipherText = anonymize(plainText, field.getFormat());
                            if (cipherText == null) {
                                LOG.error(String.format("Unable to anonymize '%s' value for column '%s' at line %d in '%s' file.",
                                        plainText, field.getName(), csvRecord.getRecordNumber(), fileMetaData.getPath().getName()));
                                printer.close();
                                throw new IOException(String.format("Unable to anonymize '%s' value for column '%s' at line %d in '%s' file.",
                                        plainText, field.getName(), csvRecord.getRecordNumber(), fileMetaData.getPath().getName()));
                            } else {
                                fieldValues.add(cipherText);
                            }
                        } else {
                            //otherwise use the original value as is
                            LOG.debug("in else");
                            fieldValues.add(plainText);
                        }
                    }
                }
                printer.printRecord(fieldValues);
            }

            LOG.debug("Processed all records");

            printer.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
        LOG.info("buildAnonymizedContents completed");
    }

    private String anonymize(String plainText, String format) {
        String cipherText = null;
        try {
            LOG.debug("Anonymizing {} with {} format...", plainText, format);

            FPE fpe = mapFPE.get(format);
            if (fpe == null) {
                LOG.error("Invalid Anonymize format {}", format);
                return null;
            }
            // Dev Guide Code Snippet: FPEPROTECT; LC:1
            cipherText = fpe.protect(plainText);

            LOG.debug("Anonymizing {} with {} format = {}...", plainText, format, cipherText);
        } catch (VeException ve) {
            LOG.error("Error while anonymizing.", ve);
        }

        return cipherText;
    }

    private InputStream getAnonymizedStream(FileMetaData fileMetaData) throws IOException {
        LOG.debug("In getAnonymizedStream");
        PipedOutputStream outPipe = new PipedOutputStream();
        PipedInputStream inPipe = new PipedInputStream();
        inPipe.connect(outPipe);

        try {
            new Thread(
                    () -> {
                        try {
                            LOG.debug("In getAnonymizedStream::thread");
                            buildAnonymizedContents(outPipe, fileMetaData);
                        } catch (IOException e) {
                            LOG.error("Throwing RuntimeException from getAnonymizedStream::Thread", e);
                            throw new RuntimeException("Failed while getAnonymizedStream");
                        } finally {
                            try {
                                outPipe.close();
                            } catch (IOException e) {
                                LOG.error("Throwing RuntimeException while closing outPipe in getAnonymizedStream::Thread", e);
                                throw new RuntimeException("Failed while closing outPipe in getAnonymizedStream");
                            }
                        }
                    })
                    .start();

            /*
            MyThread t = new MyThread(outPipe, fileMetaData);
            t.start();
            t.join();
            if (!t.isRunning() && t.getException() != null) {
                throw t.getException();
            }
            */
        } catch (Exception e) {
            LOG.error("Caught Exception in getAnonymizedStream");
            throw new IOException(e);
        }

        LOG.debug("getAnonymizedStream completed");
        return inPipe;
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        LOG.debug("In close");

        //delete all FPE instances before the associated LibraryContext instance is deleted.
        for (FPE value : mapFPE.values()) {
            value.delete();
        }

        //it is safe to delete the shared LibraryContext instance.
        if (library != null) {
            library.delete();
        }
        LOG.debug("close completed");
    }

    public class MyThread extends Thread {
        private PipedOutputStream outPipe;
        private FileMetaData fileMetaData;
        private boolean running;
        private Exception exception;

        public MyThread(PipedOutputStream outPipe, FileMetaData fileMetaData) {
            this.outPipe = outPipe;
            this.fileMetaData = fileMetaData;
            running = false;
        }

        public boolean isRunning() {
            return running;
        }

        public Exception getException() {
            return exception;
        }

        @Override
        public void run() {
            running = true;
            try {
                LOG.debug("In MyThread::run");
                buildAnonymizedContents(outPipe, fileMetaData);
                running = false;
            } catch (IOException e) {
                LOG.error("Catching IOException in MyThread::run");
                running = false;
                exception = e;
            } finally {
                try {
                    outPipe.close();
                } catch (IOException e) {
                    LOG.error("Catching IOException while closing outPipe in MyThread::run");
                    running = false;
                    exception = e;
                }
            }
        }
    }
}
