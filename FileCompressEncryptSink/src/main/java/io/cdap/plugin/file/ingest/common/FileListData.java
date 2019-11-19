package io.cdap.plugin.file.ingest.common;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract class that contains file metadata fields. Extend from this class to add credentials
 * specific to different filesystems.
 */
public class FileListData implements Comparable<FileListData> {

    public static final String FILE_NAME = "fileName";
    public static final String FULL_PATH = "fullPath";
    public static final String RELATIVE_PATH = "relativePath";
    public static final String HOST_URI = "hostURI";

    // The default schema that will be used to convert this object to a StructuredRecord.
    public static final Schema DEFAULT_SCHEMA = Schema.recordOf(
            "metadata",
            Schema.Field.of(FILE_NAME, Schema.of(Schema.Type.STRING)),
            Schema.Field.of(FULL_PATH, Schema.of(Schema.Type.STRING)),
            Schema.Field.of(RELATIVE_PATH, Schema.of(Schema.Type.STRING)),
            Schema.Field.of(HOST_URI, Schema.of(Schema.Type.STRING))
    );

    // contains only the name of the file
    private final String fileName;

    // full path of the file in the source filesystem
    private final String fullPath;

    /*
     * The relavite path is constructed by deleting the portion of the source
     * path that comes before the last path separator ("/") from the full path.
     * It is assumed here that the source path is always a prefix of the full
     * path.
     *
     * For example, given full path http://example.com/foo/bar/baz/index.html
     *              and source path /foo/bar
     *              the relative path will be bar/baz/index.html
     */
    private final String relativePath;

    /*
     * URI for the Filesystem
     * For instance, the hostURI for http://abc.def.ghi/new/index.html is http://abc.def.ghi
     */
    private final String hostURI;

    /**
     * Constructs a FileMetadata instance given a FileStatus and source path. Override this method to add additional
     * credential fields to the instance.
     *
     * @param fileStatus The FileStatus object that contains raw file metadata for this object.
     * @param sourcePath The user specified path that was used to obtain this file.
     * @throws IOException
     */
    public FileListData(FileStatus fileStatus, String sourcePath) throws IOException {
        fileName = fileStatus.getPath().getName();
        fullPath = fileStatus.getPath().toUri().getPath();

        // check if sourcePath is a valid prefix of fullPath
        if (fullPath.startsWith(sourcePath)) {
            relativePath = fullPath.substring(sourcePath.lastIndexOf(Path.SEPARATOR) + 1);
        } else {
            throw new IOException("sourcePath should be a valid prefix of fullPath");
        }

        // construct host URI given the full path from filestatus
        try {
            hostURI = new URI(fileStatus.getPath().toUri().getScheme(), fileStatus.getPath().toUri().getHost(),
                    Path.SEPARATOR, null).toString();
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    /**
     * Use this constructor to construct a FileMetadata from a StructuredRecord. Override this method if additional
     * credentials are contained in the structured record.
     *
     * @param record The StructuredRecord instance to convert from.
     */
    public FileListData(StructuredRecord record) {
        this.fileName = record.get(FILE_NAME);
        this.fullPath = record.get(FULL_PATH);
        this.relativePath = record.get(RELATIVE_PATH);
        this.hostURI = record.get(HOST_URI);
    }

    /**
     * Use this constructor to deserialize from an input stream.
     *
     * @param dataInput The input stream to deserialize from.
     */
    public FileListData(DataInput dataInput) throws IOException {
        this.fileName = dataInput.readUTF();
        this.fullPath = dataInput.readUTF();
        this.relativePath = dataInput.readUTF();
        this.hostURI = dataInput.readUTF();
    }

    public String getFullPath() {
        return fullPath;
    }

    public String getFileName() {
        return fileName;
    }

    public String getRelativePath() {
        return relativePath;
    }

    public String getHostURI() {
        return hostURI;
    }

    /**
     * Converts to a StructuredRecord
     */
    public StructuredRecord toRecord() {
        // initialize credential schema
        List<Schema.Field> credentialSchemaList;
        if (getCredentialSchema() == null) {
            credentialSchemaList = new ArrayList<>();
        } else {
            credentialSchemaList = getCredentialSchema().getFields();
        }

        // merge default schema and credential schema to create output schema
        Schema outputSchema;
        List<Schema.Field> fieldList = new ArrayList<>(DEFAULT_SCHEMA.getFields());
        fieldList.addAll(credentialSchemaList);
        outputSchema = Schema.recordOf("metadata", fieldList);

        StructuredRecord.Builder outputBuilder = StructuredRecord.builder(outputSchema)
                .set(FILE_NAME, fileName)
                .set(FULL_PATH, fullPath)
                .set(RELATIVE_PATH, relativePath)
                .set(HOST_URI, hostURI);
        addCredentialsToRecordBuilder(outputBuilder);

        return outputBuilder.build();
    }

    /**
     * Compares the size of two files
     *
     * @param o The other file to compare to
     * @return 1 if this instance is larger than the other file.
     *         0 if this instance has the same size as the other file.
     *        -1 if this instance is smaller than the other file.
     */
    @Override
    public int compareTo(FileListData o) {
        return fullPath.compareTo(o.getFullPath());
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(getFileName());
        dataOutput.writeUTF(getFullPath());
        dataOutput.writeUTF(getRelativePath());
        dataOutput.writeUTF(getHostURI());
    }

    /**
     * Override this in extended class to return credential schema for different filesystems.
     *
     * @return Credential schema for different filesystems
     */
    protected Schema getCredentialSchema() {
        return null;
    }

    /**
     * Override this in extended class to add credential information to StructuredRecord.
     *
     * @param builder
     */
    protected void addCredentialsToRecordBuilder(StructuredRecord.Builder builder) {
        // no op
    }
}
