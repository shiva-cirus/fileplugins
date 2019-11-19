package io.cdap.plugin.file.ingest.batchsink;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.plugin.file.ingest.utils.GCSPath;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class fileCompressEncryptGCSBatchSinkConfig extends PluginConfig {
    public static final String NAME_COMPRESSION = "compression";
    public static final String NAME_ENCRYPTION = "encryption";
    public static final String NAME_PATH = "path";
    public static final String NAME_SUFFIX = "suffix";
    public static final String NAME_PROJECT = "project";
    public static final String NAME_SERVICE_ACCOUNT_FILE_PATH = "serviceFilePath";
    public static final String AUTO_DETECT = "auto-detect";
    public static final String NAME_ENCRYPTION_PUBLIC_KEY_FILE_PATH = "publicKeyPath";
    public static final String SCHEME = "gs://";

    private static final Logger LOG = LoggerFactory.getLogger(fileCompressEncryptGCSBatchSinkConfig.class);

    @Name(NAME_COMPRESSION)
    @Description("Specify the compression algorithm. If None is selected then data is not compressed.")
    protected String compression = CompressorType.ZIP.getType();

    @Name(NAME_ENCRYPTION)
    @Description("Specify the encryption algorithm. If None is selected then data is not encrypted.")
    protected String encryption = EncryptionType.PGP.getType();

    @Name(NAME_PATH)
    @Description("The path to write to. For example, gs://<bucket>")
    @Macro
    protected String path;

    @Name(NAME_SUFFIX)
    @Description("The time format for the output directory that will be appended to the path. " +
            "For example, the format 'yyyy-MM-dd-HH-mm' will result in a directory of the form '2015-01-01-20-42'. " +
            "If not specified, nothing will be appended to the path.")
    @Nullable
    @Macro
    protected String suffix;

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

    public fileCompressEncryptGCSBatchSinkConfig(String compression, String encryption, String path, @Nullable String suffix, String project, String serviceFilePath, @Nullable String publicKeyPath) {
        this.compression = compression;
        this.encryption = encryption;
        this.path = path;
        this.suffix = suffix;
        this.project = project;
        this.serviceFilePath = serviceFilePath;
        this.publicKeyPath = publicKeyPath;
    }

    public String getDestPath() {
        String destinationPath = GCSPath.from(path).getName();
        if ( StringUtils.isNotEmpty(suffix)) {
            try {
                //This will throw an exception if format is invalid
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(suffix);
                LocalDateTime now = LocalDateTime.now();
                if (!destinationPath.endsWith("/")) {
                    destinationPath += "/";
                }
                destinationPath += now.format(formatter) + "/";
            } catch (Exception e) {
                LOG.error("Error while processing suffix - ", e);
                throw new IllegalArgumentException(
                        "Error while processing suffix.");

            }
        }
        return destinationPath;
    }

    public String getBucket() {
        return GCSPath.from(path).getBucket();
    }

    public String getProject() {
        String projectId = tryGetProject();
        if (projectId == null) {
            throw new IllegalArgumentException(
                    "Could not detect Google Cloud project id from the environment. Please specify a project id.");
        }
        return projectId;
    }

    public boolean compressFile() {
        if (Strings.isNullOrEmpty(compression) || compression.equals(CompressorType.NONE.getType()))
            return false;
        return true;

    }

    public boolean encryptFile() {
        if (Strings.isNullOrEmpty(encryption) || encryption.equals(EncryptionType.NONE.getType()))
            return false;

        return true;
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

    public String getCompressor() {
        return compression;
    }

    public String getEncryption() {
        return encryption;
    }

    public String getPath() {
        return path;
    }

    public String getSuffix() {



        return suffix;
    }

    @Nullable
    public String getPublicKeyPath() {
        return publicKeyPath;
    }

    private enum CompressorType {
        ZIP("ZIP"),
        NONE("NONE");
        private String type;

        CompressorType(String type) {
            this.type = type;
        }

        String getType() {
            return type;
        }
    }

    private enum EncryptionType {
        PGP("PGP"),
        NONE("NONE");
        private String type;

        EncryptionType(String type) {
            this.type = type;
        }

        String getType() {
            return type;
        }
    }
}
