package io.cdap.plugin.file.ingest;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.plugin.file.ingest.utils.GCSPath;

import javax.annotation.Nullable;
import java.io.IOException;

public class CompressorEncryptorSinkConfig extends PluginConfig {
    private enum CompressorType {
        ZIP("ZIP"), NONE("NONE");

        private String type;

        CompressorType(String type) {
            this.type = type;
        }

        String getType() {
            return type;
        }
    }

    private enum EncryptionType {
        PGP("PGP"), NONE("NONE");

        private String type;

        EncryptionType(String type) {
            this.type = type;
        }

        String getType() {
            return type;
        }
    }

    public static final String NAME_COMPRESSOR = "compressor";
    public static final String NAME_ENCRYPTION = "encryption";
    public static final String NAME_PATH = "path";
    public static final String NAME_PROJECT = "project";
    public static final String NAME_SERVICE_ACCOUNT_FILE_PATH = "serviceFilePath";
    public static final String AUTO_DETECT = "auto-detect";
    public static final String NAME_ENCRYPTION_PUBLIC_KEY_FILE_PATH = "publicKeyPath";


    @Name(NAME_COMPRESSOR)
    @Description("Specify the compression algorithm. If None is selected then data is not compressed.")
    protected String compressor = CompressorType.ZIP.getType();

    @Name(NAME_ENCRYPTION)
    @Description("Specify the encryption algorithm. If None is selected then data is not encrypted.")
    protected String encryption = EncryptionType.PGP.getType();

    protected static final String SCHEME = "gs://";

    @Name(NAME_PATH)
    @Description("The path to write to. For example, gs://<bucket>")
    @Macro
    protected String path;

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

    public String getDestPath(){
        return GCSPath.from(path).getName();
    }

    public String getBucket() {
        return GCSPath.from(path).getBucket();
    }

    public CompressorEncryptorSinkConfig(String compressor, String encryption, String path, String project, String serviceFilePath, @Nullable String publicKeyPath) {
        this.compressor = compressor;
        this.encryption = encryption;
        this.path = path;
        this.project = project;
        this.serviceFilePath = serviceFilePath;
        this.publicKeyPath = publicKeyPath;
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
        if (Strings.isNullOrEmpty(compressor) || compressor.equals(CompressorType.NONE.getType()))
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
        return compressor;
    }

    public String getEncryption() {
        return encryption;
    }

    public String getPath() {
        return path;
    }

    @Nullable
    public String getPublicKeyPath() {
        return publicKeyPath;
    }
}
