package io.cdap.plugin.batchsink;

import com.google.cloud.ServiceOptions;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.plugin.batchsink.utils.GCSPath;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class FileAnonymizerBatchSinkConfig extends PluginConfig {
    public static final String NAME_PATH = "path";
    public static final String NAME_SUFFIX = "suffix";
    public static final String NAME_PROJECT = "project";
    public static final String NAME_SERVICE_ACCOUNT_FILE_PATH = "serviceFilePath";
    public static final String NAME_BUFFER_SIZE = "bufferSize";
    public static final String AUTO_DETECT = "auto-detect";
    public static final String NAME_PROXY = "proxy";
    public static final String NAME_PROXY_TYPE = "proxyType";

    private static final Logger LOG = LoggerFactory.getLogger(FileAnonymizerBatchSinkConfig.class);

    @Name(NAME_PATH)
    @Description("The path to write to. For example, gs://<bucket>")
    @Macro
    private String path;

    @Name(NAME_SUFFIX)
    @Description("The time format for the output directory that will be appended to the path. " +
            "For example, the format 'yyyy-MM-dd-HH-mm' will result in a directory of the form '2015-01-01-20-42'. " +
            "If not specified, nothing will be appended to the path.")
    @Nullable
    @Macro
    private String suffix;

    @Name(NAME_PROJECT)
    @Description("Google Cloud Project ID, which uniquely identifies a project. "
            + "It can be found on the Dashboard in the Google Cloud Platform Console.")
    @Macro
    @Nullable
    private String project;

    @Name(NAME_SERVICE_ACCOUNT_FILE_PATH)
    @Description("Path on the local file system of the service account key used "
            + "for authorization. Can be set to 'auto-detect' when running on a Dataproc cluster. "
            + "When running on other clusters, the file must be present on every node in the cluster.")
    @Macro
    @Nullable
    private String serviceFilePath;

    @Name(NAME_BUFFER_SIZE)
    @Description("Buffer size to read the contents. The default is 1024")
    @Macro
    private String bufferSize;

    @Name("policyUrl")
    @Description("Specify the Policy Url")
    @Macro
    private String policyUrl;

    @Name("identity")
    @Description("Specify the Identity")
    @Macro
    private String identity;

    @Name("sharedSecret")
    @Description("Specify the Shared Secret")
    @Macro
    private String sharedSecret;

    @Name("trustStorePath")
    @Description("Specify the Trust Store Path")
    @Macro
    private String trustStorePath;

    @Name("cachePath")
    @Description("Specify the Cache Path")
    @Macro
    private String cachePath;

    @Name("format")
    @Description("Specify the file format")
    private String format;

    @Name("ignoreHeader")
    @Description("Specify if header row is to be ignored or not")
    private String ignoreHeader;

    @Name("fieldList")
    @Description("Specify the field, anonymization flag and anonymization format combination. " +
            "Format is <field>:<anonymize-flag>:<anonymize-format>[,<field>:<anonymize-flag>:<anonymize-format>]*")
    private String fieldList;

    @Name(NAME_PROXY)
    @Description("The proxy to be used. If none specified it will make a direct connection or through " +
            "the proxy set at the environment level.")
    @Nullable
    @Macro
    private String proxy;

    @Name(NAME_PROXY_TYPE)
    @Description("Specify the Proxy Type. None - No Proxy , HTTP - Proxy for HTTP , SOCKS - Low level Proxy")
    private String proxyType = ProxyType.NONE.getType();

    public FileAnonymizerBatchSinkConfig(String path, @Nullable String suffix, String project, String serviceFilePath,
                                         String bufferSize, String policyUrl, String identity, String sharedSecret,
                                         String trustStorePath, String cachePath, String format, String ignoreHeader,
                                         String fieldList, @Nullable String proxy, String proxyType) {
        this.path = path;
        this.suffix = suffix;
        this.project = project;
        this.serviceFilePath = serviceFilePath;
        this.bufferSize = bufferSize;
        this.policyUrl = policyUrl;
        this.identity = identity;
        this.sharedSecret = sharedSecret;
        this.trustStorePath = trustStorePath;
        this.cachePath = cachePath;
        this.format = format;
        this.ignoreHeader = ignoreHeader;
        this.fieldList = fieldList;
        this.proxy = proxy;
        this.proxyType = proxyType;
    }

    @Nullable
    public String getSuffix() {
        return suffix;
    }

    @Nullable
    public String getProject() {
        String projectId = tryGetProject();
        if (projectId == null) {
            throw new IllegalArgumentException(
                    "Could not detect Google Cloud project id from the environment. Please specify a project id.");
        }
        return projectId;
    }

    @Nullable
    public String getServiceFilePath() {
        if (containsMacro(NAME_SERVICE_ACCOUNT_FILE_PATH) || serviceFilePath == null ||
                serviceFilePath.isEmpty() || AUTO_DETECT.equals(serviceFilePath)) {
            return null;
        }
        return serviceFilePath;
    }

    public String getBufferSize() {
        return bufferSize;
    }

    public String getPolicyUrl() {
        return policyUrl;
    }

    public String getIdentity() {
        return identity;
    }

    public String getSharedSecret() {
        return sharedSecret;
    }

    public String getTrustStorePath() {
        return trustStorePath;
    }

    public String getCachePath() {
        return cachePath;
    }

    public String getFormat() {
        return format;
    }

    public String getIgnoreHeader() {
        return ignoreHeader;
    }

    public String getFieldList() {
        return fieldList;
    }

    @Nullable
    public String getProxy() {
        return proxy;
    }

    public String getProxyType() {
        return proxyType;
    }

    public boolean useProxy() {
        if (Strings.isNullOrEmpty(proxyType) || proxyType.equals(ProxyType.NONE.getType()))
            return false;

        return true;
    }

    public String getDestinationPath() {
        String destinationPath = GCSPath.from(path).getName();
        if (StringUtils.isNotEmpty(suffix)) {
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
                throw new IllegalArgumentException("Error while processing suffix.");

            }
        }
        return destinationPath;
    }

    public String getBucket() {
        return GCSPath.from(path).getBucket();
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

    private enum ProxyType {
        HTTP("HTTP"),
        SOCKS("SOCKS"),
        NONE("NONE");
        private String type;

        ProxyType(String type) {
            this.type = type;
        }

        String getType() {
            return type;
        }
    }
}
