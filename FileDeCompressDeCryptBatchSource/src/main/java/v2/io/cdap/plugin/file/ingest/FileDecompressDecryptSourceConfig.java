package v2.io.cdap.plugin.file.ingest;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;


import javax.annotation.Nullable;

public class FileDecompressDecryptSourceConfig extends PluginConfig {


    public static final String NAME_SCHEMA = "schema";
    public static final String NAME_SCHEME = "scheme";
    public static final String NAME_SOURCE_PATH = "sourcePaths";
    public static final String NAME_FILE_FORMAT = "format";
    public static final String NAME_SPLIT_SIZE = "maxSplitSize";
    public static final String NAME_RECURSIVE_COPY = "recursiveCopy";
    public static final String NAME_DECRYPTION = "decryptionAlgorithm";
    public static final String NAME_DECOMPRESSION = "decompressionFormat";
    public static final String NAME_PRIVATE_KEY = "privateKeyFilePath";
    public static final String NAME_PRIVATE_KEY_PASS = "password";



    @Name(NAME_SCHEME)
    @Description("Scheme of the source filesystem.")
    public String scheme = SchemeType.file.getType();

    @Name(NAME_SOURCE_PATH)
    @Macro
    @Description("Collection of sourcePaths separated by \",\" to read files from")
    public String sourcePaths;


    @Name(NAME_FILE_FORMAT)
    @Description("File Format ")
    public String format = FileFormatType.csv.getType();

    @Name(NAME_SPLIT_SIZE)
    @Macro
    @Description("The number of files each split reads in")
    public Integer maxSplitSize;

    @Name(NAME_RECURSIVE_COPY)
    @Description("Read Files Recursively from Source Paths")
    public String recursiveCopyFile = RecursiveCopyType.F.getType();
    public Boolean recursiveCopy;


    @Description("File to be Decrypted or not")
    public Boolean decrypt;

    @Name(NAME_DECRYPTION)
    @Description("Decryption Algorithm ")
    public String decryptionAlgorithm;

    @Name(NAME_PRIVATE_KEY)
    @Macro
    @Description("Private key File Path.")
    public String privateKeyFilePath;

    @Name(NAME_PRIVATE_KEY_PASS)
    @Macro
    @Description("Password of the private key")
    public String password;




    @Description("File to be decompress or not")
    public Boolean decompress;

    @Name(NAME_DECOMPRESSION)
    @Description("Decompression Format")
    public String decompressionFormat;



    @Nullable
    @Description(
            "Output schema for the source. Formats like 'avro' and 'parquet' require a schema in order to "
                    + "read the data.")
    private String schema;






    @Nullable
    public Schema getSchema() {
        try {
            return containsMacro(NAME_SCHEMA) || Strings.isNullOrEmpty(schema)
                    ? null
                    : Schema.parseJson(schema);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid schema: " + e.getMessage(), e);
        }
    }


    public FileDecompressDecryptSourceConfig(String scheme, String sourcePaths, String format, Integer maxSplitSize, String recursiveCopyFile,  String decryptionAlgorithm, String privateKeyFilePath, String password,  String decompressionFormat, @Nullable String schema) {
        this.scheme = scheme;
        this.sourcePaths = sourcePaths;
        this.format = format;
        this.maxSplitSize = maxSplitSize;
        this.recursiveCopyFile=recursiveCopyFile;
        this.decryptionAlgorithm = decryptionAlgorithm;
        this.privateKeyFilePath = privateKeyFilePath;
        this.password = password;
        this.decompressionFormat = decompressionFormat;
        this.schema = schema;
    }


    public boolean decompressFile() {
        if (Strings.isNullOrEmpty(decompressionFormat) || decompressionFormat.equals(DecompressorType.NONE.getType()))
            return false;
        return true;
    }

    public boolean decryptFile() {
        if (Strings.isNullOrEmpty(decryptionAlgorithm) || decryptionAlgorithm.equals(DecryptionType.NONE.getType()))
            return false;
        return true;
    }


    public boolean recursiveCopy() {
        if (Strings.isNullOrEmpty(recursiveCopyFile) || recursiveCopyFile.equals(RecursiveCopyType.F.getType()))
            return false;
        return true;
    }


    private enum RecursiveCopyType {
        T("true"),
        F("false");
        private String type;

        RecursiveCopyType(String type) {
            this.type = type;
        }

        String getType() {
            return type;
        }
    }





    private enum SchemeType {
        file("file"),
        hdfs("hdfs");
        private String type;

        SchemeType(String type) {
            this.type = type;
        }

        String getType() {
            return type;
        }
    }

    private enum FileFormatType {
        csv("csv");
        private String type;

        FileFormatType(String type) {
            this.type = type;
        }

        String getType() {
            return type;
        }
    }


    private enum DecompressorType {
        ZIP("ZIP"),
        NONE("NONE");
        private String type;

        DecompressorType(String type) {
            this.type = type;
        }

        String getType() {
            return type;
        }
    }

    private enum DecryptionType {
        PGP("PGP"),
        NONE("NONE");
        private String type;

        DecryptionType(String type) {
            this.type = type;
        }

        String getType() {
            return type;
        }
    }


}






