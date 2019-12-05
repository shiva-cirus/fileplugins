package io.cdap.plugin.file.ingest.config;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.common.ReferencePluginConfig;

import javax.annotation.Nullable;

/** Configurations required for connecting to local filesystems. */
public class FileDecompressDecryptSourceConfig extends ReferencePluginConfig {

  @Macro
  @Description("Collection of sourcePaths separated by \",\" to read files from")
  public String sourcePaths;

  @Macro
  @Description("The number of files each split reads in")
  public Integer maxSplitSize;

  @Description("Whether or not to copy recursively")
  public Boolean recursiveCopy;

  public static final String NAME_SCHEMA = "schema";

  @Description("Scheme of the source filesystem.")
  public String scheme;

  @Macro
  @Description("Private key File Path.")
  public String privateKeyFilePath;

  @Macro
  @Description("Password of the private key")
  public String password;

  @Description("Decompression Format")
  public String decompressionFormat;

  @Description("Decryption Algorithm ")
  public String decryptionAlgorithm;

  @Description("File  Format ")
  public String format;

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

  public FileDecompressDecryptSourceConfig(
      String name, String sourcePaths, Integer maxSplitSize, String scheme,String privateKeyFilePath,String password) {
    super(name);
    this.sourcePaths = sourcePaths;
    this.maxSplitSize = maxSplitSize;
    this.scheme = scheme;
    this.privateKeyFilePath = privateKeyFilePath;
    this.password = password;
  }

  public boolean decompressFile() {
    if (Strings.isNullOrEmpty(decompressionFormat)
        || decompressionFormat.equals(DecompressorType.NONE.getType())) return false;
    return true;
  }

  public boolean decryptFile() {
    if (Strings.isNullOrEmpty(decryptionAlgorithm)
        || decryptionAlgorithm.equals(DecryptionType.NONE.getType())) return false;
    return true;
  }

  public void validate() {
    if (!this.containsMacro("maxSplitSize")) {
      if (maxSplitSize <= 0) {
        throw new IllegalArgumentException("Max split size must be a positive integer.");
      }
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
