package test;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import org.apache.commons.io.IOUtils;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.zip.GZIPOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

/** Simple class for creating, reading and modifying text blobs on Google Cloud */
public class GoogleCloudStoragePOC {
  private static final Logger log = LoggerFactory.getLogger(GoogleCloudStoragePOC.class);
  private static Storage storage;
  private Bucket bucket;
  public static String encryptionKey ="";

  public static void main(String[] args) throws Exception {

    // Use this variation to read the Google authorization JSON from the resources directory with a
    // path
    // and a project name.
    GoogleCloudStoragePOC googleCloudStorage =
        new GoogleCloudStoragePOC("rugged-alloy-254904-08590d697da8.json", "rugged-alloy-254904");

    // Bucket require globally unique names, so you'll probably need to change this
    Bucket bucket = googleCloudStorage.getBucket("pkg_test");

    Class clazz = GoogleCloudStoragePOC.class;
    InputStream inputStream = clazz.getResourceAsStream("/1.csv");
    uploadToStorageApproach1(gzipInputStream(inputStream));
    uploadToStorageApproach2();
    uploadToStorageApproach3(gzipInputStream(inputStream));
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
            })
        .start();
    return inPipe;
  }

  private static void uploadToStorageApproach1(InputStream fileInputStream)
          throws IOException {
    BlobId blobId = BlobId.of("pkg_test", "approach1.csv.gz");

    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/gzip").build();
    try {
      encryptionKey = FileEncrptKeyUtil.getEncyptedKey();
    } catch (Exception e) {
      e.printStackTrace();
    }

    // For big files:
    // When content is not available or large (1MB or more) it is recommended to write it in chunks
    // via the blob's channel writer.
    try (WriteChannel writer =
                 storage.writer(blobInfo, Storage.BlobWriteOption.encryptionKey(encryptionKey))) {
      byte[] buffer = new byte[10_240];
      try (InputStream input = fileInputStream) {
        int limit;
        while ((limit = input.read(buffer)) >= 0) {
          System.out.println("upload file " + limit);
          writer.write(ByteBuffer.wrap(buffer, 0, limit));
        }
      }
    }
  }

  private static void uploadToStorageApproach2()
          throws IOException {
    BlobId blobId = BlobId.of("pkg_test", "a5.csv.gz");

    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/gzip").build();
    String privateKeyPassword = "passphrase";
    InputStream fileInputStream=null;
    try {
      File publicKeyFile = new File("PGP1D0.pkr");
      PGPPublicKey pgpPublicKey = FileEncryptTest.readPublicKeyFromCol(new FileInputStream(publicKeyFile));
      char[] passPhrase = privateKeyPassword.toCharArray();
      String inputFileName =
              "/Users/aca/Desktop/Pawan/cdap/plugin/fileplugins/FileCompressEncryptSink/src/main/resources/1.csv";
      fileInputStream= FileEncryptTest.encryptFile(inputFileName,pgpPublicKey,passPhrase,false,true);
    } catch (Exception e) {
      e.printStackTrace();
    }
    // For big files:
    // When content is not available or large (1MB or more) it is recommended to write it in chunks
    // via the blob's channel writer.
    try (WriteChannel writer =
                 storage.writer(blobInfo)) {
      byte[] buffer = new byte[10_240];
      try (InputStream input = fileInputStream) {
        int limit;
        while ((limit = input.read(buffer)) >= 0) {
          System.out.println("upload file " + limit);
          writer.write(ByteBuffer.wrap(buffer, 0, limit));
        }
      }
    }
  }
  private static void uploadToStorageApproach3(InputStream fileInputStream)
          throws IOException {
    BlobId blobId = BlobId.of("pkg_test", "approach3.csv.gz");

    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/gzip").build();
    try {
      PGPSecretKey pgpSecretKey = FileEncrptKeyUtil.getPGPKey();
      encryptionKey = Base64.getEncoder().encodeToString(pgpSecretKey.getPublicKey().getEncoded());
    } catch (Exception e) {
      e.printStackTrace();
    }

    // For big files:
    // When content is not available or large (1MB or more) it is recommended to write it in chunks
    // via the blob's channel writer.
    try (WriteChannel writer =
                 storage.writer(blobInfo, Storage.BlobWriteOption.encryptionKey(encryptionKey))) {
      byte[] buffer = new byte[10_240];
      try (InputStream input = fileInputStream) {
        int limit;
        while ((limit = input.read(buffer)) >= 0) {
          System.out.println("upload file " + limit);
          writer.write(ByteBuffer.wrap(buffer, 0, limit));
        }
      }
    }
  }

  // Use path and project name
  private GoogleCloudStoragePOC(String pathToConfig, String projectId) throws IOException {

    Credentials credentials = GoogleCredentials.fromStream(new FileInputStream(pathToConfig));
    storage =
        StorageOptions.newBuilder()
            .setCredentials(credentials)
            .setProjectId(projectId)
            .build()
            .getService();
  }

  // Check for bucket existence and create if needed.
  private Bucket getBucket(String bucketName) {
    bucket = storage.get(bucketName);
    if (bucket == null) {
      System.out.println("Creating new bucket.");
      bucket = storage.create(BucketInfo.of(bucketName));
    }
    return bucket;
  }

  // Save a string to a blob
  private BlobId saveString(String blobName, String value, Bucket bucket) {
    byte[] bytes = value.getBytes(UTF_8);
    Blob blob = bucket.create(blobName, bytes);
    return blob.getBlobId();
  }
}
