package io.cdap.plugin.file.ingest.encryption;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.cdap.plugin.file.ingest.utils.FileMetaData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.*;
import org.bouncycastle.openpgp.operator.jcajce.JcePGPDataEncryptorBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePublicKeyKeyEncryptionMethodGenerator;

import java.io.*;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * @author Vikas K  Created On 09/11/19
 **/
public class FileCompressEncrypt {
    static Storage storage = null;
    static Configuration conf;

    static {
        conf = new Configuration();

        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    }

    public FileCompressEncrypt(String pathToConfig, String projectId) throws IOException {
        Credentials credentials = GoogleCredentials.fromStream(new FileInputStream(pathToConfig));
        FileCompressEncrypt.storage =
                StorageOptions.newBuilder()
                        .setCredentials(credentials)
                        .setProjectId(projectId)
                        .build()
                        .getService();
    }

    private static void encryptFile(OutputStream out,
                                    FileMetaData fileMetaData,
                                    boolean compressFile, boolean encryptFile, PGPPublicKey encKey,
                                    Integer bufferSize, boolean armor,
                                    boolean withIntegrityCheck) throws IOException, NoSuchProviderException {
        if (compressFile && encryptFile) {
            compressAndEncryptFile(out, fileMetaData, encKey, bufferSize, armor, withIntegrityCheck);
        } else if (compressFile) {
            compressOnly(out, fileMetaData, bufferSize);
        } else if (encryptFile) {
            encryptOnly(out, fileMetaData, encKey, bufferSize, armor, withIntegrityCheck);
        } else {
            noCompressNoEncrypt(out, fileMetaData, bufferSize);
        }
    }

    private static void compressOnly(OutputStream out, FileMetaData fileMetaData, Integer bufferSize) throws IOException, NoSuchProviderException {
        InputStream inputStream = fileMetaData.getFileSystem().open(fileMetaData.getPath());

        try (ZipOutputStream zipOutputStream = new ZipOutputStream(out)) {
            zipOutputStream.setMethod(8);
            zipOutputStream.setLevel(5);

            ZipEntry zipEntry = new ZipEntry(fileMetaData.getPath().getName());
            zipOutputStream.putNextEntry(zipEntry);

            byte[] buffer = new byte[bufferSize];
            int size;
            while ((size = inputStream.read(buffer)) > 0) {
                zipOutputStream.write(buffer, 0, size);
            }
        }
        inputStream.close();
    }

    private static void encryptOnly(OutputStream out, FileMetaData fileMetaData, PGPPublicKey encKey, Integer bufferSize, boolean armor, boolean withIntegrityCheck) throws IOException, NoSuchProviderException {
        if (armor) {
            out = new ArmoredOutputStream(out);
        }

        try {
            PGPEncryptedDataGenerator cPk = new PGPEncryptedDataGenerator(new JcePGPDataEncryptorBuilder(PGPEncryptedData.CAST5).setWithIntegrityPacket(withIntegrityCheck).setSecureRandom(new SecureRandom()).setProvider(new BouncyCastleProvider()));

            cPk.addMethod(new JcePublicKeyKeyEncryptionMethodGenerator(encKey).setProvider(new BouncyCastleProvider()));

            OutputStream cOut = cPk.open(out, new byte[bufferSize]);

            //PGPUtil.writeFileToLiteralData(cOut, PGPLiteralData.BINARY, new File(fileName), new byte[1 << 16]);
            writeFileToLiteralData(cOut, PGPLiteralData.BINARY, fileMetaData, new byte[bufferSize]);

            cOut.close();

            if (armor) {
                out.close();
            }
        } catch (PGPException e) {
            System.err.println(e);
            if (e.getUnderlyingException() != null) {
                e.getUnderlyingException().printStackTrace();
            }
        }
    }

    private static void noCompressNoEncrypt(OutputStream out, FileMetaData fileMetaData, Integer bufferSize) throws IOException, NoSuchProviderException {
        InputStream inputStream = fileMetaData.getFileSystem().open(fileMetaData.getPath());

        byte[] buffer = new byte[bufferSize];

        int size;
        while ((size = inputStream.read(buffer)) > 0) {
            out.write(buffer, 0, size);
        }

        inputStream.close();
    }

    private static void compressAndEncryptFile(
            OutputStream out,
            FileMetaData fileMetaData,
            PGPPublicKey encKey,
            Integer bufferSize,
            boolean armor,
            boolean withIntegrityCheck)
            throws IOException, NoSuchProviderException {
        if (armor) {
            out = new ArmoredOutputStream(out);
        }

        try {
            PGPEncryptedDataGenerator cPk = new PGPEncryptedDataGenerator(new JcePGPDataEncryptorBuilder(PGPEncryptedData.CAST5).setWithIntegrityPacket(withIntegrityCheck).setSecureRandom(new SecureRandom()).setProvider(new BouncyCastleProvider()));

            cPk.addMethod(new JcePublicKeyKeyEncryptionMethodGenerator(encKey).setProvider(new BouncyCastleProvider()));

            OutputStream cOut = cPk.open(out, new byte[bufferSize]);

            PGPCompressedDataGenerator comData = new PGPCompressedDataGenerator(PGPCompressedData.ZIP);

            //PGPUtil.writeFileToLiteralData(comData.open(cOut), PGPLiteralData.BINARY, new File(fileName), new byte[1 << 16]);
            writeFileToLiteralData(comData.open(cOut), PGPLiteralData.BINARY, fileMetaData, new byte[bufferSize]);
            comData.close();

            cOut.close();

            if (armor) {
                out.close();
            }
        } catch (PGPException e) {
            System.err.println(e);
            if (e.getUnderlyingException() != null) {
                e.getUnderlyingException().printStackTrace();
            }
        }
    }

    public static void writeFileToLiteralData(OutputStream var0, char var1, FileMetaData fileMetaData, byte[] var3) throws IOException {
        PGPLiteralDataGenerator var4 = new PGPLiteralDataGenerator();
        OutputStream var5 = var4.open(var0, var1, fileMetaData.getPath().getName(), new Date(fileMetaData.getLastModifiedTime()), var3);
        pipeFileContents(fileMetaData, var5, var3.length);
    }

    private static void pipeFileContents(FileMetaData var0, OutputStream var1, int var2) throws IOException {
        //FileInputStream var3 = new FileInputStream(var0);
        FSDataInputStream var3 = var0.getFileSystem().open(var0.getPath());
        byte[] var4 = new byte[var2];

        int var5;
        while ((var5 = var3.read(var4)) > 0) {
            var1.write(var4, 0, var5);
        }

        var1.close();
        var3.close();
    }

    public static InputStream gcsWriter(FileMetaData fileMetaData, boolean compressFile, boolean encryptFile, PGPPublicKey encKey, Integer bufferSize) throws IOException {
        PipedOutputStream outPipe = new PipedOutputStream();
        PipedInputStream inPipe = new PipedInputStream();
        inPipe.connect(outPipe);

        new Thread(
                () -> {
                    try {
                        encryptFile(outPipe, fileMetaData, compressFile, encryptFile, encKey, bufferSize, false, true);
                        //Thread.sleep(10000);
                        //outPipe.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (NoSuchProviderException e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            outPipe.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                })
                .start();
        return inPipe;
    }
}
