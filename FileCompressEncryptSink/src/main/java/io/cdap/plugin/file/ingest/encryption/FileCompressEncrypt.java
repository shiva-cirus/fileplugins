package io.cdap.plugin.file.ingest.encryption;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.commons.io.IOUtils;
import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.*;
import org.bouncycastle.openpgp.jcajce.JcaPGPObjectFactory;
import org.bouncycastle.openpgp.operator.jcajce.JcaKeyFingerprintCalculator;
import org.bouncycastle.openpgp.operator.jcajce.JcePGPDataEncryptorBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePublicKeyDataDecryptorFactoryBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePublicKeyKeyEncryptionMethodGenerator;
import org.bouncycastle.util.io.Streams;

import java.io.*;
import java.nio.ByteBuffer;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.Security;
import java.util.Date;
import java.util.Iterator;

/**
 * @author Vikas K  Created On 09/11/19
 **/

public class FileCompressEncrypt {
    private static final int INT = 1 << 16;
    static Storage storage = null;

    public FileCompressEncrypt(String pathToConfig, String projectId) throws IOException {
        Credentials credentials = GoogleCredentials.fromStream(new FileInputStream(pathToConfig));
        FileCompressEncrypt.storage =
                StorageOptions.newBuilder()
                        .setCredentials(credentials)
                        .setProjectId(projectId)
                        .build()
                        .getService();
    }

    private static void decryptFile(
            String inputFileName,
            String keyFileName,
            char[] passwd,
            String defaultFileName)
            throws IOException, NoSuchProviderException {
        InputStream in = new BufferedInputStream(new FileInputStream(inputFileName));
        InputStream keyIn = new BufferedInputStream(new FileInputStream(keyFileName));
        decryptFile(in, keyIn, passwd, defaultFileName);
        keyIn.close();
        in.close();
    }

    /**
     * decrypt the passed in message stream
     */
    private static void decryptFile(
            InputStream in,
            InputStream keyIn,
            char[] passwd,
            String defaultFileName)
            throws IOException, NoSuchProviderException {
        in = PGPUtil.getDecoderStream(in);

        try {
            JcaPGPObjectFactory pgpF = new JcaPGPObjectFactory(in);
            PGPEncryptedDataList enc;

            Object o = pgpF.nextObject();
            //
            // the first object might be a PGP marker packet.
            //
            if (o instanceof PGPEncryptedDataList) {
                enc = (PGPEncryptedDataList) o;
            } else {
                enc = (PGPEncryptedDataList) pgpF.nextObject();
            }

            //
            // find the secret key
            //
            Iterator it = enc.getEncryptedDataObjects();
            PGPPrivateKey sKey = null;
            PGPPublicKeyEncryptedData pbe = null;
            PGPSecretKeyRingCollection pgpSec = new PGPSecretKeyRingCollection(
                    PGPUtil.getDecoderStream(keyIn), new JcaKeyFingerprintCalculator());

            while (sKey == null && it.hasNext()) {
                pbe = (PGPPublicKeyEncryptedData) it.next();

                sKey = PGPExampleUtil.findSecretKey(pgpSec, pbe.getKeyID(), passwd);
            }

            if (sKey == null) {
                throw new IllegalArgumentException("secret key for message not found.");
            }

            InputStream clear = pbe.getDataStream(new JcePublicKeyDataDecryptorFactoryBuilder().setProvider(new BouncyCastleProvider()).build(sKey));

            JcaPGPObjectFactory plainFact = new JcaPGPObjectFactory(clear);

            PGPCompressedData cData = (PGPCompressedData) plainFact.nextObject();

            InputStream compressedStream = new BufferedInputStream(cData.getDataStream());
            JcaPGPObjectFactory pgpFact = new JcaPGPObjectFactory(compressedStream);

            Object message = pgpFact.nextObject();

            if (message instanceof PGPLiteralData) {
                PGPLiteralData ld = (PGPLiteralData) message;

                String outFileName = ld.getFileName();
                if (outFileName.length() == 0) {
                    outFileName = defaultFileName;
                }

                InputStream unc = ld.getInputStream();
                OutputStream fOut = new BufferedOutputStream(new FileOutputStream(outFileName));

                Streams.pipeAll(unc, fOut);

                fOut.close();
            } else if (message instanceof PGPOnePassSignatureList) {
                throw new PGPException("encrypted message contains a signed message - not literal data.");
            } else {
                throw new PGPException("message is not a simple encrypted file - type unknown.");
            }

            if (pbe.isIntegrityProtected()) {
                if (!pbe.verify()) {
                    System.err.println("message failed integrity check");
                } else {
                    System.err.println("message integrity check passed");
                }
            } else {
                System.err.println("no message integrity check");
            }
        } catch (PGPException e) {
            System.err.println(e);
            if (e.getUnderlyingException() != null) {
                e.getUnderlyingException().printStackTrace();
            }
        }
    }

    private static void encryptFile(
            OutputStream out,
            String fileName,
            PGPPublicKey encKey,
            boolean armor,
            boolean withIntegrityCheck)
            throws IOException, NoSuchProviderException {
        if (armor) {
            out = new ArmoredOutputStream(out);
        }

        try {
            PGPEncryptedDataGenerator cPk = new PGPEncryptedDataGenerator(new JcePGPDataEncryptorBuilder(PGPEncryptedData.CAST5).setWithIntegrityPacket(withIntegrityCheck).setSecureRandom(new SecureRandom()).setProvider(new BouncyCastleProvider()));

            cPk.addMethod(new JcePublicKeyKeyEncryptionMethodGenerator(encKey).setProvider(new BouncyCastleProvider()));

            OutputStream cOut = cPk.open(out, new byte[1 << 16]);

            PGPCompressedDataGenerator comData = new PGPCompressedDataGenerator(
                    PGPCompressedData.ZIP);

            PGPUtil.writeFileToLiteralData(comData.open(cOut), PGPLiteralData.BINARY, new File(fileName), new byte[1 << 16]);

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

    private static void decryption(String inFile) throws IOException, NoSuchProviderException {
        String privateKeyPassword = "passphrase";
        decryptFile(inFile, "PGP1D0.skr", privateKeyPassword.toCharArray(), "abc11.txt");
    }

    public static OutputStream writeFileToLiteralData(OutputStream var0, char var1, File var2, byte[] var3) throws IOException {
        PGPLiteralDataGenerator var4 = new PGPLiteralDataGenerator();
        OutputStream var5 = var4.open(var0, var1, var2.getName(), new Date(var2.lastModified()), var3);
        return pipeFileContents(var2, var5, var3.length);
    }

    private static OutputStream pipeFileContents(File var0, OutputStream var1, int var2) throws IOException {
        FileInputStream var3 = new FileInputStream(var0);
        byte[] var4 = new byte[var2];

        /*int var5;
        while ((var5 = var3.read(var4)) > 0) {
            var1.write(var4, 0, var5);
        }*/
        IOUtils.copy(var3, var1);
        //var1.close();
        var3.close();


        return var1;
    }

    public static InputStream gcsWriter(String inFileName, PGPPublicKey encKey) throws IOException {
        //InputStream inputStream = new FileInputStream(inFileName);
        PipedOutputStream outPipe = new PipedOutputStream();
        PipedInputStream inPipe = new PipedInputStream();
        inPipe.connect(outPipe);

        new Thread(
                () -> {
                    try {
                        encryptFile(outPipe, inFileName, encKey, false, true);
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

    public static void encryptionCompressAndUploadGCSWithThread(String inFileName, String bucketName, String uploadFileName) throws IOException, NoSuchProviderException, PGPException {


        PGPPublicKey encKey = PGPExampleUtil.readPublicKey("PGP1D0.pkr");

        BlobId blobId = BlobId.of(bucketName, uploadFileName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/pgp-encrypted").build();
        InputStream inputStream = gcsWriter(inFileName, encKey);
        /*try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
        byte[] buffer = new byte[INT];
        try (WriteChannel writer =
                     storage.writer(blobInfo)) {
            int limit;
            while ((limit = inputStream.read(buffer)) >= 0) {
                System.out.println("upload file " + limit);
                writer.write(ByteBuffer.wrap(buffer, 0, limit));
            }
        }
        inputStream.close();


    }

    private static void encOnLocalFileSys(String inFileName, String outFileName) throws IOException, NoSuchProviderException, PGPException {


        PGPPublicKey encKey = PGPExampleUtil.readPublicKey("PGP1D0.pkr");

        //InputStream inputStream = gcsWriter( inFileName, encKey);

        encryptFile(new FileOutputStream(new File(outFileName)), inFileName, encKey, false, true);


    }

    public static void main(
            String[] args)
            throws Exception {
        Security.addProvider(new BouncyCastleProvider());
        FileCompressEncrypt googleCloudStorage =
                new FileCompressEncrypt("rugged-alloy-254904-08590d697da8.json", "rugged-alloy-254904");

        //encOnLocalFileSys("input/pkg2_vikas.csv", "output/pkg2_vikas.csv.asc");

        String inFileName = "input/pkg1_vikas.csv";
        String bucketName = "cdap_vikas";
        String uploadFileName = "pkg1_vikas.csv.asc";

        encryptionCompressAndUploadGCSWithThread(inFileName, bucketName, uploadFileName);


        //decryption("/Users/vikaskumar/Downloads/largefile_2gb_cp.asc");
        //decryption("output/pkg2_vikas.csv.asc");

    }
}
