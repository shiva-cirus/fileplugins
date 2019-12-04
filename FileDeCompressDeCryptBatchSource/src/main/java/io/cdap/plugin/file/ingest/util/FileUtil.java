package io.cdap.plugin.file.ingest.util;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.*;
import org.bouncycastle.openpgp.jcajce.JcaPGPObjectFactory;
import org.bouncycastle.openpgp.operator.jcajce.JcaKeyFingerprintCalculator;
import org.bouncycastle.openpgp.operator.jcajce.JcePublicKeyDataDecryptorFactoryBuilder;

import java.io.*;
import java.security.NoSuchProviderException;
import java.security.Security;
import java.util.Iterator;

public class FileUtil {

  /** decrypt and decompress the passed in message stream */
  public static InputStream decryptAndDecompress(
      String inputFileName, String keyFileName, char[] privateKeyPassword)
      throws IOException, NoSuchProviderException, PGPException {
    InputStream in = new BufferedInputStream(new FileInputStream(inputFileName));
    InputStream keyIn = new BufferedInputStream(new FileInputStream(keyFileName));

    in = PGPUtil.getDecoderStream(in);

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
    PGPSecretKeyRingCollection pgpSec =
        new PGPSecretKeyRingCollection(
            PGPUtil.getDecoderStream(keyIn), new JcaKeyFingerprintCalculator());

    while (sKey == null && it.hasNext()) {
      pbe = (PGPPublicKeyEncryptedData) it.next();

      sKey = PGPCertUtil.findSecretKey(pgpSec, pbe.getKeyID(), privateKeyPassword);
    }

    if (sKey == null) {
      throw new IllegalArgumentException("secret key for message not found.");
    }

    InputStream clear =
        pbe.getDataStream(
            new JcePublicKeyDataDecryptorFactoryBuilder()
                .setProvider(new BouncyCastleProvider())
                .build(sKey));
    keyIn.close();
    JcaPGPObjectFactory plainFact = new JcaPGPObjectFactory(clear);

    PGPCompressedData cData = (PGPCompressedData) plainFact.nextObject();

    InputStream compressedStream = new BufferedInputStream(cData.getDataStream());
    JcaPGPObjectFactory pgpFact = new JcaPGPObjectFactory(compressedStream);

    Object message = pgpFact.nextObject();

    if (message instanceof PGPLiteralData) {
      PGPLiteralData ld = (PGPLiteralData) message;
      InputStream unc = ld.getInputStream();
      return unc;
    } else if (message instanceof PGPOnePassSignatureList) {
      throw new PGPException("encrypted message contains a signed message - not literal data.");
    } else {
      throw new PGPException("message is not a simple encrypted file - type unknown.");
    }
  }
  /** decrypt */
  public static InputStream decrypt(
      String inputFileName, String keyFileName, char[] privateKeyPassword)
      throws IOException, NoSuchProviderException, PGPException {
    InputStream in = new BufferedInputStream(new FileInputStream(inputFileName));
    InputStream keyIn = new BufferedInputStream(new FileInputStream(keyFileName));

    in = PGPUtil.getDecoderStream(in);

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
    PGPSecretKeyRingCollection pgpSec =
        new PGPSecretKeyRingCollection(
            PGPUtil.getDecoderStream(keyIn), new JcaKeyFingerprintCalculator());

    while (sKey == null && it.hasNext()) {
      pbe = (PGPPublicKeyEncryptedData) it.next();

      sKey = PGPCertUtil.findSecretKey(pgpSec, pbe.getKeyID(), privateKeyPassword);
    }

    if (sKey == null) {
      throw new IllegalArgumentException("secret key for message not found.");
    }

    InputStream clear =
        pbe.getDataStream(
            new JcePublicKeyDataDecryptorFactoryBuilder()
                .setProvider(new BouncyCastleProvider())
                .build(sKey));
    keyIn.close();
    JcaPGPObjectFactory plainFact = new JcaPGPObjectFactory(clear);

    Object message = plainFact.nextObject();

    if (message instanceof PGPLiteralData) {
      PGPLiteralData ld = (PGPLiteralData) message;
      InputStream decryptStream = ld.getInputStream();
      return decryptStream;
    } else if (message instanceof PGPOnePassSignatureList) {
      throw new PGPException("encrypted message contains a signed message - not literal data.");
    } else {
      throw new PGPException("message is not a simple encrypted file - type unknown.");
    }
  }

  public static void main(String[] args) throws Exception {
    Security.addProvider(new BouncyCastleProvider());
    String filePath = "/Users/aca/Desktop/Pawan/cdap/data/vcp8/csv/domain_master.csv.zip.pgp";
    String keyFileName =
        "/Users/aca/Desktop/Pawan/cdap/plugin/fileplugins/FileDeCompressDeCryptBatchSource/PGP1D0.skr";
    String privateKeyPassword = "passphrase";
    InputStream inputStream =
        decryptAndDecompress(filePath, keyFileName, privateKeyPassword.toCharArray());
    try {
      InputStreamReader isReader = new InputStreamReader(inputStream);
      // Creating a BufferedReader object
      BufferedReader reader = new BufferedReader(isReader);

      CSVParser csvParser =
          new CSVParser(
              reader,
              CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim());
      for (CSVRecord csvRecord : csvParser) {
        System.out.println(csvRecord.get("body1"));
        System.out.println(csvRecord.get("body2"));
      }

      inputStream.close();
      isReader.close();
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}