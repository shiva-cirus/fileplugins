package io.cdap.plugin.file.ingest.util;

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
  public static InputStream decryptionAndDecompress(String inFile, String keyFileName, String privateKeyPassword)
      throws IOException, NoSuchProviderException, PGPException {
   return decryptFile(inFile, keyFileName, privateKeyPassword.toCharArray());
  }

  private static InputStream decryptFile(String inputFileName, String keyFileName, char[] passwd)
      throws IOException, NoSuchProviderException, PGPException {
    InputStream in = new BufferedInputStream(new FileInputStream(inputFileName));
    InputStream keyIn = new BufferedInputStream(new FileInputStream(keyFileName));
    InputStream unCompressed= decryptFile(in, keyIn, passwd);
    keyIn.close();
    return unCompressed;
  }
  /** decrypt the passed in message stream */
  private static InputStream decryptFile(InputStream in, InputStream keyIn, char[] passwd)
      throws IOException, NoSuchProviderException, PGPException {
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

      sKey = PGPCertUtil.findSecretKey(pgpSec, pbe.getKeyID(), passwd);
    }

    if (sKey == null) {
      throw new IllegalArgumentException("secret key for message not found.");
    }

    InputStream clear =
        pbe.getDataStream(
            new JcePublicKeyDataDecryptorFactoryBuilder()
                .setProvider(new BouncyCastleProvider())
                .build(sKey));

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

  public static void main(String[] args) throws Exception {
    Security.addProvider(new BouncyCastleProvider());
    String filePath =
        "/Users/aca/Desktop/Pawan/cdap/data/vcp8/enc_compress/_2019-11-27-12-30_domain_master.csv.zip.pgp";
    String keyFileName = "PGP1D0.skr";
    String privateKeyPassword = "passphrase";
    InputStream inputStream=decryptionAndDecompress(filePath, keyFileName, privateKeyPassword);
      try {
          InputStreamReader isReader = new InputStreamReader(inputStream);
          // Creating a BufferedReader object
          BufferedReader reader = new BufferedReader(isReader);

          String line = reader.readLine();

          while (line != null) {
              // read next line
              System.out.println(line);
              line = reader.readLine();
          }
          inputStream.close();
          isReader.close();
          reader.close();
      } catch (IOException e) {
          e.printStackTrace();
      }

  }
}
