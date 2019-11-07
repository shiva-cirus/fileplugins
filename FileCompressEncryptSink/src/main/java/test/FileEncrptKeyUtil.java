package test;

import org.bouncycastle.bcpg.HashAlgorithmTags;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.*;
import org.bouncycastle.openpgp.operator.PGPDigestCalculator;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPContentSignerBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPDigestCalculatorProviderBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPKeyPair;
import org.bouncycastle.openpgp.operator.jcajce.JcePBESecretKeyEncryptorBuilder;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.File;
import java.io.IOException;
import java.security.*;
import java.util.Base64;
import java.util.Date;

public class FileEncrptKeyUtil {

  private static File publicKeyFile = new File("PGP1D0.pkr");
  private static File privateKeyFile = new File("PGP1D0.skr");
  private static String privateKeyPassword = "passphrase";
  private static String identity = "tommybee";
  private static boolean isAll = true;

  public static String getEncyptedKey() throws NoSuchAlgorithmException {
    KeyGenerator keyGen = KeyGenerator.getInstance("AES");
    keyGen.init(256);
    SecretKey secretKey = keyGen.generateKey();
    String encryption_key = Base64.getEncoder().encodeToString(secretKey.getEncoded());
    return encryption_key;
  }

  public static PGPSecretKey getPGPKey()
      throws IOException, NoSuchProviderException, PGPException, NoSuchAlgorithmException {

    Security.addProvider(new BouncyCastleProvider());

    KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA", "BC");

    kpg.initialize(1024);

    KeyPair kp = kpg.generateKeyPair();
    PGPDigestCalculator sha1Calc =
        new JcaPGPDigestCalculatorProviderBuilder().build().get(HashAlgorithmTags.SHA1);
    PGPKeyPair keyPair = new JcaPGPKeyPair(PGPPublicKey.RSA_GENERAL, kp, new Date());
    PGPSecretKey secretKey =
        new PGPSecretKey(
            PGPSignature.DEFAULT_CERTIFICATION,
            keyPair,
            identity,
            sha1Calc,
            null,
            null,
            new JcaPGPContentSignerBuilder(
                keyPair.getPublicKey().getAlgorithm(), HashAlgorithmTags.SHA1),
            new JcePBESecretKeyEncryptorBuilder(PGPEncryptedData.CAST5, sha1Calc)
                .setProvider("BC")
                .build(privateKeyPassword.toCharArray()));

    return secretKey;
  }
}
