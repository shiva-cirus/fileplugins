package io.cdap.plugin.file.ingest.compress;

public class FileEncryptionProcessor {/*
   private static Logger logger = LoggerFactory.getLogger(FileEncryptionProcessor.class);
   private static InMemoryKeyring inMemoryKeyring = null;
   private static String recipientID = null;
   public static String encryptFile(String inputFileName, String publicKey, String recipientID) throws NoSuchAlgorithmException, SignatureException, NoSuchProviderException, IOException, PGPException, EncryptionException {
       logger.info("Encrypting file " + inputFileName);
       if (inMemoryKeyring == null | recipientID == null) {
           if (!new File(publicKey).exists()) {
               throw new RuntimeException("Could not find public key at specified location " + publicKey);
           }
           FileEncryptionProcessor.recipientID = recipientID;
           FileEncryptionProcessor.inMemoryKeyring = KeyringConfigs.forGpgExportedKeys(KeyringConfigCallbacks.withPassword(""));
           inMemoryKeyring.addPublicKey(getBytesFrom(publicKey));
       }
       encryptWithBouncyGPG(inputFileName);
       return "complete";
   }
   private static void encryptWithBouncyGPG(String inputFileName) throws IOException, PGPException, NoSuchAlgorithmException, SignatureException, NoSuchProviderException, EncryptionException {
       File inputFile = new File(inputFileName);
       if (!inputFile.exists()) {
           throw new EncryptionException("Could not find file " + inputFileName);
       }
       if (inputFile.getName().startsWith(".")) {
               logger.info("Rejecting file '" + inputFile + "' as we do not process hidden files.");
       }
       String encryptedOutputFile = OutputFileRenamer.generateOutputFileName(inputFileName);
       logger.info("Output encrypted file to " + encryptedOutputFile);
       File outputFile = new File (encryptedOutputFile);
       outputFile.getParentFile().mkdirs();
       try (final FileOutputStream fileOutput = new FileOutputStream(outputFile)) {
           try (final BufferedOutputStream bufferedOut = new BufferedOutputStream(fileOutput)) {
               try (final OutputStream outputStream = BouncyGPG
                       .encryptToStream()
                       .withConfig(inMemoryKeyring)
                       .withStrongAlgorithms()
                       .toRecipient(recipientID)
                       .andDoNotSign()
                       .binaryOutput()
                       .andWriteTo(bufferedOut)) {
                   final FileInputStream is = new FileInputStream(inputFile);
                   Streams.pipeAll(is, outputStream);
               }
           }
       } catch (Exception e) {
           logger.error("Could not encrypt file", e);
       }
   }
   private static byte[] getBytesFrom(String privateEncryptionKey) {
       try {
           return  IOUtils.toByteArray(new FileInputStream(new File(privateEncryptionKey)));
       } catch (IOException e) {
           e.printStackTrace();
       }
       return null;
   }*/
}