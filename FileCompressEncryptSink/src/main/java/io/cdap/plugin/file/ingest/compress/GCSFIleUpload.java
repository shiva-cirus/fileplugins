package io.cdap.plugin.file.ingest.compress;

import com.google.cloud.Tuple;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author Vikas K  Created On 04/11/19
 **/

/**
 * This class demonstrates how to create a new Blob or to update its content.
 *
 * @see <a href="https://cloud.google.com/storage/docs/json_api/v1/objects/insert">Objects:
 * insert</a>
 */
public class GCSFIleUpload {//extends StorageAction<Tuple<Path, BlobInfo>> {
    //@Override
    public void run(Storage storage, Tuple<Path, BlobInfo> tuple) throws Exception {
        run(storage, tuple.x(), tuple.y());
    }

    private void run(Storage storage, Path uploadFrom, BlobInfo blobInfo) throws IOException {
        if (Files.size(uploadFrom) > 1_000_000) {
            // When content is not available or large (1MB or more) it is recommended
            // to write it in chunks via the blob's channel writer.
            try (WriteChannel writer = storage.writer(blobInfo)) {
                byte[] buffer = new byte[1024];
                try (InputStream input = Files.newInputStream(uploadFrom)) {
                    int limit;
                    while ((limit = input.read(buffer)) >= 0) {
                        try {
                            writer.write(ByteBuffer.wrap(buffer, 0, limit));
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            }
        } else {
            byte[] bytes = Files.readAllBytes(uploadFrom);
            // create the blob in one request.
            storage.create(blobInfo, bytes);
        }
        System.out.println("Blob was created");
    }

    private void run(Storage storage, InputStream inputStream, BlobInfo blobInfo) throws IOException {
        /*String uploadFrom = null;
        if (Files.size(uploadFrom) > 1_000_000) {
            // When content is not available or large (1MB or more) it is recommended
            // to write it in chunks via the blob's channel writer.
            try (WriteChannel writer = storage.writer(blobInfo)) {
                byte[] buffer = new byte[1024];
                try (InputStream input = Files.newInputStream(uploadFrom)) {
                    int limit;
                    while ((limit = input.read(buffer)) >= 0) {
                        try {
                            writer.write(ByteBuffer.wrap(buffer, 0, limit));
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                }
            }
        } else {
            byte[] bytes = Files.readAllBytes(uploadFrom);
            // create the blob in one request.
            storage.create(blobInfo, bytes);
        }
        System.out.println("Blob was created");*/
    }

    //@Override
    Tuple<Path, BlobInfo> parse(String filePath, String bucketName, String gcsPath) throws IOException {

        Path path = Paths.get(filePath);
        String contentType = Files.probeContentType(path);
        String blob = gcsPath == null ? path.getFileName().toString() : gcsPath;
        return Tuple.of(path, BlobInfo.newBuilder(bucketName, blob).setContentType(contentType).build());
    }

    //@Override
    public String params() {
        return "<local_file> <bucket> [<path>]";
    }
}