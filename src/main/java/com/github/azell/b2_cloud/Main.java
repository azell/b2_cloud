package com.github.azell.b2_cloud;

import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.contentSources.B2ContentTypes;
import com.backblaze.b2.client.contentSources.B2FileContentSource;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.structures.B2FileVersion;
import com.backblaze.b2.client.structures.B2ListFileVersionsRequest;
import com.backblaze.b2.client.structures.B2UploadFileRequest;
import com.backblaze.b2.client.structures.B2UploadListener;
import com.backblaze.b2.client.webApiHttpClient.B2StorageHttpClientBuilder;
import com.backblaze.b2.util.B2ExecutorUtils;
import com.backblaze.b2.util.B2Sha1;
import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  private final ExecutorService executor;
  private final B2StorageClient client;
  private final String bucketId;

  private Main(ExecutorService executor, B2StorageClient client, String bucketId) {
    this.executor = executor;
    this.client = client;
    this.bucketId = bucketId;
  }

  public static void main(String[] args) throws B2Exception, IOException {
    var executor =
        Executors.newFixedThreadPool(8, B2ExecutorUtils.createThreadFactory("b2_cloud-%d"));

    try (var client = B2StorageHttpClientBuilder.builder("b2_cloud").build();
        var reader = new BufferedReader(new InputStreamReader(System.in))) {
      var main = new Main(executor, client, args[0]);

      main.cleanupFiles();
      reader.lines().forEach(main::uploadFile);
    } finally {
      B2ExecutorUtils.shutdownAndAwaitTermination(executor, 30, 15);
    }
  }

  public void cleanupFiles() throws B2Exception {
    client.unfinishedLargeFiles(bucketId).forEach(this::cancel);
  }

  public void uploadFile(String fileName) {
    try {
      upload(fileName);
    } catch (B2Exception | IOException e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  private void cancel(B2FileVersion version) {
    try {
      client.cancelLargeFile(version.getFileId());
    } catch (B2Exception e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  private boolean exists(String fileName, String sha1) throws B2Exception {
    var request =
        B2ListFileVersionsRequest.builder(bucketId)
            .setStartFileName(fileName)
            .setPrefix(fileName)
            .build();

    Predicate<B2FileVersion> filter =
        v ->
            fileName.equals(v.getFileName())
                && (sha1.equals(v.getContentSha1()) || sha1.equals(v.getLargeFileSha1OrNull()));

    var opt =
        StreamSupport.stream(client.fileVersions(request).spliterator(), false)
            .filter(filter)
            .findFirst();

    opt.ifPresent(v -> LOGGER.info("{} exists -> {}", v.getFileName(), v.getFileId()));

    return opt.isPresent();
  }

  private B2UploadListener listener(String fileName) {
    return (p) -> {
      LOGGER.info(
          "{} {} {}% {}/{}",
          fileName,
          p.getState(),
          (int) (100. * (p.getBytesSoFar() / (double) p.getLength())),
          p.getPartIndex() + 1,
          p.getPartCount());
    };
  }

  private void upload(String fileName) throws B2Exception, IOException {
    var file = new File(fileName);
    String sha1;

    try (InputStream in = new FileInputStream(file)) {
      sha1 = B2Sha1.hexSha1OfInputStream(in);
    }

    if (exists(fileName, sha1)) {
      return;
    }

    var request =
        B2UploadFileRequest.builder(
                bucketId,
                fileName,
                B2ContentTypes.B2_AUTO,
                B2FileContentSource.builder(file).setSha1(sha1).build())
            .setListener(listener(fileName))
            .build();

    var version =
        client.getFilePolicy().shouldBeLargeFile(request.getContentSource().getContentLength())
            ? client.uploadLargeFile(request, executor)
            : client.uploadSmallFile(request);

    LOGGER.info("{} upload completed -> {}", version.getFileName(), version.getFileId());
  }
}
