package com.github.azell.b2_cloud;

import com.backblaze.b2.client.B2StorageClient;
import com.backblaze.b2.client.contentSources.B2ContentTypes;
import com.backblaze.b2.client.contentSources.B2FileContentSource;
import com.backblaze.b2.client.exceptions.B2Exception;
import com.backblaze.b2.client.exceptions.B2NotFoundException;
import com.backblaze.b2.client.structures.B2Bucket;
import com.backblaze.b2.client.structures.B2FileVersion;
import com.backblaze.b2.client.structures.B2GetFileInfoByNameRequest;
import com.backblaze.b2.client.structures.B2UploadFileRequest;
import com.backblaze.b2.client.structures.B2UploadListener;
import com.backblaze.b2.client.webApiHttpClient.B2StorageHttpClientBuilder;
import com.backblaze.b2.util.B2ExecutorUtils;
import com.backblaze.b2.util.B2Sha1;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  private final ExecutorService executor;
  private final B2StorageClient client;
  private final B2Bucket bucket;
  private final State state;

  public static class State implements AutoCloseable {
    private static final String SEP = "\t";
    private final Map<String, String[]> map = new ConcurrentHashMap<>();
    private final File src;

    private State(File src) throws IOException {
      this.src = src;

      // file<tab>last modified time<tab>sha1
      try (var lines = Files.lines(src.toPath(), StandardCharsets.UTF_8)) {
        lines.map(s -> s.split(SEP)).forEach(v -> map.put(v[0], new String[] {v[1], v[2]}));
      }
    }

    @Override
    public void close() throws IOException {
      var dst = File.createTempFile("b2c", null, src.getCanonicalFile().getParentFile());

      dst.deleteOnExit();

      try (var writer = new BufferedWriter(new FileWriter(dst))) {
        for (var e : map.entrySet()) {
          var v = e.getValue();

          writer.write(String.join(SEP, e.getKey(), v[0], v[1]));
          writer.newLine();
        }
      }

      Files.move(dst.toPath(), src.toPath(), StandardCopyOption.ATOMIC_MOVE);
    }

    public String calc(File file) throws IOException {
      var lastMod = file.lastModified();

      if (lastMod <= 0L) {
        LOGGER.warn("{} has an invalid last modified time {}", file, lastMod);

        return calcSha1(file);
      }

      var key = file.getPath();
      var row = map.get(key);
      var ts = Long.toString(lastMod, 10);

      if (row == null || !row[0].equals(ts)) {
        LOGGER.info("{} has an out of date hash", file);

        map.put(key, row = new String[] {ts, calcSha1(file)});
      }

      return row[1];
    }

    private String calcSha1(File file) throws IOException {
      try (var inp = new FileInputStream(file)) {
        return B2Sha1.hexSha1OfInputStream(inp);
      }
    }
  }

  private Main(ExecutorService executor, B2StorageClient client, B2Bucket bucket, State state) {
    this.executor = executor;
    this.client = client;
    this.bucket = bucket;
    this.state = state;
  }

  public static void main(String[] args) throws B2Exception, IOException {
    var executor =
        Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors(),
            B2ExecutorUtils.createThreadFactory("b2_cloud-%d"));

    try (var client = B2StorageHttpClientBuilder.builder("b2_cloud").build();
        var reader = new BufferedReader(new InputStreamReader(System.in));
        var state = new State(new File("state.txt"))) {
      var main = new Main(executor, client, client.getBucketOrNullByName(args[0]), state);

      main.cleanupFiles();
      reader.lines().forEach(main::uploadFile);
    } finally {
      B2ExecutorUtils.shutdownAndAwaitTermination(executor, 30, 15);
    }
  }

  public void cleanupFiles() throws B2Exception {
    client.unfinishedLargeFiles(bucket.getBucketId()).forEach(this::cancel);
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
    try {
      var version =
          client.getFileInfoByName(
              B2GetFileInfoByNameRequest.builder(bucket.getBucketName(), fileName).build());

      return sha1.equals(version.getContentSha1()) || sha1.equals(version.getLargeFileSha1OrNull());
    } catch (B2NotFoundException e) {
      return false;
    }
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
    String sha1 = state.calc(file);

    if (exists(fileName, sha1)) {
      LOGGER.info("{} exists with matching checksum {}", fileName, sha1);

      return;
    }

    var request =
        B2UploadFileRequest.builder(
                bucket.getBucketId(),
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
