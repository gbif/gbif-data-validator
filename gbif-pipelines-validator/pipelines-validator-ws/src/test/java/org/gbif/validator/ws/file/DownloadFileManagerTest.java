package org.gbif.validator.ws.file;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.junit.jupiter.MockServerExtension;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * {@link DownloadFileManager} tests.
 */
@ExtendWith(MockServerExtension.class)
public class DownloadFileManagerTest  extends DownloadFileBaseTest {

  public DownloadFileManagerTest(ClientAndServer clientAndServer) {
    super(clientAndServer);
  }

  @Test
  public void isAvailableTest() {
    //Existing url
    assertTrue(DownloadFileManager.isAvailable(testPath("/Archive.zip")));

    //Non-existing url
    assertFalse(DownloadFileManager.isAvailable(testPath("/ThisDoesNotExist.zip")));
  }

  /**
   * Synchronous download test.
   */
  @Test
  public void downloadTest() {
    Path targetFilePath = tempDir.resolve("Archive.zip");

    DownloadFileManager downloadFileManager = ctx.getBean(DownloadFileManager.class);
    downloadFileManager.download(testPath("/Archive.zip"), targetFilePath);

    assertTrue(Files.exists(targetFilePath));
  }

  /**
   * Asynchronous download test.
   */
  @Test
  @SneakyThrows
  public void downloadAsyncTest() {
    Path targetFilePath = tempDir.resolve("Archive.zip");

    DownloadFileManager downloadFileManager = ctx.getBean(DownloadFileManager.class);
    CompletableFuture<File> fileDownload = downloadFileManager.downloadAsync(testPath("/Archive.zip"),
                                                                             targetFilePath,
                                                                             file -> assertTrue(Files.exists(targetFilePath)),
                                                                             err -> fail());

    assertTrue(Files.exists(fileDownload.get().toPath()));
  }
}
