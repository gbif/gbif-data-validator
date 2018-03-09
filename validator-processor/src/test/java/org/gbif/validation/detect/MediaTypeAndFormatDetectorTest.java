package org.gbif.validation.detect;

import org.gbif.detect.MediaTypeAndFormatDetector;
import org.gbif.utils.file.FileUtils;
import org.gbif.validation.api.vocabulary.FileFormat;
import org.gbif.ws.util.ExtraMediaTypes;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests related to {@link MediaTypeAndFormatDetector}
 */
public class MediaTypeAndFormatDetectorTest {

  @Test
  public void testDetectMediaType() throws IOException {
    assertEquals(ExtraMediaTypes.APPLICATION_EXCEL, MediaTypeAndFormatDetector.detectMediaType(
            FileUtils.getClasspathFile("workbooks/occurrence-workbook.xls").toPath()));
    assertEquals(ExtraMediaTypes.APPLICATION_OFFICE_SPREADSHEET, MediaTypeAndFormatDetector.detectMediaType(
            FileUtils.getClasspathFile("workbooks/occurrence-workbook.xlsx").toPath()));
    assertEquals(ExtraMediaTypes.APPLICATION_OPEN_DOC_SPREADSHEET, MediaTypeAndFormatDetector.detectMediaType(
            FileUtils.getClasspathFile("workbooks/occurrence-workbook.ods").toPath()));
    assertEquals(ExtraMediaTypes.TEXT_CSV, MediaTypeAndFormatDetector.detectMediaType(
            FileUtils.getClasspathFile("workbooks/occurrence-workbook.csv").toPath()));
  }

  @Test
  public void testDetectMediaTypeByStream() throws IOException {
     // without hint from the filename that's the best we can get
    assertEquals("application/x-tika-ooxml", MediaTypeAndFormatDetector.detectMediaType(
            FileUtils.classpathStream("workbooks/occurrence-workbook.xlsx"), null));
    // if we give the filename (including the extension) Tika use it to "improve the detail of the detection"
    assertEquals(ExtraMediaTypes.APPLICATION_OFFICE_SPREADSHEET, MediaTypeAndFormatDetector.detectMediaType(
            FileUtils.classpathStream("workbooks/occurrence-workbook.xlsx"), "occurrence-workbook.xlsx"));
  }

  /**
   * The following tests consider the folder as if it was the result of a zip extraction.
   * @throws IOException
   */
  @Test
  public void testEvaluateMediaTypeAndFormat() throws IOException {

    Path extractedFolder = FileUtils.getClasspathFile("dwca/dwca-id-with-term").toPath();
    Optional<MediaTypeAndFormatDetector.MediaTypeAndFormat> mediaTypeAndFormat =
            MediaTypeAndFormatDetector.evaluateMediaTypeAndFormat(extractedFolder, org.apache.tika.mime.MediaType.APPLICATION_ZIP.toString());
    assertTrue(mediaTypeAndFormat.isPresent());
    assertEquals(FileFormat.DWCA, mediaTypeAndFormat.get().getFileFormat());

    // if the extracted folder contains only a csv file, we can change the mediaType
    extractedFolder = FileUtils.getClasspathFile("tabular/single-file").toPath();
    mediaTypeAndFormat =
            MediaTypeAndFormatDetector.evaluateMediaTypeAndFormat(extractedFolder, org.apache.tika.mime.MediaType.APPLICATION_ZIP.toString());
    assertTrue(mediaTypeAndFormat.isPresent());
    assertEquals(FileFormat.TABULAR, mediaTypeAndFormat.get().getFileFormat());
  }
}
