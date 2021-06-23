package org.gbif.validator.ws.resource;

import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.validator.api.Validation;
import org.gbif.validator.persistence.mapper.ValidationMapper;
import org.gbif.validator.ws.file.DataFile;
import org.gbif.validator.ws.file.FileSizeException;
import org.gbif.validator.ws.file.UnsupportedMediaTypeException;
import org.gbif.validator.ws.file.UploadedFileManager;

import java.io.IOException;
import java.net.URL;
import java.security.Principal;
import java.util.UUID;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import static org.gbif.registry.security.UserRoles.USER_ROLE;

/**
 *  Asynchronous web resource to process data validations.
 *  Internally, redirects all the requests to a JobServer instances that coordinates all data validations.
 */
@RestController
@RequestMapping(value = "validation", produces = MediaType.APPLICATION_JSON_VALUE)
@Secured({USER_ROLE})
public class ValidationResource {

  private static final Logger LOG = LoggerFactory.getLogger(ValidationResource.class);

  private final UploadedFileManager fileTransferManager;
  private final ValidationMapper validationMapper;

  public ValidationResource(
    UploadedFileManager fileTransferManager,
    ValidationMapper validationMapper)
    throws IOException {
    this.fileTransferManager = fileTransferManager;
    this.validationMapper = validationMapper;
  }

  @PostMapping(consumes = {MediaType.MULTIPART_FORM_DATA_VALUE},
               produces = {MediaType.APPLICATION_JSON_VALUE})
  public Validation submitFile(HttpServletRequest request, @RequestParam("file")
    MultipartFile file, @Autowired Principal principal) throws FileSizeException, UnsupportedMediaTypeException, IOException {
    UUID key = UUID.randomUUID();
    DataFile dataFile = fileTransferManager.uploadDataFile(file, key.toString());
    return create(key, dataFile, principal);
  }

  @PostMapping(
    path = "/url",
    consumes = {MediaType.MULTIPART_FORM_DATA_VALUE},
    produces = {MediaType.APPLICATION_JSON_VALUE})
  public Validation submitUrl(@RequestParam("fileUrl") String fileURL, @Autowired Principal principal) throws FileSizeException, UnsupportedMediaTypeException {
    try {
      UUID key = UUID.randomUUID();
      //this should also become asynchronous at some point
      DataFile dataFile = fileTransferManager.downloadDataFile(new URL(fileURL), key.toString());
      return create(key, dataFile, principal);
    } catch (FileSizeException fsEx) {
      // let FileSizeExceptionMapper handle it
      throw fsEx;
    } catch (IOException ioEx) {
      LOG.warn("Can not download file submitted", ioEx);
      throw new RuntimeException(ioEx);
    }
  }

  @GetMapping(
    produces = {MediaType.APPLICATION_JSON_VALUE}
  )
  public PagingResponse<Validation> list(Pageable page, @Autowired Principal principal) {
    page = page == null ? new PagingRequest() : page;
    long total = validationMapper.count(principal.getName());
    return new PagingResponse<>(page.getOffset(), page.getLimit(), total, validationMapper.list(page, principal.getName()));
  }

  public Validation create(UUID key, DataFile dataFile, Principal principal) {
    Validation validation = Validation.builder()
                              .key(key)
                              .fileFormat(dataFile.getFileFormat())
                              .status(Validation.Status.SUBMITTED)
                              .file(dataFile.getFilePath().toString())
                              .username(principal.getName())
                              .build();
    validationMapper.create(validation);
    return validationMapper.get(key);
  }

}
