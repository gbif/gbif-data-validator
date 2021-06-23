package org.gbif.validator.ws.config;

import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.registry.identity.service.BasicUserSuretyDelegate;
import org.gbif.registry.identity.service.UserSuretyDelegate;
import org.gbif.registry.identity.util.RegistryPasswordEncoder;
import org.gbif.registry.persistence.mapper.UserMapper;
import org.gbif.registry.security.RegistryUserDetailsService;
import org.gbif.registry.surety.ChallengeCodeManager;
import org.gbif.validator.ws.file.UploadedFileManager;
import org.gbif.ws.security.NoAuthWebSecurityConfigurer;

import java.io.IOException;
import java.net.URI;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import lombok.Data;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.multipart.commons.CommonsMultipartResolver;
import org.springframework.web.multipart.support.MultipartFilter;

@Configuration
@Data
public class ValidatorWsConfiguration {

  public static final String FILE_POST_PARAM_NAME = "file";

  @Data
  public static class XmlSchemaLocations {
    private String eml;
    private String emlGbifProfile;
    private String[] dwcMeta;
  }

  @Bean
  @ConfigurationProperties("schemas")
  public XmlSchemaLocations schemaLocations() {
    return new XmlSchemaLocations();
  }

  @Bean
  @SneakyThrows
  public SchemaValidatorFactory schemaValidatorFactory(XmlSchemaLocations xmlSchemaLocations) {
    SchemaValidatorFactory schemaValidatorFactory = new SchemaValidatorFactory(xmlSchemaLocations.getDwcMeta());
    schemaValidatorFactory.load(new URI(xmlSchemaLocations.getEml()));
    schemaValidatorFactory.load(new URI(xmlSchemaLocations.getEmlGbifProfile()));
    return schemaValidatorFactory;
  }

  @Bean
  public UserDetailsService userDetailsService(UserMapper userMapper) {
    return new RegistryUserDetailsService(userMapper);
  }

  @Bean
  public PasswordEncoder passwordEncoder() {
    return new RegistryPasswordEncoder();
  }

  @Bean
  public UserSuretyDelegate userSuretyDelegate(ChallengeCodeManager<Integer> challengeCodeManager) {
    return new BasicUserSuretyDelegate(challengeCodeManager);
  }

  @Bean
  public UploadedFileManager uploadedFileManager(@Value("${upload.workingDirectory}") String uploadWorkingDirectory,
                                                 @Value("${storePath}") String storePath,
                                                 @Value("${upload.maxFileSize}") Long maxFileSize) throws IOException  {
    return new UploadedFileManager(uploadWorkingDirectory, maxFileSize, storePath);
  }

  @Bean public CommonsMultipartResolver multipartResolver() {
    CommonsMultipartResolver multipart = new CommonsMultipartResolver();
    multipart.setMaxUploadSize(3 * 1024 * 1024); return multipart;
  }
  @Bean @Order(0)
  public MultipartFilter multipartFilter() { MultipartFilter
    multipartFilter = new MultipartFilter();
    multipartFilter.setMultipartResolverBeanName("multipartResolver");
    return multipartFilter; }

  /**
   * Configure the Jackson ObjectMapper adding a custom JsonFilter for errors.
   */
  @Configuration
  public class FilterConfiguration {

    public FilterConfiguration (ObjectMapper objectMapper) {
      //This filter only keeps a minimum of fields in a SaxParserException
      FilterProvider simpleFilterProvider = new SimpleFilterProvider()
        .addFilter("stackTrace",
                   SimpleBeanPropertyFilter.filterOutAllExcept("lineNumber", "columnNumber", "message"));
      objectMapper.setFilterProvider(simpleFilterProvider);
    }
  }

  @Configuration
  @EnableWebSecurity(debug = true)
  public static class ValidatorWebSecurity extends NoAuthWebSecurityConfigurer {

    public ValidatorWebSecurity(
      UserDetailsService userDetailsService, ApplicationContext context, PasswordEncoder passwordEncoder
    ) {
      super(userDetailsService, context, passwordEncoder);
    }
  }
}
