package org.gbif.validator.it;

import org.gbif.validator.ws.config.ValidatorWsConfiguration;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.EnableAsync;

@TestConfiguration
@EnableAsync
@PropertySource("classpath:application-test.yml")
@ComponentScan(
  basePackages = {"org.gbif.validator.ws.resource" } )
public class ValidatorWsItConfiguration extends ValidatorWsConfiguration {

}
