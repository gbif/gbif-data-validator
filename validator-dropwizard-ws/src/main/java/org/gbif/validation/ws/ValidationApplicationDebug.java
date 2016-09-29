package org.gbif.validation.ws;

public class ValidationApplicationDebug {
  public static void main(String[] args) throws Exception {

    ValidationApplication validationApplication = new ValidationApplication();
    validationApplication.run(args[0],args[1]);
  }
}
