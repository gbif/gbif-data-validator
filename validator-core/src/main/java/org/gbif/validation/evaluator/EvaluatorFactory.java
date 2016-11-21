package org.gbif.validation.evaluator;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.processor.interpreting.CoordinateInterpreter;
import org.gbif.occurrence.processor.interpreting.LocationInterpreter;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpreter;
import org.gbif.occurrence.processor.interpreting.TaxonomyInterpreter;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.ResourceStructureEvaluator;
import org.gbif.validation.api.model.FileFormat;
import org.gbif.validation.api.model.RecordEvaluatorChain;
import org.gbif.validation.xml.XMLSchemaValidatorProvider;
import org.gbif.ws.json.JacksonJsonContextResolver;
import org.gbif.ws.mixin.Mixins;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.apache.ApacheHttpClient;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates instances of RecordProcessor.
 */
public class EvaluatorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(EvaluatorFactory.class);

  private final String apiUrl;

  private static final String XML_CATALOG = "xml-catalog.xml";

  private static final XMLSchemaValidatorProvider XML_SCHEMA_VALIDATOR_PROVIDER = createXMLSchemaValidatorProvider();
  private static final ApacheHttpClient HTTP_CLIENT = createHttpClient();

  private static final int CLIENT_TO = 600000; // registry client default timeout

  public EvaluatorFactory(String apiUrl) {
    this.apiUrl = apiUrl;
  }

  /**
   * Create an OccurrenceLineProcessor.
   *
   * @return new instance
   */
  public RecordEvaluator create(List<Term> columns, Term rowType) {
    Objects.requireNonNull(columns, "columns shall be provided");
    Objects.requireNonNull(rowType, "rowType shall be provided");

    List<RecordEvaluator> evaluators = new ArrayList<>();
    evaluators.add(new RecordStructureEvaluator(columns));

    if(DwcTerm.Occurrence == rowType) {
      evaluators.add(new OccurrenceInterpretationEvaluator(buildOccurrenceInterpreter(),
              columns));
    }
    return new RecordEvaluatorChain(evaluators);
  }

  /**
   * Create a {@link ResourceStructureEvaluator} instance for a specific {@link FileFormat}.
   *
   * @param fileFormat
   * @return
   */
  public static ResourceStructureEvaluator createResourceStructureEvaluator(FileFormat fileFormat) {
    Objects.requireNonNull(fileFormat, "fileFormat shall be provided");

    switch(fileFormat) {
      case DWCA:
        return new DwcaResourceStructureEvaluator(XML_SCHEMA_VALIDATOR_PROVIDER);
      default: return (dwcFolder, sourceFilename) -> Optional.empty();
    }
  }

  /**
   * Try to build a new XMLSchemaValidatorProvider using a XMLCatalog in the classpath.
   * If the catalog can not be found, return a new XMLSchemaValidatorProvider without XMLCatalog.
   * @return
   */
  private static XMLSchemaValidatorProvider createXMLSchemaValidatorProvider() {
    //we do not load the XMLCatalog from the classpath since it is required to have a path that resolves on
    //the filesystem. It can not be loaded from inside a .jar (see XMLCatalogResolver)
    File xmlCatalog = new File(XML_CATALOG);
    if(xmlCatalog != null && xmlCatalog.exists()) {
      return new XMLSchemaValidatorProvider(Optional.of(Paths.get(xmlCatalog.getAbsolutePath())));
    }
    LOG.warn("Could not load {} from the classpath. Continuing without XMLCatalog.", XML_CATALOG);
    return new XMLSchemaValidatorProvider(Optional.empty());
  }

  /**
   * Creates an HTTP client.
   */
  private static ApacheHttpClient createHttpClient() {

    ClientConfig cc = new DefaultClientConfig();
    cc.getClasses().add(JacksonJsonContextResolver.class);
    cc.getClasses().add(JacksonJsonProvider.class);
    cc.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, true);
    cc.getProperties().put(ClientConfig.PROPERTY_CONNECT_TIMEOUT, CLIENT_TO);
    JacksonJsonContextResolver.addMixIns(Mixins.getPredefinedMixins());

    return ApacheHttpClient.create(cc);
  }

  /**
   * Builds an OccurrenceInterpreter using the current HttpClient instance.
   */
  private OccurrenceInterpreter buildOccurrenceInterpreter() {
    WebResource webResource = HTTP_CLIENT.resource(apiUrl);
    TaxonomyInterpreter taxonomyInterpreter = new TaxonomyInterpreter(webResource);
    LocationInterpreter locationInterpreter = new LocationInterpreter(new CoordinateInterpreter(webResource));
    return new OccurrenceInterpreter(taxonomyInterpreter, locationInterpreter);
  }

}
