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

import java.net.URL;
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
  private static final String XML_CATALOG = "xml/xml-catalog.xml";
  private static final XMLSchemaValidatorProvider XML_SCHEMA_VALIDATOR_PROVIDER = createXMLSchemaValidatorProvider();
  private static final int CLIENT_TO = 600000; // registry client default timeout
  private static final ApacheHttpClient HTTP_CLIENT = createHttpClient();
  private final String apiUrl;

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
      default: return (dataFile) -> Optional.empty();
    }
  }

  /**
   * Try to build a new XMLSchemaValidatorProvider using a XMLCatalog in the classpath.
   * If the catalog can not be found, return a new XMLSchemaValidatorProvider without XMLCatalog.
   * @return
   */
  private static XMLSchemaValidatorProvider createXMLSchemaValidatorProvider() {
    URL xmlCatalog = EvaluatorFactory.class.getClassLoader().getResource(XML_CATALOG);
    if(xmlCatalog != null) {
      return new XMLSchemaValidatorProvider(Optional.of(XML_CATALOG));
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
    evaluators.add(new RecordStructureEvaluator(rowType, columns));

    if(DwcTerm.Occurrence == rowType) {
      evaluators.add(new OccurrenceInterpretationEvaluator(buildOccurrenceInterpreter(), DwcTerm.Occurrence,
                                                           columns));
    }
    return new RecordEvaluatorChain(evaluators);
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
