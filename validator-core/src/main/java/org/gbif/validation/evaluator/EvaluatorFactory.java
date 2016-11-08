package org.gbif.validation.evaluator;

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

/**
 * Creates instances of RecordProcessor.
 */
public class EvaluatorFactory {

  private final String apiUrl;

  private static ApacheHttpClient httpClient = createHttpClient();

  private static final int CLIENT_TO = 600000; // registry client default timeout

  public EvaluatorFactory(String apiUrl) {
    this.apiUrl = apiUrl;
  }

  /**
   * Create an OccurrenceLineProcessor.
   *
   * @return new instance
   */
  public RecordEvaluator create(List<Term> columns) {
    List<RecordEvaluator> evaluators = new ArrayList<>();
    evaluators.add(new RecordStructureEvaluator(columns));
    evaluators.add(new OccurrenceInterpretationEvaluator(buildOccurrenceInterpreter(),
            columns));
    return new RecordEvaluatorChain(evaluators);
  }

  /**
   * Create a {@link ResourceStructureEvaluator} instance for a specific {@link FileFormat}.
   *
   * @param fileFormat
   * @return
   */
  public ResourceStructureEvaluator createResourceStructureEvaluator(FileFormat fileFormat) {
    Objects.requireNonNull(fileFormat, "fileFormat shall be provided");

    switch(fileFormat) {
      case DWCA: return new DwcaResourceStructureEvaluator(new XMLSchemaValidatorProvider());
      default: return (dwcFolder, sourceFilename) -> Optional.empty();
    }
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
    WebResource webResource = httpClient.resource(apiUrl);
    TaxonomyInterpreter taxonomyInterpreter = new TaxonomyInterpreter(webResource);
    LocationInterpreter locationInterpreter = new LocationInterpreter(new CoordinateInterpreter(webResource));
    return new OccurrenceInterpreter(taxonomyInterpreter, locationInterpreter);
  }

}
