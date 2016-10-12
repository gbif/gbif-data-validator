package org.gbif.occurrence.validation.evaluator;

import org.gbif.occurrence.processor.interpreting.CoordinateInterpreter;
import org.gbif.occurrence.processor.interpreting.DatasetInfoInterpreter;
import org.gbif.occurrence.processor.interpreting.LocationInterpreter;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpreter;
import org.gbif.occurrence.processor.interpreting.TaxonomyInterpreter;
import org.gbif.occurrence.validation.api.RecordEvaluator;
import org.gbif.ws.json.JacksonJsonContextResolver;
import org.gbif.ws.mixin.Mixins;

import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.apache.ApacheHttpClient;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;

import static org.gbif.occurrence.validation.util.TempTermsUtils.buildTermMapping;

/**
 * Creates instances of RecordProcessor.
 */
public class OccurrenceEvaluatorFactory {

  private final String apiUrl;

  private static ApacheHttpClient httpClient = createHttpClient();

  private static final int CLIENT_TO = 600000; // registry client default timeout

  public OccurrenceEvaluatorFactory(String apiUrl) {
    this.apiUrl = apiUrl;
  }

  /**
   * Create an OccurrenceLineProcessor.
   *
   * @return new instance
   */
  public RecordEvaluator create(String[] columns) {
    return new OccurrenceInterpretationEvaluator(buildOccurrenceInterpreter(), buildTermMapping(columns));
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
    DatasetInfoInterpreter datasetInfoInterpreter = new DatasetInfoInterpreter(webResource);
    TaxonomyInterpreter taxonomyInterpreter = new TaxonomyInterpreter(webResource);
    LocationInterpreter locationInterpreter = new LocationInterpreter(new CoordinateInterpreter(webResource));
    return new OccurrenceInterpreter(datasetInfoInterpreter, taxonomyInterpreter, locationInterpreter);
  }
}
