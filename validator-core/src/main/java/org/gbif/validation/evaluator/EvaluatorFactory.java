package org.gbif.validation.evaluator;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.processor.interpreting.CoordinateInterpreter;
import org.gbif.occurrence.processor.interpreting.DatasetInfoInterpreter;
import org.gbif.occurrence.processor.interpreting.LocationInterpreter;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpreter;
import org.gbif.occurrence.processor.interpreting.TaxonomyInterpreter;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.model.EvaluationType;
import org.gbif.validation.api.model.RecordEvaluatorChain;
import org.gbif.ws.json.JacksonJsonContextResolver;
import org.gbif.ws.mixin.Mixins;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.apache.ApacheHttpClient;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;

import static org.gbif.validation.util.TempTermsUtils.buildTermMapping;

/**
 * Creates instances of RecordProcessor.
 */
public class EvaluatorFactory {

  private final String apiUrl;

  private static ApacheHttpClient httpClient = createHttpClient();

  private static final int CLIENT_TO = 600000; // registry client default timeout

  private static Map<EvaluationType, List<Term>> COMPLETENESS_TERMS_MAP = new HashMap<>();
  static {
    COMPLETENESS_TERMS_MAP.put(EvaluationType.TAXONOMIC_DATA_NOT_PROVIDED, Arrays.asList(
            DwcTerm.kingdom, DwcTerm.phylum, DwcTerm.class_, DwcTerm.order, DwcTerm.family, DwcTerm.genus, DwcTerm.scientificName));
    COMPLETENESS_TERMS_MAP.put(EvaluationType.GEOSPATIAL_DATA_NOT_PROVIDED, Arrays.asList(DwcTerm.decimalLatitude,
            DwcTerm.decimalLongitude, DwcTerm.geodeticDatum));
    COMPLETENESS_TERMS_MAP.put(EvaluationType.TEMPORAL_DATA_NOT_PROVIDED, Arrays.asList(DwcTerm.eventDate, DwcTerm.year,
            DwcTerm.month, DwcTerm.day));
  }

  public EvaluatorFactory(String apiUrl) {
    this.apiUrl = apiUrl;
  }

  /**
   * Create an OccurrenceLineProcessor.
   *
   * @return new instance
   */
  public RecordEvaluator create(String[] columns) {
    Term[] termsColumnsMapping = buildTermMapping(columns);
    List<RecordEvaluator> evaluators = new ArrayList<>();
    evaluators.add(new RecordStructureEvaluator(columns));
    evaluators.add(new OccurrenceInterpretationEvaluator(buildOccurrenceInterpreter(),
                    termsColumnsMapping));
    evaluators.addAll(createCompletenessEvaluators(Arrays.asList(termsColumnsMapping)));
    return new RecordEvaluatorChain(evaluators);
  }

  /**
   * Creates completeness Evaluators based on the columns available in the resource.
   * TODO maybe move that somewhere else since it's a little bit specific
   * @param columns
   * @return
   */
  public List<RecordEvaluator> createCompletenessEvaluators(List<Term> columns) {
    List<RecordEvaluator> completenessEvaluators = new ArrayList<>();
    for(EvaluationType _type : COMPLETENESS_TERMS_MAP.keySet()) {
      List<Term> termList = new ArrayList<>();
      List<Integer> idxList = new ArrayList<>();

      for(Term t : COMPLETENESS_TERMS_MAP.get(_type)) {
        if(columns.contains(t)){
          termList.add(t);
          idxList.add(columns.indexOf(t));
        }
      }
      //we need at least one term to check completeness
      if(termList.isEmpty()) {
        termList.add(COMPLETENESS_TERMS_MAP.get(_type).get(0));
        //index of of bounds
        idxList.add(columns.size());
      }
      completenessEvaluators.add(new RecordCompletenessEvaluator(_type, termList.toArray(new Term[termList.size()]),
              idxList.toArray(new Integer[idxList.size()])));
    }

    return completenessEvaluators;
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
