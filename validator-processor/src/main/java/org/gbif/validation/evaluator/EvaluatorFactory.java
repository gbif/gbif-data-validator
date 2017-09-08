package org.gbif.validation.evaluator;

import org.gbif.checklistbank.cli.normalizer.NormalizerConfiguration;
import org.gbif.dwc.extensions.ExtensionManager;
import org.gbif.dwc.extensions.ExtensionManagerFactory;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.processor.interpreting.CoordinateInterpreter;
import org.gbif.occurrence.processor.interpreting.LocationInterpreter;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpreter;
import org.gbif.occurrence.processor.interpreting.TaxonomyInterpreter;
import org.gbif.utils.HttpUtil;
import org.gbif.validation.api.DataFile;
import org.gbif.validation.api.DwcDataFileEvaluator;
import org.gbif.validation.api.RecordCollectionEvaluator;
import org.gbif.validation.api.RecordEvaluator;
import org.gbif.validation.api.ResourceStructureEvaluator;
import org.gbif.validation.api.RowTypeKey;
import org.gbif.validation.api.vocabulary.FileFormat;
import org.gbif.validation.api.model.RecordEvaluatorChain;
import org.gbif.validation.conf.ValidatorConfiguration;
import org.gbif.validation.evaluator.record.OccurrenceInterpretationEvaluator;
import org.gbif.validation.evaluator.record.RecordStructureEvaluator;
import org.gbif.validation.xml.XMLSchemaValidatorProvider;
import org.gbif.ws.json.JacksonJsonContextResolver;
import org.gbif.ws.mixin.Mixins;

import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.apache.ApacheHttpClient;
import org.apache.http.client.HttpClient;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates instances of mostly all evaluators.
 */
public class EvaluatorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(EvaluatorFactory.class);
  private static final String XML_CATALOG = "xml/xml-catalog.xml";
  private static final XMLSchemaValidatorProvider XML_SCHEMA_VALIDATOR_PROVIDER = createXMLSchemaValidatorProvider();
  private static final int CLIENT_TO = 600000; // registry client default timeout

  private static final ApacheHttpClient HTTP_CLIENT = createHttpClient();
  //FIXME we should refactor the ExtensionManager and reuse the ApacheHttpClient
  private static final HttpClient PLAIN_HTTP_CLIENT = createPlainHttpClient(10, 10);

  private final String apiUrl;
  private final NormalizerConfiguration normalizerConfiguration;
  private final ExtensionManager extensionManager;

  /**
   * Get a new {@link EvaluatorFactory} instance based on {@link ValidatorConfiguration}.
   * @param config
   */
  public EvaluatorFactory(ValidatorConfiguration config) {
    this(config, ExtensionManagerFactory.buildExtensionManager(PLAIN_HTTP_CLIENT,
            config.getExtensionListURL(), true));
  }

  /**
   * Get a new {@link EvaluatorFactory} instance based on {@link ValidatorConfiguration} but using the provided
   * {@link ExtensionManager} ignoring the related configuration in the {@link ValidatorConfiguration}.
   * Mostly use for testing.
   * @param config
   * @param extensionManager use the provided
   */
  public EvaluatorFactory(ValidatorConfiguration config, ExtensionManager extensionManager) {
    Objects.requireNonNull(config, "ValidatorConfiguration shall be provided");

    this.apiUrl = config.getApiUrl();
    this.normalizerConfiguration = config.getNormalizerConfiguration();
    this.extensionManager = extensionManager;
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
      case DWCA:
        return new DwcaResourceStructureEvaluator(XML_SCHEMA_VALIDATOR_PROVIDER, extensionManager);
      default: return (dataFile) -> Optional.empty();
    }
  }

  /**
   * Creates a {@link RecordCollectionEvaluator} that validates the uniqueness of the value on a specific column index.
   *
   * @param rowTypeKey rowTypeKey targeted by this uniqueness validation
   * @param caseSensitive
   * @param workingFolder folder specific to the rsource to evaluate where temporary files will be created
   * @return
   */
  public static RecordCollectionEvaluator createUniquenessEvaluator(RowTypeKey rowTypeKey, boolean caseSensitive, Path workingFolder) {
    return new UniquenessEvaluator(rowTypeKey, caseSensitive, workingFolder);
  }

  /**
   * Creates a {@link RecordCollectionEvaluator} instance for a specific rowType.
   * Given a {@link DataFile} that represents the entire Dwc-A, this {@link RecordCollectionEvaluator} instance
   * will check for referential integrity issues between the given extension (rowType) and the core.
   *
   * @param rowType
   * @return
   */
  public static RecordCollectionEvaluator createReferentialIntegrityEvaluator(Term rowType) {
    Objects.requireNonNull(rowType, "rowType shall be provided");
    return new ReferentialIntegrityEvaluator(rowType);
  }

  /**
   * Creates a {@link RecordCollectionEvaluator} instance for a evaluating checklist.
   * @param workingFolder folder specific to the resource to evaluate where temporary files will be created
   * @return
   */
  public RecordCollectionEvaluator createChecklistEvaluator(Path workingFolder) {
    return new ChecklistEvaluator(normalizerConfiguration, workingFolder);
  }

  /**
   * Creates a {@link DwcDataFileEvaluator} to evaluate prerequisites of the evaluation.
   * @return
   */
  public DwcDataFileEvaluator createPrerequisiteEvaluator() {
    return new PrerequisiteEvaluator();
  }

  /**
   * Creates a {@link DwcDataFileEvaluator} to evaluate the content of the metadata document (EML).
   * @return
   */
  public DwcDataFileEvaluator createMetadataContentEvaluator() {
    return new BasicMetadataEvaluator();
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
   * Create an OccurrenceLineProcessor.
   *
   * @return new instance
   */
  public RecordEvaluator create(Term rowType, List<Term> columns,
                                Map<Term, String> defaultValues) {
    Objects.requireNonNull(columns, "columns shall be provided");
    Objects.requireNonNull(rowType, "rowType shall be provided");

    List<RecordEvaluator> evaluators = new ArrayList<>();
    evaluators.add(new RecordStructureEvaluator(rowType, columns));

    if (DwcTerm.Occurrence == rowType) {
      evaluators.add(new OccurrenceInterpretationEvaluator(buildOccurrenceInterpreter(),
              columns.toArray(new Term[columns.size()]), defaultValues));
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

  private static HttpClient createPlainHttpClient(int maxConnections, int maxPerRoute) {
    return HttpUtil.newMultithreadedClient(CLIENT_TO, maxConnections, maxPerRoute);
  }

}
