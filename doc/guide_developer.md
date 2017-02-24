This document is intended for developers.
It covers the internal concepts of the gbif-data-validator.

## DataFile
[DataFile](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/DataFile.java)
represents the resource to be validated as provided by the user.

DataFile instances are obtained by [DataFileFactory](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/source/DataFileFactory.java).

## DwcDataFile
DataFile are transformed and prepared in order to facilitate their usage within the evaluation chain.

Including :
 * Counting the number of lines
 * Extracting the headers
 * Darwin Core Archive into a list of all its components (core + extensions)
 * Excel file into CSV
 * Conversion to UTF-8
 * Standardisation of end line characters

The result of those operations is kept in a DwcDataFile.

## TabularDataFile
[TabularDataFile](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/TabularDataFile.java)
represents data held into a file in tabular format.
TabularDataFile are obtained by [DataFileFactory](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/source/DataFileFactory.java).

## Additional TabularDataFile Transformations

A TabularDataFile can be transformed again under some circumstances. The best example is when a TabularDataFile
needs tp be split into multiple (smaller) tabular files.

When a DataFile is transformed the link to the original/previous DataFile is kept and accessible via the `getParent()` method of TabularDataFile.

## RecordSource
[RecordSource](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/RecordSource.java) allows to expose records independently from their source.

RecordSource are obtained by [RecordSourceFactory](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/source/RecordSourceFactory.java).

## Evaluators
### ResourceStructureEvaluator
[ResourceStructureEvaluator](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/ResourceStructureEvaluator.java) represents an evaluation against the structure of the resource itself. If the evaluation bring results, depending of the [EvaluationCategory](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/model/EvaluationCategory.java), the evaluation chain can be stopped. For example if DarwinCore Archive and cannot be opened/extracted.

### MetadataEvaluator


### RecordCollectionEvaluator
[RecordCollectionEvaluator](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/RecordCollectionEvaluator.java) operates at a higher level than RecordEvaluator and work on more than one record but, it also produces RecordEvaluationResult at a record level.
 
### RecordEvaluator
[RecordEvaluator](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/RecordEvaluator.java) is responsible to take a record and produce an RecordEvaluationResult.

## EvaluationChain
[EvaluationChain](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/processor/EvaluationChain.java) is used to build/configure/define the sequence of evaluation that will be performed.

Example of simple evaluation chain based from unit test:
```java
List<TabularDataFile> dataFiles = DataFileFactory.prepareDataFile(dwcaDataFile);
EvaluationChain.Builder builder = EvaluationChain.Builder.using(dwcaDataFile, dataFiles,
              TestUtils.getEvaluatorFactory());

builder.evaluateReferentialIntegrity();
builder.build().runRowTypeEvaluation(rowTypeEvaluationUnit -> {
  Optional<Stream<RecordEvaluationResult>> result = rowTypeEvaluationUnit.evaluate();
  if(DwcTerm.Identification.equals(rowTypeEvaluationUnit.getRowType())){
    assertTrue("Got referential integrity issue on Identification extensions", result.isPresent());
  }
});
```
## Actors

## Collectors
