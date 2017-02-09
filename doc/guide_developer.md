This document is intended for developers and covers the internal concepts of the gbif-data-validator.

## DataFile
[DataFile](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/DataFile.java)
represents the resource to be validated. DataFile are transformed into one or multiple
TabularDataFile prior to validation. When a DataFile is transformed the link to
the original/previous DataFile is kept and accessible via the `getParent()` method of TabularDataFile.

Transformations include:
 * Darwin Core Archive into a list of all its components (core + extensions)
 * Excel file into CSV
 * Splitting a tabular file into multiple (smaller) tabular files
DataFile are obtained by [DataFileFactory](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/source/DataFileFactory.java).

## TabularDataFile
[TabularDataFile](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/TabularDataFile.java)
represents data held into a file in tabular format.
TabularDataFile are obtained by [DataFileFactory](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/source/DataFileFactory.java).

## RecordSource
[RecordSource](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/RecordSource.java) allows to expose records independently from their source.

RecordSource are obtained by [RecordSourceFactory](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/source/RecordSourceFactory.java).

## Evaluators
 * [ResourceStructureEvaluator](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/ResourceStructureEvaluator.java)
 * [RecordCollectionEvaluator](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/RecordCollectionEvaluator.java)
 * [RecordEvaluator](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/RecordEvaluator.java)

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
