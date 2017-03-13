This document is intended for developers.
It covers the internal concepts of the gbif-data-validator.

## Unix utility dependencies
The current implementation requires Unix utilities such as `awk`, `split` (see [FileBashUtilities](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/util/FileBashUtilities.java)) in order to perform evaluations like uniqueness and referential integrity checks and split files in an efficient manner.

These utilities expect `lf` endline characters. Therefore, we normalize files in order to allow evaluations to be performed. At the moment, [DataFileFactory](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/source/DataFileFactory.java) is responsible to coordinate this task.

## DataFiles

### DataFileFactory
 [DataFileFactory](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/source/DataFileFactory.java) is the entry point for everything related to DataFile, DwcDataFile and TabularDataFile.

### DataFile
[DataFile](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/DataFile.java)
represents the resource to be validated as provided by the user. It can represent any format from [FileFormat](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/model/FileFormat.java).

### DwcDataFile
DataFile are transformed and prepared in order to facilitate their usage within the evaluation chain.

Preparation and transformation includes:
 * Counting the number of lines
 * Extracting the headers
 * Darwin Core Archive into a list of all its components (core + extensions)
 * Excel file into CSV
 * Standardisation of end line characters

The result of those operations is kept in a [DwcDataFile](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/DwcDataFile.java) and is obtained by the `prepareDataFile` method of the DataFileFactory.

### TabularDataFile
Inside the DwcDataFile, we have one [TabularDataFile](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/TabularDataFile.java) per [rowType](http://rs.tdwg.org/dwc/terms/guides/text/index.htm#coreTag). It basically represents what is required to be validated, in a standardized format.

Additionally, a TabularDataFile can be transformed again under some circumstances. The best example is when a TabularDataFile
needs tp be split into multiple (smaller) tabular files to run some evaluations in parallel.

### RecordSource
[RecordSource](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/RecordSource.java) allows to expose records independently from their source.

RecordSource are obtained by [RecordSourceFactory](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/source/RecordSourceFactory.java).

## Evaluation Chain

The evaluation of the submitted DataFile is achieve by a chain of evaluators. One can find 2 types of evaluation chain based on the nature of the evaluators: [StructuralEvaluationChain](#StructuralEvaluationChain) and [EvaluationChain](#EvaluationChain)

### __StructuralEvaluationChain__
The StructuralEvaluationChain is used to build and store the sequence of evaluation that will be performed on the structure of the DataFile submitted. Do to the nature of the validations, the StructuralEvaluationChain shall be run sequentially and can stop before it reaches the last evaluation.

#### ResourceStructureEvaluator
[ResourceStructureEvaluator](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/ResourceStructureEvaluator.java) represents an evaluation against the structure of the resource itself. If the evaluation bring results, depending of the [EvaluationCategory](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/model/EvaluationCategory.java), the evaluation chain can be stopped. For example if DarwinCore Archive and cannot be opened/extracted.

### __EvaluationChain__
[EvaluationChain](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/processor/EvaluationChain.java) is used to build/configure/define the sequence of evaluation that will be performed.

### RecordCollectionEvaluator
[RecordCollectionEvaluator](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/RecordCollectionEvaluator.java) operates at a higher level than RecordEvaluator and work on more than one record but, it also produces RecordEvaluationResult at a record level.
 
### RecordEvaluator
[RecordEvaluator](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/RecordEvaluator.java) is responsible to take a record and produce an RecordEvaluationResult.


## EvaluationChain example

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
