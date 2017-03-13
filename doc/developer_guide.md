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

The evaluation of the submitted `DataFile` is achieve by a chain of evaluators. One can find 2 types of evaluation chain based on the nature of the evaluators: [StructuralEvaluationChain](#structuralevaluationchain) and [EvaluationChain](#evaluationchain). Example can be found in the implementation based on Akka Actors [DataFileProcessorMaster](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/processor/DataFileProcessorMaster.java).

### StructuralEvaluationChain
The [StructuralEvaluationChain](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/evaluator/StructuralEvaluationChain.java) is used to build and store the sequence of evaluation that will be performed on the structure of the `DataFile` submitted. The `StructuralEvaluationChain` shall be run sequentially and can stop before it reaches the last evaluation. The condition to stop depends on the [EvaluationCategory](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/model/EvaluationCategory.java) of a validation result received. For example if DarwinCore Archive and cannot be opened/extracted we would stop the evaluation since nothing else could be evaluated.

#### ResourceStructureEvaluator
[ResourceStructureEvaluator](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/ResourceStructureEvaluator.java) represents an evaluation against the structure of the resource itself.

### EvaluationChain
[EvaluationChain](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/processor/EvaluationChain.java) is used to build/configure/define the sequence of evaluation that will be performed on the content of the `DataFile`. This main evaluation chain is run by calling the 3 different `run` methods including a [runner](https://github.com/gbif/gbif-data-validator/tree/master/validator-processor/src/main/java/org/gbif/validation/evaluator/runner) as lambda expression for each of them. The reason behind the usage of the different `runner` is to allow the caller to run it synchronously or asynchronously. It also allows the caller to decide how to collect the different results.

### MetadataEvaluator
[MetadataEvaluator](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/MetadataEvaluator.java) is responsible to evluate the content of the metadata document (EML).

### RecordCollectionEvaluator
[RecordCollectionEvaluator](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/RecordCollectionEvaluator.java) operates at a higher level than RecordEvaluator and work on more than one record but, it also produces RecordEvaluationResult at a record level.
 
### RecordEvaluator
[RecordEvaluator](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/RecordEvaluator.java) is responsible to take a record and produce an RecordEvaluationResult.


## EvaluationChain example

Example of evaluation chain based from unit test. Please note that some code was removed to make it more readable, see [EvaluationChainTest]( https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/test/java/org/gbif/validation/evaluator/EvaluationChainTest.java) for the complete example.
```java
DwcDataFile dwcDataFile = DataFileFactory.prepareDataFile(dataFile, folder.newFolder().toPath());
EvaluationChain.Builder builder = EvaluationChain.Builder.using(dwcDataFile, TestUtils.getEvaluatorFactory());

builder.evaluateUniqueness()
       .evaluateReferentialIntegrity();

builder.build().runRowTypeEvaluation((dwcDF, rowType, recordCollectionEvaluator) -> {
  Optional<Stream<RecordEvaluationResult>> result = recordCollectionEvaluator.evaluate(dwcDF);
  if (DwcTerm.Identification.equals(rowType)) {
    assertTrue("Got referential integrity issue on Identification extensions", result.isPresent());
  }
});
```
## Actors

## Collectors
