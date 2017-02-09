

##DataFile
[DataFile](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/DataFile.java)
represents the resource to be validated. DataFile are transformed into one or multiple
TabularDataFile prior to validation. When a DataFile is transformed the link to
the original/previous DataFile is kept and accessible via the getParent() method of TabularDataFile.

Transformations include:
 * Darwin Core Archive into a list of all its components (core + extensions)
 * Excel file into CSV
 * Splitting a tabular file into multiple (smaller) tabular files
DataFile are obtained by [DataFileFactory](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/source/DataFileFactory.java).

##TabularDataFile
[TabularDataFile](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/TabularDataFile.java)
represents data held into a file in tabular format.
TabularDataFile are obtained by [DataFileFactory](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/source/DataFileFactory.java).

##RecordSource
(RecordSource)[https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/RecordSource.java] allows to expose records independently from their source.

RecordSource are obtained by [RecordSourceFactory](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/source/RecordSourceFactory.java).

##Actors

##Collectors
