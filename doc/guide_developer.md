

##DataFile
[DataFile](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/DataFile.java) represents the resource (or a part of it) to be validated that is held into a file (or folder).

If a DataFile is transformed into one or multiple a new DataFile(s), the link to the original/previous DataFile is kept and accessible via the getParent() method.

Possible transformations on DataFile:
 * Transforming a Darwin Core Archive into a list of all its components (core + extensions)
 * Tranforming Excel file into CSV
 * Splitting a tabular file into multiple (smaller) tabular files

##RecordSource

##Actors

##Collectors
