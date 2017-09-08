## DataFile
Documentation of the structure of the different classes related to "file" in the context of the GBIF Data Validator:

### DataFile
[DataFile](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/DataFile.java)
represents the resource to be validated as provided by the user. It can represent any format from [FileFormat](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/model/FileFormat.java).

### DwcDataFile
Each DataFile will be prepared/transformed into a [DwcDataFile](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/DwcDataFile.java)
to facilitate their usage within the evaluation chain. A DwcDataFile represents a DataFile in the context of DarwinCore 
including core and extension as TabularDataFile and the path of the metadata document (if any).

### TabularDataFile
[TabularDataFile](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/api/TabularDataFile.java) 
represents what is required to be validated at the record level, in a standardized format. Each `TabularDataFile` is linked to a `RowTypeKey` wich identifies
it uniquely within the context of the DwcDataFile. It is also possible to split a `TabularDataFile` into multiple smaller pieces to run record level evaluations in parallel.

### DataFileFactory
 [DataFileFactory](https://github.com/gbif/gbif-data-validator/blob/master/validator-processor/src/main/java/org/gbif/validation/source/DataFileFactory.java) is the entry point for everything related to `DataFile`, `DwcDataFile` and `TabularDataFile`.

#### List of Preparation and Transformation:
 * Counting the number of lines
 * Extracting/Reading the headers
 * Darwin Core Archive into a list of all its components (core + extensions)
 * Excel file into CSV
 * Standardisation of end line characters

The result of those operations is kept in a DwcDataFile and is obtained by the `prepareDataFile` method of the DataFileFactory.

### Simplified example
Simplified example of the sequence when want to validate an Excel Spreadsheet named occurrence.xlsx with 5000 lines.

 * DataFile => `path:occurrence.xlsx, fileFormat:SPREADSHEET`
 * Transformation => from Excel into CSV
 * DwcDataFile => `core: TabularFile`
 * TabularFile: `filePath:occurrence.csv, rowTypeKey:core_Occurrence`
 * Transformation => Split by smaller files of 1000 lines (example only)
 * TabularFile[0] => `filePath:occurrence.csv, rowTypeKey:core_Occurrence, fileLineOffset:0, numOfLines:1000`
 * TabularFile[1] => `filePath:occurrence.csv, rowTypeKey:core_Occurrence, fileLineOffset:1000, numOfLines:1000`
