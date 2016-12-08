# API Response
Specifications of the GBIF Data Validator API response.

## Response Example
```json
{
  "indexeable": "[true|false]",
  "fileName": "myoccurrencefile.csv",
  "fileFormat": "[delimited | dwca | excel]",
  "validationProfile": "GBIF_INDEXING_PROFILE",
  "errorCode": "INVALID_FILE_FORMAT",
  "errorMessage": "Invalid file format",
  "results": [
    {
      "fileName": "myoccurrencefile.csv",
      "numberOfLines": 18,
      "rowType": "http://rs.tdwg.org/dwc/terms/Occurrence",
      "termsFrequency": {
        "dwc:occurrenceID": 11
      },
      "interpretedValueCounts": {
        "gbif:taxonKey": 0
      },
      "issues": [{}]
    }
  ]
}
```

## Main structure
- `"indexeable"` : Is the provided resource indexeable by GBIF?
- `"fileName"` : File name of the submitted file
- `"fileFormat"` : File format used be the server handle the submitted file
- `"validationProfile"` : Validation profile used to validate the provided resource
- `"errorCode"` : Contains the error code in case the provided resource can not be validated
- `"errorMessage"` : Contains human readable message in case the provided resource can not be validated

### Results structure
- `"fileName"` : File name of the submitted file
- `"numberOfLines"` : Number of lines in the file
- `"rowType"` : rowType based on DarwinCore term
- `"termsFrequency"` : Contains frequency of all terms in the provided resource
- `"interpretedValueCounts"` : Contains counts of a preselected interpreted values
- `"issues"` : List of all issues found in the provided resource

## Issues structure

### Resource structure
Structure of the result of an evaluation of the structure of the resource.

```json
{
  "issue": "DUPLICATED_IDENTIFIER",
  "count": 1,
  "sample": [
    {
      "relatedData": {
        "dwc:occurrenceID": "1",
        "lines": [
          "1",
          "2"
        ]
      }
    }
  ]
}
```

### Record structure
```json
{
  "issue": "COLUMN_COUNT_MISMATCH",
  "count": 1,
  "identifierTerm": "dwc:occurrenceId",
  "sample": [
    {
      "relatedData": {
        "line:": "1",
        "identifier": "occ-1",
        "expected": "90",
        "found": "89"
      }
    }
  ]
}
```

### Record interpretation
Structure of the result of the interpretation of a record.
```json
{
  "issue": "RECORDED_DATE_MISMATCH",
  "count": 1,
  "identifierTerm": "dwc:occurrenceId",
  "sample": [
    {
      "lineNumber": 1,
      "relatedData": {
        "identifier": "occ-1",
        "dwc:month": "2",
        "dwc:day": "26",
        "dwc:year": "1996",
        "dwc:eventDate": "1996-01-26T01:00Z"
      }
    }
  ]
}
```

