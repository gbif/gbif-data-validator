# API Response
Specifications of the GBIF Data Validator API response.

## Response Example
```json
{
  "status": "[OK,FAILED]",
  "fileFormat": "[delimited | dwca | excel]",
  "indexeable": "[true|false]",
  "errorCode": "INVALID_FILE_FORMAT",
  "errorMessage": "Invalid file format",
  "issues": [
    {
      "issue": "RECORDED_DATE_MISMATCH",
      "count": 1,
      "identifierTerm" : "dwc:occurrenceId",
      "sample": [
        {
          "relatedData": {
            "line:": "1",
            "identifier": "occ-1",
            "dwc:month": "2",
            "dwc:day": "26",
            "dwc:year": "1996",
            "dwc:eventDate": "1996-01-26T01:00Z"
          }
        }
      ]
    },
    {
      "issue": "COLUMN_COUNT_MISMATCH",
      "count": 1,
      "identifierTerm" : "dwc:occurrenceId",
      "sample": [
        {
          "relatedData": {
            "line:": "1",
            "identifier": "occ-1",
            "expected" : "90",
            "found" : "89",
            "message": " Expected 90 columns but found 89"
          }
        }
      ]
    },
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
  ]
}
    
```

## Main structure

- `"status"` : The global status of the validation
- `"fileFormat"` : File format used be the server handle the submitted file
- `"indexeable"` : Is the provided resource indexeable by GBIF?
- `"errorCode"` : Contains the error code in case the provided resource can not be validated
- `"errorMessage"` : Contains human readable message in case the provided resource can not be validated
- `"issues"` : List of all issues found in the provided resource


