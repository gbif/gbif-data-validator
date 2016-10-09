# API Response

```json
{
  "status": "[OK,FAILED]",
  "fileFormat": "[delimited | dwca | excel]",
  "indexeable": "[true|false]",
  "errorCode": "INVALID_FILE_FORMAT", //only provided in case of error
  "errorMessage": "Invalid file format", //only provided in case of error
  "issues": [
    {
      "issue": "RECORDED_DATE_MISMATCH",
      "count": 16,
      "sample": [
        {
          "relatedData": {
            "line:": "1",
            "identifier": "occ-1",
            "identifierTerm" : "dwc:occurrenceId",
            "dwc:month": "2",
            "dwc:day": "26",
            "dwc:year": "1996",
            "dwc:eventDate": "1996-01-26T01:00Z"
          }
        } //other samples hidden for readability
      ]
    },
    {
      "issue": "COLUMN_COUNT_MISMATCH",
      "count": 1,
      "sample": [
        {
          "relatedData": {
            "line:": "1",
            "identifier": "occ-1",
            "identifierTerm" : "dwc:occurrenceId",
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