
## curl

Examples of using the `curl` to interact with the `gbif-data-validator`.

### Upload File

__DarwinCore Archive__
```
curl -i -X POST -F 'file=@dwca.zip;type=application/zip' http://localhost:8080/jobserver/submit
```

__Comma Separated Value (CSV)__
```
curl -i -X POST -F 'file=@my_occurrences.csv;type=text/csv' http://localhost:8080/jobserver/submit
```

__Reponse__
```
HTTP/1.1 201 Created
Date: Fri, 17 Mar 2017 08:34:39 GMT
Location: http://localhost:8080/jobserver/status/1489739662155
Content-Type: application/json
{"status":"ACCEPTED","jobId":1489739662155}
```
In this example, the jobId is 1489739662155.


### Check Status
```
curl http://localhost:8080/jobserver/status/{YOUR_JOB_ID}
```
