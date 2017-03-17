
## curl

### Upload a File
```
curl -i -X POST -F 'file=@dwca.zip;type=application/zip' http://localhost:8080/jobserver/submit
```
Reponse: 
```
HTTP/1.1 201 Created
Date: Fri, 17 Mar 2017 08:34:39 GMT
Location: http://localhost:8080/jobserver/status/1489739662155
Content-Type: application/json
{"status":"ACCEPTED","jobId":1489739662155}
```
