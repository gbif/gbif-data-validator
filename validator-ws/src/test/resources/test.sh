#!/usr/bin/env bash
curl -i -X POST -H "Content-Type: multipart/form-data" -F "format=TABULAR" -F "fieldsTerminatedBy=\t" -F "file=http://download.gbif.org/validation/0008759-160822134323880.csvar" http://api.gbif-dev.org/v1/data/validation/file


curl -i -X POST -H "Content-Type: multipart/form-data" -F "format=TABULAR" -F "fieldsTerminatedBy=\t" -F "file=@/Users/fmendez/dev/git/gbif/gbif-data-validator/validator-core/src/test/resources/0008759-160822134323880.csvar" http://localhost:8080/validate/file

curl -i -X POST -H "Content-Type: multipart/form-data" -F "format=TABULAR" -F "fieldsTerminatedBy=\t" -F "file=http://download.gbif.org/validation/0008759-160822134323880.csvar" http://localhost:8080/validate/file

