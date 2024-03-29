# GBIF Data Validator (2017–2021)

This project will soon be superseded. Validation will be done within the [Pipelines](https://github.com/gbif/pipelines) project, allowing it to be maintained as a primary component of data ingestion and interpretation.

The new validator is available for testing at https://www.gbif-uat.org/tools/data-validator

## Vision

The GBIF Data Validator is a service to provide a report on the syntactical correctness and the validity of content contained in a dataset. It allows for anybody to quickly determine issues in data without registering the dataset in GBIF. It provides the ability to easily do a *pre-publication* review of data. It supports various data formats in use within GBIF including:

 - Darwin Core Archives (DwC-A): Occurrence, Taxon, Sampling Event
 - [Excel spreadsheets](http://www.gbif.org/newsroom/news/new-darwin-core-spreadsheet-templates)
 - Simple CSV files using Darwin Core terms for header values
 - ABCD Archives (ABCD-A)

> Only formats where the data are represented in a single file are
> supported.  Where data is needed to be crawled (e.g. TAPIR) a separate
> process must crawl and reformat the data in advance.  BioCASe tools
> supports both ABCD-A and DwC-A as of 2015

It provides information to data publishers and data users on items such as:

 - Is the file correctly formatted?  (Encoding, uniqueness of primary keys, referential integrity etc)
 - Are data mapped to data profiles correctly? (e.g. accessible DwC extensions)
 - Are the mandatory fields present and correctly populated?
 - Are data well formed (e.g. dates, geodetic datum etc)
 - Is there contradictory information within the data (e.g. coordinate lies outside the stated country)
 - Does the dataset meet the requirements for indexing at GBIF?
 - What errors and warnings would be generated if indexed by GBIF?
 - Is the dataset metadata well structured, and containing the minimum necessary content fields (e.g. contact information)
 - What might a user to to improve the data and metadata content (e.g. add geodeticDatum field, enrich the metadata description, add a DOI for the funding agency)

Over time it is expected that this project will be enhanced with growing validity checking, and profiles to address targeted use of the data (e.g. suitability for a specific analysis).

## Rationale

This project serves as a replacement for the [GBIF Darwin Core Validator 3](https://github.com/gbif/dwca-validator3). The perceived issues with the validator are:

1. It is inconsistent with what happens at indexing in GBIF — data said to be valid are not indexable under certain circumstances
2. It doesn’t expose information about data interpretation issues that later can appear while indexing a file in the GBIF portal — they use different underlying libraries
3. It does not do any meaningful content level validation — only correctness of the file (e.g. referential integrity)
4. It only supports DwC-A
5. It does not provide any means to verify the data is suitable for a specific use

## Architecture

* A Java based web service provides the entry for validation of data.  A client can POST a dataset, or provide a callback URL to an online dataset for validation.  Since validation may take some time, this returns a callback to check the job status.  Once complete, the validation report is available as JSON.  During validation, the service makes use of both libraries for validation, and online web services.
* A simple Node based web application provides a basic front end to the validation web service.  This application is part of the [GBIF.org project](https://github.com/gbif/portal16)   .

## Documentation
 * [Here](https://github.com/gbif/gbif-data-validator/blob/master/doc/README.md)

## Contributions and process
Enhancement requests are welcome through issues and/or pull requests.
