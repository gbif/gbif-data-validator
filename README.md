# gbif-data-validator
Home of the GBIF's data validator

WORK IN PROGRESS

##Situation

  •	[http://tools.gbif.org/dwca-validator/](http://tools.gbif.org/dwca-validator/) doesn’t expose information about data interpretation issues that later can appear while indexing a file in the GBIF portal; which is mainly caused due that this tool doesn’t make use of the validation libraries used during data interpretation.
  
  •	Data publishers cannot validate their occurrence data before its submission to GBIF, which leads to use the GBIF portal as an indexing and data-cleaning platform.
  
  •	GBIF doesn’t provide a service to validate generic occurrence data in a different format to DwC-A.
  
  •	The current validation libraries do not consider the usage of data quality profiles to assess the usage of datasets in particular use cases (a.k.a. fitness for use)
  
  
##High-level Requirements

* The validation routines applied in the GBIF platform must be consistent between: occurrence data interpretation, IPT data publishing and data validation as isolated service (similar to the dwca-validator).
  
* A data validation service must help users/publishers to diagnose issues at different levels:
  
 * Archive/File level: Is this file a valid DwC-A file? Are the fields/columns in this file recognised as indexable terms by GBIF?
  
 * Dataset level: would be this dataset indexed by GBIF? In summary what data issues were found in this dataset? Would this dataset usable in a particular context (fitness-for-use)?
  
 * Occurrence record level: what data issues were found in this record? What data interpretation would GBIF performed on this record? Is there any way of how this record can be improved?

