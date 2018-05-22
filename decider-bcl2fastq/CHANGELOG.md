## 1.2
- [GP-1516](https://jira.oicr.on.ca/browse/GP-1516)
  - Trim barcode when index was sequenced with less cycles
  - Schedule workflow runs by barcode length (support for calculating use_bases_mask from sequenced/run bases mask)
  - Support for "run_bases_mask" lane attribute or overriding via "--override-run-bases-mask"
  - Don't retrieve analysis provenance if "--ignore-previous-lims-keys" is provided
## 1.1.1 - 2017-07-05
- [GP-1224](https://jira.oicr.on.ca/browse/GP-1224) - Filter by sample name + demultiplex single sample mode
- Update to pipedev 2.2
## 1.1 - 2017-03-15
- Update to pipedev 2.1 (provenance-client 1.1)
- [GP-1022](https://jira.oicr.on.ca/browse/GP-1022) - Validate barcodes before scheduling
- [GP-1040](https://jira.oicr.on.ca/browse/GP-1040) - Reduce memory usage
- [GP-1082](https://jira.oicr.on.ca/browse/GP-1082) - Modify decider to remain in error state if there is an error during workflow run scheduling
## 1.0 - 2017-02-02
- Initial java implementation of the bcl2fastq/CASAVA decider
- [GP-867](https://jira.oicr.on.ca/browse/GP-867) - Add support to create IUS-LimsKey, a SeqWare object that is used to link LIMS data from Pinery and SeqWare to SeqWare workflow runs
- [GP-875](https://jira.oicr.on.ca/browse/GP-875) - Support for SeqWare 1.1.1-gsi
