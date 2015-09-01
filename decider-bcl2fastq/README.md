##BCL2FastQ Decider

Version 1.0, SeqWare version 1.1.0

###Overview

This decider launches the [BCL2FastQ (AKA Casava) Workflow](../workflow-casava) to demultiplex and convert BCL files from an Illumina sequencer run to FASTQ format. The sequencer run name and run directory must be provided, and most of the other data is pulled from Pinery and SeqWare.

This decider does not extend OicrDecider or BasicDecider, and only the parameters listed below are available.

###Preconditions and Validation

* The run and samples must exist and be linked in the LIMS database
* The run and samples must exist in the SeqWare database
* Samples must have a sample type matching 'Illumina * Library Seq'

###Compile

```
mvn clean install
```

###Usage

java -jar Decider.jar --wf-accession \<bcl2fastq-Workflow-accession\> --run-name \<run-name\> --run-dir \<run-dir\> --pinery-url \<pinery-url\>

###Options

**Required**

Parameter | Type | Description \[default\]
----------|------|-------------------------
pinery-url | String (URL) | URL of Pinery webservice
run-name | String | sequencer run name
run-dir | String (path) | sequencer run directory
wf-accession | Integer | Bcl2FastQ workflow accession

**Optional**

Parameter | Type | Description \[default\]
----------|------|-------------------------
do-olb | none | Enable offline basecalling
help | none | Display help
ignore-missing-bcl | none | Bustard parameter
ignore-missing-stats | none | Bustard parameter
insecure-pinery | none | Force decider to ignore certificate errors from Pinery
manual-output | none | Set output path manually
no-metadata | none | Prevent metadata writeback
output-path | String (path) | Absolute path of directory to put the final files
output-folder | String (path) | Path to put the final files, relative to output-path
read-ends | Integer | 1 for single-end, 2 for paired-end \[2\]
test | none | Runs the decider entirely except for launching the workflow
verbose | none | Log verbose output

##Support

For support, please file an issue on the [Github project](https://github.com/oicr-gsi) or send an email to gsi@oicr.on.ca .

