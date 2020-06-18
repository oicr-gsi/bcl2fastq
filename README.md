# bcl2fastq

Workflow to produce FASTQ files from an Illumina instrument's run directory

## Overview

## Dependencies

* [bcl2fastq](https://emea.support.illumina.com/sequencing/sequencing_software/bcl2fastq-conversion-software.html)


## Usage

### Cromwell
```
java -jar cromwell.jar run bcl2fastq.wdl --inputs inputs.json
```

### Inputs

#### Required workflow parameters:
Parameter|Value|Description
---|---|---
`lanes`|Array[Int]+|The lane numbers to process from this run
`mismatches`|Int|Number of mismatches to allow in the barcodes (usually, 1)
`modules`|String|The modules to load when running the workflow. This should include bcl2fastq and the helper scripts.
`samples`|Array[Sample]+|The information about the samples. Tname of the sample which will determine the output file prefix. The list of barcodes in the format i7-i5 for this sample. If multiple barcodes are provided, they will be merged into a single output.
`runDirectory`|String|The path to the instrument's output directory.


#### Optional workflow parameters:
Parameter|Value|Default|Description
---|---|---|---
`basesMask`|String?|None|An Illumina bases mask string to use. If absent, the one written by the instrument will be used.
`timeout`|Int|40|The maximum number of hours this workflow can run for.


#### Optional task parameters:
Parameter|Value|Default|Description
---|---|---|---
`process.bcl2fastq`|String|"bcl2fastq"|The name or path of the BCL2FASTQ executable.
`process.bcl2fastqJail`|String|"bcl2fastq-jail"|The name ro path of the BCL2FASTQ wrapper script executable.
`process.extraOptions`|String|""|Any other options that will be passed directly to bcl2fastq.
`process.ignoreMissingBcls`|Boolean|false|Flag passed to bcl2fastq, allows missing bcl files.
`process.ignoreMissingFilter`|Boolean|false|Flag passed to bcl2fastq, allows missing or corrupt filter files.
`process.ignoreMissingPositions`|Boolean|false|Flag passed to bcl2fastq, allows missing or corrupt positions files.
`process.memory`|Int|32|The memory for the BCL2FASTQ process in GB.
`process.temporaryDirectory`|String|"."|A directory where bcl2fastq can dump massive amounts of garbage while running.
`process.threads`|Int|8|The number of processing threads to use when running BCL2FASTQ


### Outputs

Output | Type | Description
---|---|---
`fastqs`|Array[Output]+|A list of FASTQs generated and annotations that should be applied to them.


## Niassa + Cromwell

This WDL workflow is wrapped in a Niassa workflow (https://github.com/oicr-gsi/pipedev/tree/master/pipedev-niassa-cromwell-workflow) so that it can used with the Niassa metadata tracking system (https://github.com/oicr-gsi/niassa).

* Building
```
mvn clean install
```

* Testing
```
mvn clean verify \
-Djava_opts="-Xmx1g -XX:+UseG1GC -XX:+UseStringDeduplication" \
-DrunTestThreads=2 \
-DskipITs=false \
-DskipRunITs=false \
-DworkingDirectory=/path/to/tmp/ \
-DschedulingHost=niassa_oozie_host \
-DwebserviceUrl=http://niassa-url:8080 \
-DwebserviceUser=niassa_user \
-DwebservicePassword=niassa_user_password \
-Dcromwell-host=http://cromwell-url:8000
```

## Support

For support, please file an issue on the [Github project](https://github.com/oicr-gsi) or send an email to gsi@oicr.on.ca .

_Generated with wdl_doc_gen (https://github.com/oicr-gsi/wdl_doc_gen/)_
