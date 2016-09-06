##BCL2FastQ Decider

Version 1.0, SeqWare version 1.1.1-gsi-rc1

###Overview

This decider launches the [BCL2FastQ (AKA Casava) Workflow](../workflow-casava) to demultiplex and convert BCL files from an Illumina sequencer run to FASTQ format. This decider assumes paired-end reads, so if this is not the case, the read-ends parameter must be used.

The decider operates as follows:
- retrieves all lanes that are available from the sample and lane provenance providers (specified in the provenance settings file)
- retrieves all lanes that have associated analysis from the analysis provenance providers (specified in the provenance settings file)
- calculates the difference between the above two lane name sets to determine the set of lanes that have not be analyzed
- creates a SeqWare IUS-LimsKey (a SeqWare object that is used to link LIMS data from provenance providers such as Pinery and SeqWare to SeqWare workflow runs) for each lane and a SeqWare IUS-LimsKey for each sample in the associated lane
- schedules a separate SeqWare workflow run for each lane and links the workflow run to the appropriate SeqWare IUS-LimsKey(s)

###Compile

```
mvn clean install
```

###Usage

java -jar Decider.jar --wf-accession \<bcl2fastq-workflow-accession\> --provenance-settings /path/to/provenance-settings.json

###Options

**Required**

Parameter | Type | Description \[default\]
----------|------|-------------------------
provenance-settings | String (Path) | Path to provenance settings json
wf-accession | Integer | Bcl2FastQ workflow accession

**Optional**

Please see [basic deciders](http://seqware.github.io/docs/17-plugins/#basicdecider) and [oicr deciders](https://github.com/oicr-gsi/pipedev/tree/master/deciders#options) for general decider options.

Additional optional parameters include:
Parameter | Type | Description \[default\]
----------|------|-------------------------
help | none | Display help
output-path | String (path) | Absolute path of directory to put the final files
output-folder | String (path) | Path to put the final files, relative to output-path
test | none | Runs the decider entirely except for launching the workflow
verbose | none | Log verbose output

**Note**
All of the [workflow properties](../workflow-casava) can be overridden by providing <property, value> pairs in the command, for example:
```
java -jar /path/to/decider.jar --wf-accession 000000 --provenance-settings /path/to/provenance-settings.json -- --property1 value1 --property2 value2
```

##Support

For support, please file an issue on the [Github project](https://github.com/oicr-gsi) or send an email to gsi@oicr.on.ca .

