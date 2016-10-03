# bcl2fastq

This [SeqWare](http://seqware.github.io/) workflow transforms Illumina base calls from sequencer run directories into paired-end fastq.gz files and log files using [bcl2fastq](http://support.illumina.com/downloads/bcl2fastq-conversion-software-v2-18.html). The "CASAVA" name is a misnomer since bcl2fastq is used exclusively, but is maintained for historical purposes.

##Workflow

The source code for the [bcl2fastq SeqWare workflow](workflow-casava) is freely available from github at https://github.com/oicr-gsi .

###Decider
For most workflows, the recommended way to configure and launch workflow runs is by using a [decider](http://seqware.github.io/docs/6-pipeline/basic_deciders/). Deciders query SeqWare's metadata repository, looking for appropriate files to launch against.

Since bcl2fastq does not operate on files like a conventional workflow but rather on sequencer run directories, there is no universal decider for this workflow. We recommend scripting a decider that will create the appropriate structures in SeqWare's metadata to represent a sequencer run that is associated with particular libraries. SeqWare's command line interface provides the ability to create these structures. [A short tutorial is available in the SeqWare documentation](http://seqware.github.io/docs/3-getting-started/user-tutorial/#creating-studies-experiments-and-samples).

Here some pseudocode for that process:
```
seqware create study || provide study accession 

seqware create experiment --study-accession <acc>

for all samples: 
    seqware create sample(s) --experiment-accession <acc>
    record sample-accession

#

seqware create sequencer-run

for all lanes:
    seqware create lane --sequencer-run-accession <acc>
    record lane-accession

    for all barcodes:
         seqware create ius --lane-accession <acc> --sample-accession <acc>
         record ius-accession
```

The two halves (separated by #) can be run independently, but you will need to keep records of the sample accessions so that you can link them with the IUS later (indivisible unit of sequencing, equivalent to 1 lane without barcoding or 1 barcode in a lane). You will also need to record the lane and ius accessions for use in the `lanes` string (see below). It may be useful to create the `lanes` string as you create the metadata objects.

Once these structures exist in SeqWare's metadata repository, you can schedule the workflow. Make sure that the run is finished sequencing before launching this workflow.  We recommend overriding the options below in the 'required' and 'input/output' categories.


##Support
For support, please file an issue on the [Github project](https://github.com/oicr-gsi) or send an email to gsi@oicr.on.ca .
