# dockstore_bcl2fastq

The workflow is made to run in Docker and uploaded to [Dockstore](https://docs.dockstore.org/en/develop/getting-started/getting-started.html).
You can find OICR's Dockstore page [here](https://dockstore.org/organizations/OICR).
The Docker container is based on [Modulator](https://gitlab.oicr.on.ca/ResearchIT/modulator), which builds environment modules to set up the docker runtime environment.

### Set Up and Run
Currently, this WDL must be run with Cromwell. 
It uses Cromwell configuration files to mount a directory to the docker container.
You must obtain run files locally and copy the run directory to local.

#### 1. Obtain Files Locally
In the test json, change file paths like so:
- File type files should be copied to local
    - E.g. use scp to copy from UGE
    - In the json, change the file path from UGE to local path
- String type files should be copied or moved to the mounted directory, if it's not already part of a module
    - In the json, change the file path to how the file would be accessed from inside the docker container
- $MODULE_ROOT paths can stay the same
```
# File type files
# File is copied to local machine
UGE: "/.mounts/labs/gsi/testdata/wgsPipeline/input_data/wgsPipeline_test_pcsi/hg19_random.genome.sizes.bed"
Dockstore: "/home/ubuntu/data/sample_data/callability/hg19_random.genome.sizes.bed"
 
# String type files
# /data_modules/ is a directory mounted to the docker container
UGE: "/.mounts/labs/gsi/modulator/sw/data/hg19-p13/hg19_random.fa"
Dockstore: "/data_modules/gsi/modulator/sw/data/hg19-p13/hg19_random.fa"
 
# Root type paths
# The value of $MODULE_ROOT changes, but the path stays the same
UGE: "$HG19_BWA_INDEX_ROOT/hg19_random.fa"
Dockstore: "$HG19_BWA_INDEX_ROOT/hg19_random.fa"
```

#### 2. Run with Cromwell
Submit the preprocessed subworkflow and modified json to Cromwell, with configs and options attached
```
# Validate the wrapper workflow and json
java -jar $womtool validate [WDL] --inputs [TEST JSON]
 
# For example:
java -jar $womtool validate wgsPipeline.wdl --inputs tests/wgsPipeline_test_cre_uge.json

# Submit to Cromwell
java -Dconfig.file=[CONFIG] -jar $cromwell run [WRAPPER WDL] --inputs [JSON] --options [OPTIONS]
 
# For example:
java -Dconfig.file=local.config -jar $cromwell run wgsPipeline.wdl --inputs tests/wgsPipeline_test_cre.json --options options.json
```