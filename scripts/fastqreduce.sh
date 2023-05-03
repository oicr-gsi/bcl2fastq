#!/bin/bash

programname=$0
function usage {
    echo ""
    echo "Splits a fastq file based on the supplied proportion to two files, keep and exclude."
    echo ""
    echo "usage: $programname --file string --id string --p numeric(0-1) --seed numeric "
    echo ""
    echo "  --file string           name of the zipped fastq file to reduce"
#    echo "  --id string             prefix for the output, will be appended with keep.fastq.gz and exclude.fastq.gz"
    echo "  --p floating point      a value between 0 and 1 indicating the proportion of reads to keep"
    echo "  --seed integer          a random seed, the same value should be applied to fastq pairs"
    echo ""
}

function die {
    printf "Script failed: %s\n\n" "$1"
    exit 1
}


### convert arguments to variables
while [ $# -gt 0 ]; do
    if [[ $1 == "--help" ]]; then
        usage
        exit 0
    elif [[ $1 == "--"* ]]; then
        v="${1/--/}"
        declare "$v"="$2"
        shift
    fi
    shift
done






#### check that all required values are supplied
if [[ -z $file ]]; then
    usage
    die "Missing parameter --file"
#   elif [[ -z $id ]]; then
#    usage
#    die "Missing parameter --id"
elif [[ -z $p ]]; then
    usage
    die "Missing parameter --p"
elif [[ -z $seed ]]; then
    usage
    die "Missing parameter --seed"
fi

### get the id as the basename of the file
id=`basename $file .fastq.gz`


echo "reducing $file, keeping $p records"
zcat $file | paste -d'|' - - - - | gawk -v id=$id -v seed=$seed -v p=$p ' BEGIN {srand(seed)} {f = id (rand() <= p ? ".keep.fastq.gz" : ".archive.fastq.gz"); gsub(/\|/,"\n"); print | "gzip >" f} '

zcat $file | paste -d'|' - - - - | gawk -v id=$id -v seed=$seed -v p=$p  \
'BEGIN {srand(seed)} \
{f = (rand() <= p ? "keep" : "archive"); gsub(/\|/,"\n"); count[f]++;echo count["keep"];print | "gzip >" id "." f ".fastq.gz"} \
END { print "{\"keep\":" count["keep"] ",\"archive\":" count["archive"] "}" > "stats.json" }'

'

