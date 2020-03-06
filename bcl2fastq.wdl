version 1.0

workflow bcl2fastq {
  input {
    Array[String]+ barcodes
    String? basesMask
    Array[Int]+ lanes
    Int mismatches
    String modules
    String runDirectory
    String sampleName
    Int timeout = 40
  }
  parameter_meta {
    barcodes: "A list of barcodes in the format i7-i5 for this sample. If multiple barcodes are provided, they will be merged into a single output."
    basesMask: "An Illumina bases mask string to use. If absent, the one written by the instrument will be used."
    lanes: "The lane numbers to process from this run"
    mismatches: "Number of mismatches to allow in the barcodes (usually, 1)"
    modules: "The modules to load when running the workflow. This should include bcl2fastq and the helper scripts."
    runDirectory: "The path to the instrument's output directory."
    sampleName: "The name of the sample which will determine the output file prefix."
    timeout: "The maximum number of hours this workflow can run for."
  }
  meta {
    author: "Andre Masella"
    description: "Workflow to produce FASTQ files from an Illumina instrument's run directory"
    dependencies: [{
      name: "bcl2fastq",
      url: "https://emea.support.illumina.com/sequencing/sequencing_software/bcl2fastq-conversion-software.html"
    }]
    output_meta: {
      fastqs: "A list of FASTQs generated and annotations that should be applied to them."
    }
  }
  call process {
    input:
      barcodes = barcodes,
      basesMask = basesMask,
      lanes = lanes,
      mismatches  = mismatches,
      modules = modules,
      runDirectory = runDirectory,
      sampleName = sampleName,
      timeout = timeout
  }
  output {
    Pair[Array[File]+, Map[String, String]] fastqs = (process.files, process.attributes)
  }
}


task process {
  input {
    Array[String]+ barcodes
    String? basesMask
    String bcl2fastq = "bcl2fastq"
    String extraOptions = ""
    Boolean ignoreMissingBcls = false
    Boolean ignoreMissingFilter = false
    Boolean ignoreMissingPositions = false
    Array[Int]+ lanes
    Int memory = 32
    Int mismatches
    String modules
    String runDirectory
    String sampleName
    String temporaryDirectory = "."
    Int threads = 8
    Int timeout = 40
  }
  parameter_meta {
    barcodes: "A list of barcodes in the format i7-i5 for this sample. If multiple barcodes are provided, they will be merged into a single output."
    basesMask: "An Illumina bases mask string to use. If absent, the one written by the instrument will be used."
    bcl2fastq: "The name of the BCL2FASTQ executable."
    extraOptions: "Any other options that will be passed directly to bcl2fastq."
    ignoreMissingBcls: "Flag passed to bcl2fastq, allows missing bcl files."
    ignoreMissingFilter: "Flag passed to bcl2fastq, allows missing or corrupt filter filesa."
    ignoreMissingPositions: "Flag passed to bcl2fastq, allows missing or corrupt positions files."
    lanes: "The set of lanes to process."
    memory: "The memory for the BCL2FASTQ process in GB."
    mismatches: "Number of mismatches to allow in the barcodes (usually, 1)"
    modules: "The modules to load when running the workflow. This should include bcl2fastq and the helper scripts."
    runDirectory: "The path to the instrument's output directory."
    sampleName: "The name of the sample which will determine the output file prefix."
    temporaryDirectory: "A directory where bcl2fastq can dump massive amounts of garbage while running."
    threads: "The number of processing threads to use when running BCL2FASTQ"
    timeout: "The maximum number of hours this workflow can run for."
  }
  meta {
    output_meta: {
      fastqs: "The FASTQ files for reads for the sample with an attributes set containing the number of reads"
    }
  }

  command <<<
    bcl2fastq-sample-sheet \
      --lanes ~{sep=" " lanes} \
      --sample "~{sampleName}" \
      --barcodes ~{sep=" " barcodes} \
      --sample-sheet sample-sheet.csv &&
    ~{bcl2fastq} \
      --barcode-mismatches ~{mismatches} \
      --input-dir "~{runDirectory}/Data/Intensities/BaseCalls" \
      --intensities-dir "~{runDirectory}/Data/Intensities" \
      --no-lane-splitting \
      --output-dir "~{temporaryDirectory}" \
      --processing-threads ~{threads} \
      --runfolder-dir "~{runDirectory}" \
      --sample-sheet sample-sheet.csv \
      --tiles "^(s_)?[~{sep="" lanes}]_" \
      ~{if ignoreMissingBcls then "--ignore-missing-bcls" else ""} \
      ~{if ignoreMissingFilter then "--ignore-missing-filter" else ""} \
      ~{if ignoreMissingPositions then "--ignore-missing-positions" else ""} \
      ~{if defined(basesMask) then "--use-bases-mask ~{basesMask}" else ""} \
      ~{extraOptions} &&
    rm Undetermined*.fastq.gz &&
    for read in $(jq -r '[.ConversionResults[].Undetermined.ReadMetrics[].ReadNumber] | unique | .[]' "~{temporaryDirectory}"/Stats/Stats.json); do
       # This will try to find all the files matching the current read and
       # concatentat them together into a new gzip file while simultaneously
       # counting them. Since there might be no files, we send a null gzipped
       # data stream on stdin; if zcat gets no parameters from find, it will
       # read stdin, otherwise it will read the files. We overwrite the
       # annotations file because we assume the read count is going to be the
       # same for every read (yay Illumina for read to mean two different
       # things).

       gzip < /dev/null | zcat -c $(find "~{temporaryDirectory}" -name "~{sampleName}*_S*_R${read}_001.fastq.gz") \
         | tee >(gzip -n > "~{sampleName}_R${read}.fastq.gz") \
         | wc -l | sed -e s/^/read_count\\t/ > annotations
       echo "~{sampleName}_R${read}.fastq.gz" >> output_fastqs
    done &&
    find "~{temporaryDirectory}" -type f -exec {} \;
  >>>
  output {
    Array[File]+ files = read_lines("output_fastqs")
    Map[String, String] attributes = read_map("annotations")
  }
  runtime {
    memory: "~{memory}G"
    modules: "~{modules}"
    timeout: "~{timeout}"
  }
}
