package ca.on.oicr.pde.workflows;

import ca.on.oicr.pde.utilities.workflows.OicrWorkflow;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import net.sourceforge.seqware.pipeline.workflowV2.model.Command;
import net.sourceforge.seqware.pipeline.workflowV2.model.Job;
import net.sourceforge.seqware.pipeline.workflowV2.model.SqwFile;
import org.apache.commons.io.FileUtils;

//CASAVA
public class WorkflowClient extends OicrWorkflow {

    private String binDir;
    private String dataDir;
    private String perl;
    //private String java;
    private String swModuleCallBustard;
    private String intensityFolder;
    private String flowcell;
    private String lanes;
    private String tile;
    private String bustard;
    private String bclToFastq;
    //private String slots;
    private String threads;
    private String memory;
    private String doOlb;
    private String calledBases;
    private String queue;
    private Integer readEnds;
    private Boolean manualOutput;
    private Boolean ignoreMissingBcl;
    private Boolean ignoreMissingStats;
    private String useBasesMask;
    private String mismatches;
    private String otherBclToFastqOptions;
    private String otherBustardOptions;

    private void WorkflowClient() {

        binDir = getWorkflowBaseDir() + "/bin/";
        dataDir = "data/";
        perl = getOptionalProperty("perl", binDir + "perl-5.14.1/perl");
        //java = getOptionalProperty("java", binDir + "jre1.6.0_29/bin/java");
        swModuleCallBustard = binDir + "sw_module_call_bustard.pl";
        intensityFolder = getProperty("intensity_folder");
        flowcell = getProperty("flowcell");
        readEnds = Integer.parseInt(getProperty("read_ends"));
        lanes = getProperty("lanes");
        tile = getProperty("tile");
        bustard = getProperty("bustard");
        bclToFastq = getProperty("bcl_to_fastq");
        //slots = getProperty("slots");
        threads = getProperty("threads");
        memory = getProperty("memory");
        doOlb = getProperty("do_olb");
        calledBases = getProperty("called_bases");
        queue = getOptionalProperty("queue", "");
        manualOutput = Boolean.valueOf(getOptionalProperty("manual_output", "false"));
        ignoreMissingBcl = Boolean.valueOf(getOptionalProperty("ignore_missing_bcl", "false"));
        ignoreMissingStats = Boolean.valueOf(getOptionalProperty("ignore_missing_stats", "false"));
        useBasesMask = getOptionalProperty("use_bases_mask", "");
        mismatches = getOptionalProperty("mismatches", "");
        otherBclToFastqOptions = getOptionalProperty("other_bcltofastq_options", "");
        otherBustardOptions = getOptionalProperty("other_bustard_options", "");

    }

    @Override
    public void setupDirectory() {

        WorkflowClient(); //constructor hack
        addDirectory(dataDir);

    }

    @Override
    public Map<String, SqwFile> setupFiles() {

        return this.getFiles();

    }

    @Override
    public void buildWorkflow() {

        List<ProcessEvent> ls = ProcessEvent.parseLanesString(lanes);

        for (String laneNum : ProcessEvent.getUniqueSetOfLaneNumbers(ls)) {
            Job bustardJob = getBustardJob(laneNum, ProcessEvent.getProcessEventListFromLaneNumber(ls, laneNum));
            bustardJob.setMaxMemory(memory).setQueue(queue);

            Job zipJob = getZipReportJob(getLanePath(dataDir, flowcell, laneNum), ProcessEvent.getLaneSwid(ls, laneNum));
            zipJob.setMaxMemory(memory).setQueue(queue);
            zipJob.addParent(bustardJob);
        }

    }

    private Job getBustardJob(String laneNum, List<ProcessEvent> ps) {

        String barcodes = ProcessEvent.getBarcodesStringFromProcessEventList(ps);

        //NOTE: newJob adds autoincrement counter to job name
        Job job = newJob("ID10_Bustard");
        Command c = job.getCommand();
        c.addArgument(perl).addArgument(swModuleCallBustard);
        c.addArgument("--intensity-folder " + intensityFolder);
        c.addArgument("--threads " + threads);
        c.addArgument("--bustard " + bustard);
        c.addArgument("--bcl-to-fastq " + bclToFastq);
        c.addArgument("--flowcell " + flowcell);
        c.addArgument("--lane " + laneNum);
        if (Integer.valueOf(tile) > 0) {
            c.addArgument("--tile " + tile);
        }
        c.addArgument("--barcodes " + barcodes);
        c.addArgument("--output-dir " + dataDir);
        c.addArgument("--do-olb " + doOlb);
        c.addArgument("--called-bases-folder " + calledBases);
        c.addArgument("--cleanup");
        if (ignoreMissingBcl) {
            c.addArgument("--ignore-missing-bcl");
        }
        if (ignoreMissingStats) {
            c.addArgument("--ignore-missing-stats");
        }
        if (!useBasesMask.isEmpty()) {
            c.addArgument("--use-bases-mask " + useBasesMask);
        }
        if (!mismatches.isEmpty()) {
            c.addArgument("--mismatches " + mismatches);
        }
        if (!otherBclToFastqOptions.isEmpty()) {
            c.addArgument("--other-bcltofastq-options " + otherBclToFastqOptions);
        }
        if (!otherBustardOptions.isEmpty()) {
            c.addArgument("--other-bustard-options " + otherBustardOptions);
        }

        //for each sample sheet entry, provision out the associated fastq(s).
        for (ProcessEvent p : ps) {
            SqwFile r1 = createOutputFile(generateOutputPath(dataDir, flowcell, laneNum, p.getIusSwAccession(), p.getSampleName(), p.getBarcode(), "1"),
                    "chemical/seq-na-fastq-gzip", manualOutput);
            r1.setParentAccessions(Arrays.asList(p.getLaneSwAccession(), p.getIusSwAccession()));
            job.addFile(r1);

            if (readEnds > 1) {
                SqwFile r2 = createOutputFile(generateOutputPath(dataDir, flowcell, laneNum, p.getIusSwAccession(), p.getSampleName(), p.getBarcode(), "2"),
                        "chemical/seq-na-fastq-gzip", manualOutput);
                r2.setParentAccessions(Arrays.asList(p.getLaneSwAccession(), p.getIusSwAccession()));
                job.addFile(r2);
            }
        }

        // if the sample sheet for the lane does not have a "NoIndex" entry, Undetermined_indicies will exist
        if (!ProcessEvent.containsBarcode(ps, "NoIndex")) {
            SqwFile r1 = createOutputFile(getUndeterminedFastqPath(dataDir, flowcell, laneNum, "1"), "chemical/seq-na-fastq-gzip", manualOutput);
            r1.setParentAccessions(Arrays.asList(ProcessEvent.getLaneSwid(ps, laneNum)));
            job.addFile(r1);
            if (readEnds > 1) {
                SqwFile r2 = createOutputFile(getUndeterminedFastqPath(dataDir, flowcell, laneNum, "2"), "chemical/seq-na-fastq-gzip", manualOutput);
                r2.setParentAccessions(Arrays.asList(ProcessEvent.getLaneSwid(ps, laneNum)));
                job.addFile(r2);
            }
        }

        return job;

    }

    private Job getZipReportJob(String inputDirectoryPath, String laneAccession) {

        String zipFileName = FileUtils.getFile(inputDirectoryPath).getName() + ".zip";
        String outputZipFilePath = dataDir + zipFileName;

        Job job = newJob("ZipReports");

        Command c = job.getCommand();
        c.addArgument("cd " + inputDirectoryPath + " &&");
        c.addArgument("zip -r");
        c.addArgument("../" + zipFileName);
        c.addArgument("."); //currect directory which is "inputDirectoryPath"
        c.addArgument("-x \\*.fastq.gz 1>/dev/null");

        SqwFile f = createOutputFile(outputZipFilePath, "application/zip-report-bundle", manualOutput);
        f.setParentAccessions(Arrays.asList(laneAccession));
        job.addFile(f);

        return job;

    }

    public static String generateOutputPath(String dataDir, String flowcell, String laneNum, String iusSwAccession, String sampleName, String barcode, String read) {

        StringBuilder o = new StringBuilder();
        o.append(getLanePath(dataDir, flowcell, laneNum));
        o.append("/Project_na/Sample_SWID_").append(iusSwAccession).append("_").append(sampleName).append("_").append(flowcell);
        o.append("/SWID_").append(iusSwAccession).append("_").append(sampleName).append("_").append(flowcell).append("_").append(barcode).append("_");
        o.append("L00").append(laneNum).append("_R").append(read).append("_001.fastq.gz");

        return o.toString();

    }

    public static String getUndeterminedFastqPath(String dataDir, String flowcell, String laneNum, String read) {

        StringBuilder o = new StringBuilder();
        o.append(getLanePath(dataDir, flowcell, laneNum));
        o.append("/Undetermined_indices");
        o.append("/Sample_lane").append(laneNum);
        o.append("/lane").append(laneNum).append("_Undetermined_L00").append(laneNum).append("_R").append(read).append("_001.fastq.gz");

        return o.toString();

    }

    public static String getLanePath(String dataDir, String flowcell, String laneNum) {

        StringBuilder o = new StringBuilder();
        o.append(dataDir);
        o.append("Unaligned_").append(flowcell).append("_").append(laneNum);

        return o.toString();

    }

}
