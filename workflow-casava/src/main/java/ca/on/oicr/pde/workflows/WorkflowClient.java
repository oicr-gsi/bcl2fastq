/**
 *  Copyright (C) 2014  Ontario Institute of Cancer Research
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * Contact us:
 * 
 *  Ontario Institute for Cancer Research  
 *  MaRS Centre, West Tower
 *  661 University Avenue, Suite 510
 *  Toronto, Ontario, Canada M5G 0A3
 *  Phone: 416-977-7599
 *  Toll-free: 1-866-678-6427
 *  www.oicr.on.ca
**/

package ca.on.oicr.pde.workflows;

import ca.on.oicr.pde.utilities.workflows.OicrWorkflow;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import net.sourceforge.seqware.pipeline.workflowV2.model.Command;
import net.sourceforge.seqware.pipeline.workflowV2.model.Job;
import net.sourceforge.seqware.pipeline.workflowV2.model.SqwFile;
import org.apache.commons.io.FileUtils;

/**
 * Workflow for bcl2fastq. See the README for more information.
 */
public class WorkflowClient extends OicrWorkflow {

    private String binDir;
    private String dataDir;
    private String perl;
    //private String java;
    private String swModuleCallBustard;
    private String runFolder;
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
    private Boolean ignoreMissingFilter;
    private Boolean ignoreMissingPositions;
    private Boolean ignoreMissingStats;
    private String useBasesMask;
    private Boolean noLaneSplitting;
    private String mismatches;
    private String otherBclToFastqOptions;
    private String otherBustardOptions;

    private void WorkflowClient() {

        binDir = getWorkflowBaseDir() + "/bin/";
        dataDir = "data/";
        perl = getOptionalProperty("perl", binDir + "perl-5.14.1/perl");
        //java = getOptionalProperty("java", binDir + "jre1.6.0_29/bin/java");
        swModuleCallBustard = binDir + "sw_module_call_bustard.pl";
        runFolder = getProperty("run_folder");
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
        ignoreMissingFilter = Boolean.valueOf(getOptionalProperty("ignore_missing_filter", "false"));
        ignoreMissingPositions = Boolean.valueOf(getOptionalProperty("ignore_missing_positions", "false"));
        ignoreMissingStats = Boolean.valueOf(getOptionalProperty("ignore_missing_stats", "false"));
        useBasesMask = getOptionalProperty("use_bases_mask", "");
        noLaneSplitting = Boolean.valueOf(getOptionalProperty("no_lane_splitting", "false"));
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
        c.addArgument("--run-folder " + runFolder);
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
        if (ignoreMissingFilter) {
            c.addArgument("--ignore-missing-filter");
        }
        if (ignoreMissingPositions) {
            c.addArgument("--ignore-missing-positions");
        }
        if (ignoreMissingStats) {
            c.addArgument("--ignore-missing-stats");
        }
        if (!useBasesMask.isEmpty()) {
            c.addArgument("--use-bases-mask " + useBasesMask);
        }
        if (noLaneSplitting) {
            c.addArgument("--no-lane-splitting");
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

        //Temporary workaround until https://jira.oicr.on.ca/browse/SEQWARE-1895 is fixed
        c.addArgument("2>lane_" + laneNum + "_stderr.log");

        //for each sample sheet entry, provision out the associated fastq(s).
        int sampleSheetRowNumber = 1;
        for (ProcessEvent p : ps) {
            SqwFile r1 = createOutputFile(
                    getOutputPath(dataDir, flowcell, laneNum, p.getIusSwAccession(), p.getSampleName(), p.getBarcode(),
                            "1", p.getGroupId(), sampleSheetRowNumber, noLaneSplitting),
                    //maintain file name produced by previous versions of bcl2fastq
                    generateOutputFilename(flowcell, laneNum, p.getIusSwAccession(), p.getSampleName(), p.getBarcode(), "1", p.getGroupId()),
                    "chemical/seq-na-fastq-gzip",
                    manualOutput);
            r1.setParentAccessions(Arrays.asList(p.getLaneSwAccession(), p.getIusSwAccession()));
            job.addFile(r1);

            if (readEnds > 1) {
                SqwFile r2 = createOutputFile(
                        getOutputPath(dataDir, flowcell, laneNum, p.getIusSwAccession(), p.getSampleName(), p.getBarcode(),
                                "2", p.getGroupId(), sampleSheetRowNumber, noLaneSplitting),
                        //maintain file name produced by previous versions of bcl2fastq
                        generateOutputFilename(flowcell, laneNum, p.getIusSwAccession(), p.getSampleName(), p.getBarcode(), "2", p.getGroupId()),
                        "chemical/seq-na-fastq-gzip",
                        manualOutput);
                r2.setParentAccessions(Arrays.asList(p.getLaneSwAccession(), p.getIusSwAccession()));
                job.addFile(r2);
            }

            sampleSheetRowNumber++;
        }

        // if the sample sheet for the lane does not have a "NoIndex" entry, Undetermined_indicies will exist
        if (!ProcessEvent.containsBarcode(ps, "NoIndex")) {
            SqwFile r1 = createOutputFile(
                    getUndeterminedFastqPath(dataDir, flowcell, laneNum, "1", noLaneSplitting),
                    //maintain file name produced by previous versions of bcl2fastq
                    "lane" + laneNum + "_Undetermined_L00" + laneNum + "_R1_001.fastq.gz",
                    "chemical/seq-na-fastq-gzip",
                    manualOutput);
            r1.setParentAccessions(Arrays.asList(ProcessEvent.getLaneSwid(ps, laneNum)));
            job.addFile(r1);
            if (readEnds > 1) {
                SqwFile r2 = createOutputFile(
                        getUndeterminedFastqPath(dataDir, flowcell, laneNum, "2", noLaneSplitting),
                        //maintain file name produced by previous versions of bcl2fastq
                        "lane" + laneNum + "_Undetermined_L00" + laneNum + "_R2_001.fastq.gz",
                        "chemical/seq-na-fastq-gzip",
                        manualOutput);
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

    public static String generateOutputFilename(String flowcell, String laneNum, String iusSwAccession, String sampleName, String barcode, String read, String groupId) {
        StringBuilder o = new StringBuilder();
        o.append("SWID_");
        o.append(iusSwAccession).append("_");
        o.append(sampleName).append("_");
        o.append(groupId).append("_");
        o.append(flowcell).append("_");
        o.append(barcode).append("_");
        o.append("L00").append(laneNum).append("_");
        o.append("R").append(read).append("_");
        o.append("001.fastq.gz");
        return o.toString();
    }

    public static String getOutputPath(String dataDir, String flowcell, String laneNum, String iusSwAccession, String sampleName,
            String barcode, String read, String groupId, int sampleSheetRowNumber, boolean noLaneSplitting) {
        StringBuilder o = new StringBuilder();
        o.append(getLanePath(dataDir, flowcell, laneNum));
        o.append("SWID_").append(iusSwAccession).append("_").append(sampleName).append("_").append(groupId).append("_").append(flowcell).append("_");
        o.append("S").append(sampleSheetRowNumber).append("_");
        if (!noLaneSplitting) {
            o.append("L00").append(laneNum).append("_");
        }
        o.append("R").append(read).append("_001.fastq.gz");

        return o.toString();
    }

    public static String getUndeterminedFastqPath(String dataDir, String flowcell, String laneNum, String read, boolean noLaneSplitting) {
        StringBuilder o = new StringBuilder();
        o.append(getLanePath(dataDir, flowcell, laneNum));
        o.append("Undetermined_S0_");
        if (!noLaneSplitting) {
            o.append("L00").append(laneNum).append("_");
        }
        o.append("R").append(read).append("_001.fastq.gz");

        return o.toString();
    }

    public static String getLanePath(String dataDir, String flowcell, String laneNum) {
        StringBuilder o = new StringBuilder();
        o.append(dataDir);
        o.append("Unaligned_").append(flowcell).append("_").append(laneNum).append("/");

        return o.toString();
    }

    protected SqwFile createOutputFile(String workingPath, String outputFileName, String metatype, boolean manualOutput) {
        SqwFile file = new SqwFile();
        file.setForceCopy(true);
        file.setIsOutput(true);
        file.setSourcePath(workingPath);
        file.setType(metatype);

        if (manualOutput) {
            file.setOutputPath(this.getMetadata_output_file_prefix() + getMetadata_output_dir() + "/" + outputFileName);
        } else {
            file.setOutputPath(this.getMetadata_output_file_prefix()
                    + getMetadata_output_dir() + "/" + this.getName() + "_" + this.getVersion() + "/" + this.getRandom() + "/" + outputFileName);
        }

        return file;
    }

}
