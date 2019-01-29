package ca.on.oicr.pde.deciders.handlers;

import ca.on.oicr.pde.deciders.data.Bcl2FastqData;
import ca.on.oicr.pde.deciders.data.WorkflowRunV2;
import com.google.common.collect.Iterables;
import java.util.Arrays;
import java.util.SortedSet;

/**
 *
 * @author mlaszloffy
 */
public class Bcl2Fastq_2_9_1_Handler extends Bcl2FastqHandler {

    @Override
    public boolean isHandlerFor(String workflowName, String workflowVersion) {
        return "CASAVA".equals(workflowName) && Arrays.asList("2.9.1").contains(workflowVersion);
    }

    @Override
    public void validate(Bcl2FastqData data, WorkflowRunV2 workflowRun) {
        if (!data.getProvisionOutUndetermined()) {
            workflowRun.addError(Bcl2Fastq_2_7_1_Handler.class.getSimpleName() + " does not support disabling provision-out-undetermined");
        }
    }

    @Override
    public void modifyWorkflowRun(Bcl2FastqData data, WorkflowRunV2 workflowRun) {
        String runDir = null;
        SortedSet<String> runDirs = data.getLp().getSequencerRunAttributes().get("run_dir");
        if (runDirs == null || runDirs.size() != 1) {
            runDir = "ERROR";
            workflowRun.addError("Run dir is missing");
        } else {
            runDir = Iterables.getOnlyElement(runDirs);
            if (!runDir.endsWith("/")) {
                runDir = runDir + "/";
            }
        }
        if (data.getNoLaneSplitting()) {
            workflowRun.addProperty("no_lane_splitting", "true");
        }

        workflowRun.addProperty("run_folder", runDir);
        workflowRun.addProperty("intensity_folder", runDir + "Data/Intensities/");
        workflowRun.addProperty("called_bases", runDir + "Data/Intensities/BaseCalls/");
    }

}
