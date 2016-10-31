package ca.on.oicr.pde.deciders.handlers;

import ca.on.oicr.pde.deciders.DataMismatchException;
import ca.on.oicr.pde.deciders.WorkflowRun;
import com.google.common.collect.Iterables;
import java.util.Arrays;
import java.util.SortedSet;

/**
 *
 * @author mlaszloffy
 */
public class Bcl2Fastq2Handler extends Bcl2FastqHandler {

    @Override
    public boolean isHandlerFor(String workflowName, String workflowVersion) {
        return "CASAVA".equals(workflowName) && Arrays.asList("2.9.1").contains(workflowVersion);
    }

    @Override
    public WorkflowRun modifyWorkflowRun(Bcl2FastqData data, WorkflowRun run) throws DataMismatchException {
        String runDir = null;
        SortedSet<String> runDirs = data.getLp().getSequencerRunAttributes().get("run_dir");
        if (runDirs == null || runDirs.size() != 1) {
            throw new DataMismatchException("Run dir is missing");
        } else {
            runDir = Iterables.getOnlyElement(runDirs);
            if (!runDir.endsWith("/")) {
                runDir = runDir + "/";
            }
        }
        run.addProperty("run_folder", runDir);
        run.addProperty("intensity_folder", runDir + "Data/Intensities/");
        run.addProperty("called_bases", runDir + "Data/Intensities/BaseCalls/");

        return run;
    }

}
