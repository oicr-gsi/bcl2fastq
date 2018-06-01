package ca.on.oicr.pde.deciders.handlers;

import ca.on.oicr.pde.deciders.data.Bcl2FastqData;
import ca.on.oicr.pde.deciders.data.WorkflowRunV2;

/**
 *
 * @author mlaszloffy
 */
public class Bcl2Fastq1Handler extends Bcl2FastqHandler {

    @Override
    public boolean isHandlerFor(String workflowName, String workflowVersion) {
        return "CASAVA".equals(workflowName) && "2.7.1".equals(workflowVersion);
    }

    @Override
    public WorkflowRunV2 modifyWorkflowRun(Bcl2FastqData data, WorkflowRunV2 run) {
        return run;
    }
}
