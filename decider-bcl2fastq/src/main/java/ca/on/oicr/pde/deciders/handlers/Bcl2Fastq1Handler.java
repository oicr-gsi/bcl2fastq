package ca.on.oicr.pde.deciders.handlers;

import ca.on.oicr.pde.deciders.WorkflowRun;

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
    public WorkflowRun modifyWorkflowRun(Bcl2FastqData data, WorkflowRun run) {
        return run;
    }
}
