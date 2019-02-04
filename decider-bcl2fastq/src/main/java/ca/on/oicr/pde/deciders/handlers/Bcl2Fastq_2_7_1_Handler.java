package ca.on.oicr.pde.deciders.handlers;

import ca.on.oicr.pde.deciders.data.Bcl2FastqData;
import ca.on.oicr.pde.deciders.data.WorkflowRunV2;

/**
 *
 * @author mlaszloffy
 */
public class Bcl2Fastq_2_7_1_Handler extends Bcl2FastqHandler {

    @Override
    public boolean isHandlerFor(String workflowName, String workflowVersion) {
        return "CASAVA".equals(workflowName) && "2.7.1".equals(workflowVersion);
    }

    @Override
    public void validate(Bcl2FastqData data, WorkflowRunV2 workflowRun) {
        if (!data.getDoLaneSplitting()) {
            workflowRun.addError(Bcl2Fastq_2_7_1_Handler.class.getSimpleName() + " does not support no lane splitting");
        }
        if (!data.getProvisionOutUndetermined()) {
            workflowRun.addError(Bcl2Fastq_2_7_1_Handler.class.getSimpleName() + " does not support disabling provision-out-undetermined");
        }
    }

    @Override
    public void modifyWorkflowRun(Bcl2FastqData data, WorkflowRunV2 workflowRun) {
        //Bcl2FastqHandler produces a valid CASAVA 2.7.1 workflow run - no additional modification is required
    }

}
