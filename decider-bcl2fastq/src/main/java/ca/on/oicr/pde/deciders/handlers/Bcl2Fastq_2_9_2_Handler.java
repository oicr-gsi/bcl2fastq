package ca.on.oicr.pde.deciders.handlers;

import ca.on.oicr.pde.deciders.data.Bcl2FastqData;
import ca.on.oicr.pde.deciders.data.WorkflowRunV2;
import java.util.Arrays;

/**
 *
 * @author mlaszloffy
 */
public class Bcl2Fastq_2_9_2_Handler extends Bcl2Fastq_2_9_1_Handler {

    @Override
    public boolean isHandlerFor(String workflowName, String workflowVersion) {
        return "CASAVA".equals(workflowName) && Arrays.asList("2.9.2").contains(workflowVersion);
    }

    @Override
    public void validate(Bcl2FastqData data, WorkflowRunV2 workflowRun) {
        // override Bcl2Fastq_2_9_1_Handler validate
    }

    @Override
    public void modifyWorkflowRun(Bcl2FastqData data, WorkflowRunV2 workflowRun) {
        super.modifyWorkflowRun(data, workflowRun);

        if (!data.getProvisionOutUndetermined()) {
            workflowRun.addProperty("provision_out_undetermined", "false");
        }
    }

}
