package ca.on.oicr.pde.deciders.data;

import ca.on.oicr.pde.deciders.FileAttributes;
import ca.on.oicr.pde.deciders.WorkflowRun;
import ca.on.oicr.pde.deciders.data.Bcl2FastqData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author mlaszloffy
 */
public class WorkflowRunV2 extends WorkflowRun {

    private final List<String> errors = new ArrayList<>();
    private final Bcl2FastqData data;

    public WorkflowRunV2(Map<String, String> iniFile, FileAttributes[] files, Bcl2FastqData data) {
        super(iniFile, files);
        this.data = data;
    }

    public List<String> getErrors() {
        return errors;
    }

    public void addError(String error) {
        errors.add(error);
    }

    public Bcl2FastqData getBcl2FastqData() {
        return data;
    }

}
