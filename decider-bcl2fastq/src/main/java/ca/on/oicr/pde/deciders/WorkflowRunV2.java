package ca.on.oicr.pde.deciders;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author mlaszloffy
 */
public class WorkflowRunV2 extends WorkflowRun {

    private final List<String> errors = new ArrayList<>();

    public WorkflowRunV2(Map<String, String> iniFile, FileAttributes[] files) {
        super(iniFile, files);
    }

    public List<String> getErrors() {
        return errors;
    }

    public void addError(String error) {
        errors.add(error);
    }
}
