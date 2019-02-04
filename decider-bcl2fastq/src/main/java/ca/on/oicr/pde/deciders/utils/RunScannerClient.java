package ca.on.oicr.pde.deciders.utils;

import java.io.IOException;
import java.util.Optional;

/**
 *
 * @author mlaszloffy
 */
public class RunScannerClient extends JsonClient {

    public RunScannerClient(String runscannerUrl) {
        super(runscannerUrl);
    }

    public Optional<String> getRunWorkflowType(String runName) throws IOException {
        return fetch(String.format("run/%s", runName)).map(run -> run.get("workflowType")).map(workflowType -> workflowType.textValue());
    }

}
