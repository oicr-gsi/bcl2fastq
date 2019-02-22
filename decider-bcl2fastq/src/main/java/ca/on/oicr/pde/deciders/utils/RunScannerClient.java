package ca.on.oicr.pde.deciders.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
        Optional<ObjectNode> runInfoNode = fetch(String.format("run/%s", runName));
        if (!runInfoNode.isPresent()) {
            throw new IOException("Unable to get run info for " + runName);
        }

        Optional<JsonNode> workflowTypeNode = runInfoNode.map(run -> run.get("workflowType"));
        if (!workflowTypeNode.isPresent()) {
            throw new IOException("workflowType is missing for run " + runName);
        }

        return workflowTypeNode.map(workflowType -> workflowType.textValue());
    }
}
