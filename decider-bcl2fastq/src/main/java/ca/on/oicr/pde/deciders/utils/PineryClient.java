package ca.on.oicr.pde.deciders.utils;

import java.io.IOException;
import java.util.Optional;

/**
 *
 * @author mlaszloffy
 */
public class PineryClient extends JsonClient {

    public PineryClient(String pineryUrl) {
        super(pineryUrl);
    }

    public Optional<String> getRunStatus(String runName) throws IOException {
        return fetch(String.format("sequencerrun?name=%s", runName)).map(run -> run.get("state")).map(runState -> runState.textValue());
    }

}
