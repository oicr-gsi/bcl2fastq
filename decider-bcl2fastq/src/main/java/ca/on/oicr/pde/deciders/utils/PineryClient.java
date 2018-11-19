package ca.on.oicr.pde.deciders.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.Optional;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

/**
 *
 * @author mlaszloffy
 */
public class PineryClient {

    private final CloseableHttpClient httpClient;
    private final String pineryUrl;
    private final ObjectMapper mapper;

    public PineryClient(String pineryUrl) {
        this.httpClient = HttpClients.createDefault();
        this.pineryUrl = pineryUrl;
        this.mapper = new ObjectMapper();
    }

    public Optional<ObjectNode> fetch(String path) throws IOException {
        String requestUrl = String.format("%s/%s", pineryUrl, path);
        HttpGet request = new HttpGet(requestUrl);
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            if (response.getStatusLine().getStatusCode() == 200) {
                return Optional.of(mapper.readValue(response.getEntity().getContent(), ObjectNode.class));
            } else {
                return Optional.empty();
            }
        }
    }

    public Optional<String> getRunStatus(String runName) throws IOException {
        return fetch(String.format("sequencerrun?name=%s", runName)).map(run -> run.get("state")).map(runState -> runState.asText());
    }

}
