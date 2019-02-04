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
public class JsonClient {

    private final CloseableHttpClient httpClient;
    private final String baseUrl;
    private final ObjectMapper mapper;

    public JsonClient(String baseUrl) {
        this.httpClient = HttpClients.createDefault();
        this.baseUrl = baseUrl;
        this.mapper = new ObjectMapper();
    }

    public Optional<ObjectNode> fetch(String path) throws IOException {
        String requestUrl = String.format("%s/%s", baseUrl, path);
        HttpGet request = new HttpGet(requestUrl);
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            if (response.getStatusLine().getStatusCode() == 200) {
                return Optional.of(mapper.readValue(response.getEntity().getContent(), ObjectNode.class));
            } else {
                return Optional.empty();
            }
        }
    }
}
