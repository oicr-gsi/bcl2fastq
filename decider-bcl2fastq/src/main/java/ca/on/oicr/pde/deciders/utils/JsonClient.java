package ca.on.oicr.pde.deciders.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/**
 *
 * @author mlaszloffy
 */
public class JsonClient {

    private final CloseableHttpClient httpClient;
    private final String baseUrl;
    private static final ObjectMapper MAPPER;

    static {
        MAPPER = new ObjectMapper();
        MAPPER.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    }

    public JsonClient(String baseUrl) {
        this.httpClient = HttpClients.createDefault();
        this.baseUrl = baseUrl;
    }

    public Optional<String> httpGet(String path) throws IOException {
        String requestUrl = String.format("%s/%s", baseUrl, path);
        HttpGet request = new HttpGet(requestUrl);
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            if (response.getStatusLine().getStatusCode() == 200) {
                return Optional.of(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
            } else {
                return Optional.empty();
            }
        }
    }

    public Optional<ObjectNode> fetch(String path) throws IOException {
        Optional<String> input = httpGet(path);
        if (input.isPresent()) {
            return Optional.of(MAPPER.readValue(input.get(), ObjectNode.class));
        } else {
            return Optional.empty();
        }
    }
}
