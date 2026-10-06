package org.apache.ignite.spi.discovery.tcp.ipfinder;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collection;
import java.util.UUID;

public class DiscoveryInfoService {

    private static final Logger log = LoggerFactory.getLogger(DiscoveryInfoService.class);

    private static String NODE_JOINED = "node-joined";

    public static String NODE_LEFT = "node-left";

    public static String NODE_FAILED = "node-failed";

    private String masterUrl = "http://127.0.0.1:3000";
    private String accountToken = null;

    private HttpClient httpClient = null;


    public DiscoveryInfoService(String masterUrl, String accountToken){
        this(masterUrl,accountToken,60000);
    }

    public DiscoveryInfoService(String masterUrl, String accountToken, int responseWaitTime) {
        this.masterUrl = masterUrl;
        this.accountToken = accountToken;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(responseWaitTime))
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }

    public JsonArray getClusterProperties(String cluster) throws IOException, InterruptedException {
        String url = this.masterUrl+ "/api/v1/disco/"+cluster+"/"+NODE_JOINED;

        HttpRequest request = HttpRequest.newBuilder().version(HttpClient.Version.HTTP_1_1)
                .uri(URI.create(url))
                .header("Authorization", "token " + accountToken)
                .GET()
                .build();
        try {
            HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            JsonArray list = new JsonArray();
            for(String nodeInfo: resp.body().split("\n")) {
                try {
                    if (nodeInfo.isBlank())
                        continue;
                    JsonObject st = new JsonObject(nodeInfo);

                    if (st.isEmpty())
                        continue;

                    JsonArray addrsList = st.getJsonArray("discoveryAddress");
                    if(addrsList==null)
                        continue;

                    list.add(st);

                }
                catch (IllegalArgumentException e) {
                    log.error("Failed to parse node info entry: " + nodeInfo, e);
                }
            }
            return list;
        } catch (IOException e) {
            log.error("Failed to get addresses entry: " + url, e);
            throw e;
        }
    }

    public void putClusterProperties(String cluster, UUID nodeId, JsonObject st) {

        String url = this.masterUrl+ "/api/v1/disco/"+cluster+"/"+nodeId+"/"+NODE_JOINED;

        HttpRequest request = HttpRequest.newBuilder().version(HttpClient.Version.HTTP_1_1)
                .uri(URI.create(url))
                .header("Authorization", "token " + accountToken)
                .PUT(HttpRequest.BodyPublishers.ofString(st.toString()))
                .build();
        httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding());
    }

    public void unregisterAddresses(String cluster,JsonArray addresses) {

        String url = this.masterUrl + "/api/v1/disco/" + cluster + "/" + NODE_JOINED + "/to/" + NODE_LEFT;

        HttpRequest request = HttpRequest.newBuilder().version(HttpClient.Version.HTTP_1_1)
                .uri(URI.create(url))
                .header("Authorization", "token " + accountToken)
                .PUT(HttpRequest.BodyPublishers.ofString(addresses.toString()))
                .build();
        httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding());
    }

    public void clearAllAddresses(String cluster) {
        String url = this.masterUrl + "/api/v1/disco/" + cluster + "/" + NODE_JOINED + "/clear";
        try {
            // 清除历史注册数据
            HttpRequest request = HttpRequest.newBuilder().version(HttpClient.Version.HTTP_1_1)
                    .uri(URI.create(url))
                    .header("Authorization", "token " + accountToken)
                    .DELETE()
                    .build();
            httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding());
        } catch (Exception e) {
            log.error("Failed to get addresses entry: " + url, e);
            throw e;
        }
    }
}
