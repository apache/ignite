/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.builtins.config;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import javax.inject.Inject;
import javax.inject.Singleton;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import org.apache.ignite.cli.IgniteCLIException;
import org.jetbrains.annotations.Nullable;
import picocli.CommandLine.Help.ColorScheme;

/**
 * Client to get/put HOCON based configuration from/to Ignite server nodes
 */
@Singleton
public class ConfigurationClient {
    /** Url for getting configuration from REST endpoint of the node. */
    private static final String GET_URL = "/management/v1/configuration/";

    /** Url for setting configuration with REST endpoint of the node. */
    private static final String SET_URL = "/management/v1/configuration/";

    /** Http client. */
    private final HttpClient httpClient;

    /** Mapper serialize/deserialize json values during communication with node REST endpoint. */
    private final ObjectMapper mapper;

    /**
     * Creates new configuration client.
     *
     * @param httpClient Http client.
     */
    @Inject
    public ConfigurationClient(HttpClient httpClient) {
        this.httpClient = httpClient;
        mapper = new ObjectMapper();
    }

    /**
     * Gets server node configuration as a raw JSON string.
     *
     * @param host String representation of server node host.
     * @param port Host REST port.
     * @param rawHoconPath HOCON dot-delimited path of requested configuration.
     * @return JSON string with node configuration.
     */
    public String get(
        String host,
        int port,
        @Nullable String rawHoconPath) {
        var req = HttpRequest
            .newBuilder()
            .header("Content-Type", "application/json");

        if (rawHoconPath == null)
            req.uri(URI.create("http://" + host + ":" + port + GET_URL));
        else
            req.uri(URI.create("http://" + host + ":" + port + GET_URL +
                rawHoconPath));

        try {
            HttpResponse<String> res =
                httpClient.send(req.build(),
                    HttpResponse.BodyHandlers.ofString());

            if (res.statusCode() == HttpURLConnection.HTTP_OK)
                return mapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(mapper.readValue(res.body(), JsonNode.class));
            else
                throw error("Can't get configuration", res);
        }
        catch (IOException | InterruptedException e) {
            throw new IgniteCLIException("Connection issues while trying to send http request");
        }
    }

    /**
     * Sets node configuration from JSON string with configs.
     *
     * @param host String representation of server node host.
     * @param port Host REST port.
     * @param rawHoconData Valid HOCON represented as a string.
     * @param out PrintWriter for printing user messages.
     * @param cs ColorScheme to enrich user messages.
     */
    public void set(String host, int port, String rawHoconData, PrintWriter out, ColorScheme cs) {
        var req = HttpRequest
            .newBuilder()
            .POST(HttpRequest.BodyPublishers.ofString(renderJsonFromHocon(rawHoconData)))
            .header("Content-Type", "application/json")
            .uri(URI.create("http://" + host + ":" + port + SET_URL))
            .build();

        try {
            HttpResponse<String> res = httpClient.send(req, HttpResponse.BodyHandlers.ofString());

            if (res.statusCode() == HttpURLConnection.HTTP_OK) {
                out.println("Configuration was updated successfully.");
                out.println();
                out.println("Use the " + cs.commandText("ignite config get") +
                    " command to view the updated configuration.");
            }
            else
                throw error("Failed to set configuration", res);
        }
        catch (IOException | InterruptedException e) {
            throw new IgniteCLIException("Connection issues while trying to send http request");
        }
    }

    /**
     * Prepares exception with message, enriched by HTTP response details.
     *
     * @param msg Base error message.
     * @param res Http response, which cause the raising exce[tion.
     * @return Exception with detailed message.
     * @throws JsonProcessingException if response has incorrect error format.
     */
    private IgniteCLIException error(String msg, HttpResponse<String> res) throws JsonProcessingException {
        var errorMsg = mapper.writerWithDefaultPrettyPrinter()
            .writeValueAsString(mapper.readValue(res.body(), JsonNode.class));

        return new IgniteCLIException(msg + "\n\n" + errorMsg);
    }

    /**
     * Produces JSON representation of any valid HOCON string.
     *
     * @param rawHoconData HOCON string.
     * @return JSON representation of HOCON string.
     */
    private static String renderJsonFromHocon(String rawHoconData) {
        return ConfigFactory.parseString(rawHoconData)
            .root().render(ConfigRenderOptions.concise());
    }
}
