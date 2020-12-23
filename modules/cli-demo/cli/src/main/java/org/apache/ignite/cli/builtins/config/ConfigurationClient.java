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

@Singleton
public class ConfigurationClient {

    private final String GET_URL = "/management/v1/configuration/";
    private final String SET_URL = "/management/v1/configuration/";

    private final HttpClient httpClient;
    private final ObjectMapper mapper;

    @Inject
    public ConfigurationClient(HttpClient httpClient) {
        this.httpClient = httpClient;
        mapper = new ObjectMapper();
    }

    public String get(String host, int port,
        @Nullable String rawHoconPath) {
        var request = HttpRequest
            .newBuilder()
            .header("Content-Type", "application/json");

        if (rawHoconPath == null)
            request.uri(URI.create("http://" + host + ":" + port + GET_URL));
        else
            request.uri(URI.create("http://" + host + ":" + port + GET_URL +
                rawHoconPath));

        try {
            HttpResponse<String> response =
                httpClient.send(request.build(),
                    HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == HttpURLConnection.HTTP_OK)
                return mapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(mapper.readValue(response.body(), JsonNode.class));
            else
                throw error("Can't get configuration", response);
        }
        catch (IOException | InterruptedException e) {
            throw new IgniteCLIException("Connection issues while trying to send http request");
        }
    }

    public void set(String host, int port, String rawHoconData, PrintWriter out, ColorScheme cs) {
        var request = HttpRequest
            .newBuilder()
            .POST(HttpRequest.BodyPublishers.ofString(renderJsonFromHocon(rawHoconData)))
            .header("Content-Type", "application/json")
            .uri(URI.create("http://" + host + ":" + port + SET_URL))
            .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == HttpURLConnection.HTTP_OK) {
                out.println("Configuration was updated successfully.");
                out.println();
                out.println("Use the " + cs.commandText("ignite config get") +
                    " command to view the updated configuration.");
            }
            else
                throw error("Fail to set configuration", response);
        }
        catch (IOException | InterruptedException e) {
            throw new IgniteCLIException("Connection issues while trying to send http request");
        }
    }

    private IgniteCLIException error(String message, HttpResponse<String> response) throws JsonProcessingException {
        var errorMessage = mapper.writerWithDefaultPrettyPrinter()
            .writeValueAsString(mapper.readValue(response.body(), JsonNode.class));
        return new IgniteCLIException(message + "\n\n" + errorMessage);
    }

    private static String renderJsonFromHocon(String rawHoconData) {
        return ConfigFactory.parseString(rawHoconData)
            .root().render(ConfigRenderOptions.concise());
    }


}
