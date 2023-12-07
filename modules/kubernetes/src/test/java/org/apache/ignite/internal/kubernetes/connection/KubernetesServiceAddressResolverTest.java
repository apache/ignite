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

package org.apache.ignite.internal.kubernetes.connection;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.kubernetes.configuration.KubernetesConnectionConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;

import static org.junit.Assert.assertEquals;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

/** Checks that correctly parse kubernetes json response. */
public class KubernetesServiceAddressResolverTest {
    /** Mock of kubernetes API. */
    private static ClientAndServer mockServer;

    /** */
    private static final String namespace = "ns01";

    /** */
    private static final String service = "ignite";

    /** */
    @BeforeClass
    public static void startServer() {
        mockServer = startClientAndServer();
    }

    /** */
    @AfterClass
    public static void stopServer() {
        mockServer.stop();
    }

    /** */
    @Test
    public void testCorrectParseKubernetesResponse() throws IOException {
        // given
        KubernetesServiceAddressResolver rslvr = prepareResolver(false);

        mockSuccessServerResponse();

        // when
        Collection<InetAddress> result = rslvr.getServiceAddresses();

        // then
        List<String> ips = result.stream()
            .map(InetAddress::getHostAddress)
            .collect(Collectors.toList());

        assertEquals(
            Arrays.asList("10.1.1.1", "10.1.1.2", "10.1.1.4", "10.1.1.5", "10.1.1.7"),
            ips
        );
    }

    /** */
    @Test
    public void testCorrectParseKubernetesResponseWithIncludingNotReadyAddresses() throws IOException {
        // given
        KubernetesServiceAddressResolver rslvr = prepareResolver(true);

        mockSuccessServerResponse();

        // when
        Collection<InetAddress> result = rslvr.getServiceAddresses();

        // then
        List<String> ips = result.stream()
            .map(InetAddress::getHostAddress)
            .collect(Collectors.toList());

        assertEquals(
            Arrays.asList("10.1.1.1", "10.1.1.2", "10.1.1.3", "10.1.1.4", "10.1.1.5", "10.1.1.6", "10.1.1.7"),
            ips
        );
    }

    /** */
    @Test(expected = IgniteException.class)
    public void testConnectionFailure() throws IOException {
        // given
        KubernetesServiceAddressResolver rslvr = prepareResolver(true);

        mockFailureServerResponse();

        rslvr.getServiceAddresses();
    }

    /** */
    private KubernetesServiceAddressResolver prepareResolver(boolean includeNotReadyAddresses)
        throws IOException {
        File account = File.createTempFile("kubernetes-test-account", "");
        FileWriter fw = new FileWriter(account);
        fw.write("account-token");
        fw.close();

        String accountFile = account.getAbsolutePath();

        KubernetesConnectionConfiguration cfg = new KubernetesConnectionConfiguration();
        cfg.setNamespace(namespace);
        cfg.setServiceName(service);
        cfg.setMasterUrl("https://localhost:" + mockServer.getLocalPort());
        cfg.setAccountToken(accountFile);
        cfg.setIncludeNotReadyAddresses(includeNotReadyAddresses);

        return new KubernetesServiceAddressResolver(cfg);
    }

    /** */
    private void mockFailureServerResponse() {
        mockServer
            .when(
                request()
                    .withMethod("GET")
                    .withPath(String.format("/api/v1/namespaces/%s/endpoints/%s", namespace, service)),
                Times.exactly(1)
            )
            .respond(
                response()
                    .withStatusCode(401));
    }

    /** */
    private void mockSuccessServerResponse() {
        mockServer
            .when(
                request()
                    .withMethod("GET")
                    .withPath(String.format("/api/v1/namespaces/%s/endpoints/%s", namespace, service)),
                Times.exactly(1)
            )
            .respond(
                response()
                    .withStatusCode(200)
                    .withBody("{" +
                              "  \"subsets\": [" +
                              "     {" +
                              "        \"addresses\": [" +
                              "           {" +
                              "              \"ip\": \"10.1.1.1\"" +
                              "           }," +
                              "           {" +
                              "              \"ip\": \"10.1.1.2\"" +
                              "           }" +
                              "        ]," +
                              "        \"notReadyAddresses\": [" +
                              "           {" +
                              "              \"ip\": \"10.1.1.3\"" +
                              "           }" +
                              "        ]" +
                              "     }," +
                              "     {" +
                              "        \"addresses\": [" +
                              "           {" +
                              "              \"ip\": \"10.1.1.4\"" +
                              "           }," +
                              "           {" +
                              "              \"ip\": \"10.1.1.5\"" +
                              "           }" +
                              "        ]," +
                              "        \"notReadyAddresses\": [" +
                              "           {" +
                              "              \"ip\": \"10.1.1.6\"" +
                              "           }" +
                              "        ]" +
                              "     }," +
                              "     {" +
                              "        \"addresses\": [" +
                              "           {" +
                              "              \"ip\": \"10.1.1.7\"" +
                              "           }" +
                              "        ]" +
                              "     }" +
                              "  ]" +
                              "}"
                    ));
    }
}
