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

package org.apache.ignite.kubernetes.discovery;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.kubernetes.configuration.KubernetesConnectionConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.kubernetes.TcpDiscoveryKubernetesIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

/** Super class for Kubernetes discovery tests. */
public abstract class KubernetesDiscoveryAbstractTest extends GridCommonAbstractTest {
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
    @After
    public void tearDown() {
        stopAllGrids();
    }

    /** */
    protected IgniteConfiguration getConfiguration(String instanceName, Boolean clientMode) throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        KubernetesConnectionConfiguration kccfg = prepareConfiguration();
        TcpDiscoveryKubernetesIpFinder ipFinder = new TcpDiscoveryKubernetesIpFinder(kccfg);

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        discoverySpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoverySpi);

        cfg.setGridLogger(log);

        cfg.setIgniteInstanceName(instanceName);
        cfg.setClientMode(clientMode);

        return cfg;
    }

    /**
     * Mocks HTTP server response.
     *
     * @param addrs Address list to return.
     */
    protected final void mockServerResponse(String... addrs) {
        mockServerResponse(1, addrs);
    }

    /**
     * Mocks HTTP server response.
     *
     * @param times How many times mock should return value.
     * @param addrs Address list to return.
     */
    protected final void mockServerResponse(int times, String... addrs) {
        String ipAddrs = Arrays.stream(addrs)
            .map(addr -> String.format("{\"ip\":\"%s\"}", addr))
            .collect(Collectors.joining(","));

        mockServer
            .when(
                request()
                    .withMethod("GET")
                    .withPath(String.format("/api/v1/namespaces/%s/endpoints/%s", namespace, service)),
                Times.exactly(times)
            )
            .respond(
                response()
                    .withStatusCode(200)
                    .withBody("{" +
                        "  \"subsets\": [" +
                        "     {" +
                        "        \"addresses\": [" +
                        "        " + ipAddrs +
                        "        ]" +
                        "     }" +
                        "  ]" +
                        "}"
                    ));
    }

    /** */
    protected final KubernetesConnectionConfiguration prepareConfiguration()
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

        return cfg;
    }
}
