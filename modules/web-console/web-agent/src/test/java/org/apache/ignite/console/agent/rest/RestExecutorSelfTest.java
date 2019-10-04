/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.agent.rest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.console.demo.service.DemoCachesLoadService;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.utils.Utils;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.compute.VisorGatewayTask;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.eclipse.jetty.client.HttpResponseException;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockserver.integration.ClientAndServer;

import static org.apache.ignite.console.agent.AgentUtils.sslContextFactory;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

/**
 * Test for RestExecutor.
 */
public class RestExecutorSelfTest {
    static {
        System.getProperties().setProperty("IGNITE_JETTY_KEY_STORE_PATH", resolvePath("server.jks"));
        System.getProperties().setProperty("IGNITE_JETTY_TRUST_STORE_PATH", resolvePath("ca.jks"));
    }

    /** Name of the cache created by default in the cluster. */
    private static final String DEFAULT_CACHE_NAME = "default";

    /** JSON object mapper. */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** */
    private static final String HTTP_URI = "http://localhost:8080";

    /** */
    private static final String HTTPS_URI = "https://localhost:8080";

    /** */
    private static final String JETTY_WITH_SSL = "jetty-with-ssl.xml";

    /** */
    private static final String JETTY_WITH_HTTPS = "jetty-with-https.xml";

    /** */
    private static final String JETTY_WITH_CIPHERS_0 = "jetty-with-ciphers-0.xml";

    /** */
    private static final String JETTY_WITH_CIPHERS_1 = "jetty-with-ciphers-1.xml";

    /** */
    private static final String JETTY_WITH_CIPHERS_2 = "jetty-with-ciphers-2.xml";

    /** This cipher is disabled by default in JDK 8. */
    private static final List<String> CIPHER_0 = Collections.singletonList("TLS_DH_anon_WITH_AES_256_GCM_SHA384");

    /** */
    private static final List<String> CIPHER_1 = Collections.singletonList("TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384");

    /** */
    private static final List<String> CIPHER_2 = Collections.singletonList("TLS_EMPTY_RENEGOTIATION_INFO_SCSV");

    /** */
    private static final List<String> COMMON_CIPHERS = Arrays.asList(
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
        "TLS_EMPTY_RENEGOTIATION_INFO_SCSV"
    );

    /** */
    @SuppressWarnings("PublicField")
    @Rule
    public final ExpectedException ruleForExpEx = ExpectedException.none();

    /**
     * @param jettyCfg Optional path to file with Jetty XML config.
     * @return Prepare configuration for cluster node.
     */
    private IgniteConfiguration nodeConfiguration(String jettyCfg) {
        TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.registerAddresses(Collections.singletonList(new InetSocketAddress("127.0.0.1", 47500)));

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();

        discoverySpi.setIpFinder(ipFinder);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setDiscoverySpi(discoverySpi);

        CacheConfiguration<Integer, String> dfltCacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cfg.setCacheConfiguration(dfltCacheCfg);

        cfg.setIgniteInstanceName(UUID.randomUUID().toString());

        if (!F.isEmpty(jettyCfg)) {
            ConnectorConfiguration conCfg = new ConnectorConfiguration();
            conCfg.setJettyPath(resolvePath(jettyCfg));

            cfg.setConnectorConfiguration(conCfg);
        }

        return cfg;
    }

    /**
     * Convert response to JSON.
     *
     * @param res REST result.
     * @return JSON object.
     * @throws IOException If failed to parse.
     */
    private JsonNode toJson(RestResult res) throws IOException {
        assertNotNull(res);

        String data = res.getData();

        assertNotNull(data);
        assertFalse(data.isEmpty());

        return MAPPER.readTree(data);
    }

    /**
     * @param file File name.
     * @return Path to file.
     */
    private static String resolvePath(String file) {
        return RestExecutorSelfTest.class.getClassLoader().getResource(file).getPath();
    }

    /**
     * Try to execute REST command and check response.
     *
     * @param nodeCfg Node configuration.
     * @param uri Node URI.
     * @param keyStore Key store.
     * @param keyStorePwd Key store password.
     * @param trustAll Whether to trust all certificates.
     * @param trustStore Trust store.
     * @param trustStorePwd Trust store password.
     * @param cipherSuites Cipher suites.
     * @throws Exception If failed.
     */
    private void checkRest(
        IgniteConfiguration nodeCfg,
        String uri,
        String keyStore,
        String keyStorePwd,
        boolean trustAll,
        String trustStore,
        String trustStorePwd,
        List<String> cipherSuites
    ) throws Throwable {
        try(Ignite ignored = Ignition.getOrStart(nodeCfg)) {
            RestExecutor exec = new RestExecutor(sslContextFactory(
                keyStore,
                keyStorePwd,
                trustAll,
                trustStore,
                trustStorePwd,
                cipherSuites
            ));

            JsonObject params = new JsonObject()
                .add("cmd", "top")
                .add("attr", false)
                .add("mtr", false)
                .add("caches", false);

            RestResult res = exec.sendRequest(uri, params);

            JsonNode json = toJson(res);

            assertTrue(json.isArray());

            for (JsonNode item : json) {
                assertTrue(item.get("attributes").isNull());
                assertTrue(item.get("metrics").isNull());
                assertTrue(item.get("caches").isNull());
            }
        }
    }

    /** */
    @Test
    public void nodeNoSslAgentNoSsl() throws Throwable {
        checkRest(
            nodeConfiguration(""),
            HTTP_URI,
            null, null,
            false,
            null, null,
            null
        );
    }

    /** */
    @Test
    public void nodeNoSslAgentWithSsl() throws Throwable {
        ruleForExpEx.expect(Is.isA(SSLException.class));
        checkRest(
            nodeConfiguration(""),
            HTTPS_URI,
            resolvePath("client.jks"), "123456",
            false,
            resolvePath("ca.jks"), "123456",
            null
        );
    }

    /** */
    @Test
    public void nodeWithSslAgentNoSsl() throws Throwable {
        ruleForExpEx.expect(Is.isA(HttpResponseException.class));
        checkRest(
            nodeConfiguration(JETTY_WITH_SSL),
            HTTP_URI,
            null, null,
            false,
            null, null,
            null
        );
    }

    /** */
    @Test
    public void nodeWithSslAgentWithSsl() throws Throwable {
        checkRest(
            nodeConfiguration(JETTY_WITH_SSL),
            HTTPS_URI,
            resolvePath("client.jks"), "123456",
            false,
            resolvePath("ca.jks"), "123456",
            null
        );
    }

    /** */
    @Test
    public void nodeNoCiphersAgentWithCiphers() throws Throwable {
        ruleForExpEx.expect(Is.isA(SSLHandshakeException.class));
        checkRest(
            nodeConfiguration(JETTY_WITH_SSL),
            HTTPS_URI,
            resolvePath("client.jks"), "123456",
            false,
            resolvePath("ca.jks"), "123456",
            CIPHER_0
        );
   }

    /** */
    @Test
    public void nodeWithCiphersAgentNoCiphers() throws Throwable {
        ruleForExpEx.expect(Is.isA(SSLException.class));
        checkRest(
            nodeConfiguration(JETTY_WITH_CIPHERS_0),
            HTTPS_URI,
            resolvePath("client.jks"), "123456",
            false,
            resolvePath("ca.jks"), "123456",
            null
        );
   }

    /** */
    @Test
    public void nodeWithCiphersAgentWithCiphers() throws Throwable {
        checkRest(
            nodeConfiguration(JETTY_WITH_CIPHERS_1),
            HTTPS_URI,
            resolvePath("client.jks"), "123456",
            false,
            resolvePath("ca.jks"), "123456",
            CIPHER_1
        );
   }

    /** */
    @Test
    public void differentCiphers1() throws Throwable {
        ruleForExpEx.expect(Is.isA(SSLException.class));
        checkRest(
            nodeConfiguration(JETTY_WITH_CIPHERS_1),
            HTTPS_URI,
            resolvePath("client.jks"), "123456",
            false,
            resolvePath("ca.jks"), "123456",
            CIPHER_2
        );
   }

    /** */
    @Test
    public void differentCiphers2() throws Throwable {
        ruleForExpEx.expect(Is.isA(SSLException.class));
        checkRest(
            nodeConfiguration(JETTY_WITH_CIPHERS_2),
            HTTPS_URI,
            resolvePath("client.jks"), "123456",
            false,
            resolvePath("ca.jks"), "123456",
            CIPHER_1
        );
   }

    /** */
    @Test
    public void commonCiphers() throws Throwable {
        checkRest(
            nodeConfiguration(JETTY_WITH_CIPHERS_1),
            HTTPS_URI,
            resolvePath("client.jks"), "123456",
            false,
            resolvePath("ca.jks"), "123456",
            COMMON_CIPHERS
        );
   }

    /** */
    @Test
    public void testHttps() throws Throwable {
        checkRest(
            nodeConfiguration(JETTY_WITH_HTTPS),
            HTTPS_URI,
            null,
            null,
            true,
            null,
            null,
            null
        );
    }

    /** */
    @Test
    public void testRequestWithManyParams() throws Throwable {
        try(Ignite ignored = Ignition.getOrStart(nodeConfiguration(""))) {
            RestExecutor exec = new RestExecutor(new SslContextFactory.Client());

            JsonObject params = new JsonObject()
                .add("cmd", "top")
                .add("attr", false)
                .add("mtr", false)
                .add("caches", false);

            for (int i = 0; i < 1000; i++) {
                String param = UUID.randomUUID().toString();

                params.add(param, param);
            }

            RestResult res = exec.sendRequest(HTTP_URI, params);

            JsonNode json = toJson(res);

            assertTrue(json.isArray());

            for (JsonNode item : json) {
                assertTrue(item.get("attributes").isNull());
                assertTrue(item.get("metrics").isNull());
                assertTrue(item.get("caches").isNull());
            }
        }
    }

    /** */
    @Test
    public void testLargeResponse() throws Throwable {
        ClientAndServer mockSrv = null;

        try {
            mockSrv = startClientAndServer(8080);

            char[] chars = new char[3 * 1024 * 1024];

            Arrays.fill(chars, 'a');

            String err = new String(chars);

            mockSrv
                .when(request().withPath("/ignite"))
                .respond(response().withStatusCode(200).withBody(Utils.toJson(RestResult.fail(1, err))));

            RestExecutor exec = new RestExecutor(new SslContextFactory.Client());

            RestResult res = exec.sendRequest(HTTP_URI, new JsonObject());

            Assert.assertEquals(err, res.getError());
        }
        finally {
            if (mockSrv != null)
                mockSrv.stop();
        }
    }

    /** */
    @Test
    public void testLongRunningQueries() throws Throwable {
        try(Ignite ignite = Ignition.getOrStart(nodeConfiguration(""))) {
            DemoCachesLoadService load = new DemoCachesLoadService(0);

            GridTestUtils.setFieldValue(load, "ignite", ignite);

            load.init(null);

            RestExecutor exec = new RestExecutor(new SslContextFactory.Client());

            ExecutorService executor = Executors.newFixedThreadPool(2);

            Future<Boolean> fut1 = executor.submit(() -> executeQuery(exec, "select *, sleep(30) from \"CarCache\".Car"));
            Future<Boolean> fut2 = executor.submit(() -> executeQuery(exec, "select * from \"CarCache\".Car"));

            assertFalse(fut1.isDone());
            assertFalse(fut2.isDone());
            assertFalse(fut2.get(1L, TimeUnit.SECONDS));

            assertFalse(fut1.isDone());
            assertTrue(fut2.isDone());
            assertFalse(fut1.get(4L, TimeUnit.SECONDS));
        }
    }

    /**
     * @param exec Rest executor.
     * @param qry Query text.
     */
    private boolean executeQuery(RestExecutor exec, String qry) throws Exception {
        try {
            JsonObject params = new JsonObject()
                .add("cmd", GridRestCommand.EXE.key())
                .add("name", VisorGatewayTask.class.getName())
                .add("p1", null)
                .add("p2", "org.apache.ignite.internal.visor.query.VisorQueryTask")
                .add("p3", "org.apache.ignite.internal.visor.query.VisorQueryTaskArg")
                .add("p4", "CarCache")
                .add("p5", qry)
                .add("p6", false)
                .add("p7", false)
                .add("p8", false)
                .add("p9", false)
                .add("p10", 1);

            RestResult res = exec.sendRequest(HTTP_URI, params);

            JsonNode json = toJson(res);

            JsonNode taskRes = json.get("result").get("result");

            String qryId = taskRes.get("queryId").asText();
            String resNodeId = taskRes.get("responseNodeId").asText();

            params = new JsonObject()
                .add("cmd", GridRestCommand.EXE.key())
                .add("name", VisorGatewayTask.class.getName())
                .add("p1", resNodeId)
                .add("p2", "org.apache.ignite.internal.visor.query.VisorQueryFetchFirstPageTask")
                .add("p3", "org.apache.ignite.internal.visor.query.VisorQueryNextPageTaskArg")
                .add("p4", qryId)
                .add("p5", 100);

            res = exec.sendRequest(HTTP_URI, params);

            json = toJson(res);

            taskRes = json.get("result").get("result");

            return taskRes.get("hasMore").asBoolean();
        } catch (Throwable e) {
            throw U.cast(e);
        }
    }
}
