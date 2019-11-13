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

package org.apache.ignite.agent.ws;

import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.security.KeyStore;
import java.util.List;
import java.util.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.management.ManagementConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpProxy;
import org.eclipse.jetty.client.Origin;
import org.eclipse.jetty.client.ProxyConfiguration;
import org.eclipse.jetty.client.Socks4Proxy;
import org.eclipse.jetty.client.util.BasicAuthentication;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandler;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.client.jetty.JettyWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

import static java.net.Proxy.NO_PROXY;
import static java.net.Proxy.Type.SOCKS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.agent.utils.AgentObjectMapperFactory.binaryMapper;
import static org.apache.ignite.agent.utils.AgentUtils.EMPTY;
import static org.eclipse.jetty.client.api.Authentication.ANY_REALM;

/**
 * Web socket manager.
 */
public class WebSocketManager extends GridProcessorAdapter {
    /** Mapper. */
    private final ObjectMapper mapper = binaryMapper();

    /** Ws max buffer size. */
    private static final int WS_MAX_BUFFER_SIZE =  10 * 1024 * 1024;

    /** Agent version header. */
    private static final String AGENT_VERSION_HDR = "Agent-Version";

    /** Cluster id header. */
    private static final String CLUSTER_ID_HDR = "Cluster-Id";

    /** Current version. */
    private static final String CURR_VER = "1.0.0";

    /** Max sleep time seconds between reconnects. */
    private static final int MAX_SLEEP_TIME_SECONDS = 10;

    /** Client. */
    private WebSocketStompClient client;

    /** Session. */
    private StompSession ses;

    /** Reconnect count. */
    private int reconnectCnt;

    /**
     * @param ctx Kernal context.
     */
    public WebSocketManager(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param cfg Url.
     * @param sesHnd Session handler.
     */
    public void connect(URI uri, ManagementConfiguration cfg, StompSessionHandler sesHnd) throws Exception {
        if (reconnectCnt == -1)
            log.info("Connecting to server: " + uri);

        if (reconnectCnt < MAX_SLEEP_TIME_SECONDS)
            reconnectCnt++;

        Thread.sleep(reconnectCnt * 1000);

        client = new WebSocketStompClient(createWebSocketClient(uri, cfg));

        client.setMessageConverter(getMessageConverter());

        client.start();

        ses = client.connect(uri, handshakeHeaders(), connectHeaders(), sesHnd).get(10L, SECONDS);

        reconnectCnt = -1;
    }

    /**
     * TODO GG-24630: Remove synchronized and make the send method non-blocking.
     *
     * @param dest Destination.
     * @param payload Payload.
     */
    public synchronized boolean send(String dest, byte[] payload) {
        boolean connected = ses != null && ses.isConnected();

        // TODO: workaround of spring-messaging bug with send byte array data.
        // https://github.com/spring-projects/spring-framework/issues/23358
        StompHeaders headers = new StompHeaders();

        headers.setContentType(MimeTypeUtils.APPLICATION_OCTET_STREAM);
        headers.setDestination(dest);

        if (connected)
            ses.send(headers, payload);

        return connected;
    }

    /**
     * TODO GG-24630: Remove synchronized and make the send method non-blocking.
     *
     * @param dest Destination.
     * @param payload Payload.
     */
    public synchronized boolean send(String dest, Object payload) {
        boolean connected = connected();

        if (connected)
            ses.send(dest, payload);

        return connected;
    }

    /**
     * @return {@code True} if agent connected to backend.
     */
    public boolean connected() {
        return ses != null && ses.isConnected();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        if (client != null)
            client.stop();
    }

    /**
     * @return Composite message converter.
     */
    private CompositeMessageConverter getMessageConverter() {
        MappingJackson2MessageConverter mapper =
            new MappingJackson2MessageConverter(MimeTypeUtils.APPLICATION_OCTET_STREAM);

        mapper.setObjectMapper(this.mapper);

        return new CompositeMessageConverter(
            U.sealList(new StringMessageConverter(), mapper)
        );
    }

    /**
     * @return Handshake headers.
     */
    private WebSocketHttpHeaders handshakeHeaders() {
        UUID clusterId = ctx.cluster().get().id();

        WebSocketHttpHeaders handshakeHeaders = new WebSocketHttpHeaders();

        handshakeHeaders.add(AGENT_VERSION_HDR, CURR_VER);
        handshakeHeaders.add(CLUSTER_ID_HDR, clusterId.toString());

        return handshakeHeaders;
    }

    /**
     * @return Connection headers.
     */
    private StompHeaders connectHeaders() {
        UUID clusterId = ctx.cluster().get().id();

        StompHeaders connectHeaders = new StompHeaders();

        connectHeaders.add(CLUSTER_ID_HDR, clusterId.toString());

        return connectHeaders;
    }

    /**
     * @param uri Uri.
     * @param cfg Config.
     * @return Jetty websocket client.
     */
    private JettyWebSocketClient createWebSocketClient(URI uri, ManagementConfiguration cfg) throws Exception {
        HttpClient httpClient = new HttpClient(createServerSslFactory(log, cfg));
        
        // TODO GG-18379 Investigate how to establish native websocket connection with proxy.
        configureProxy(log, httpClient, uri);

        httpClient.setName("mgmt-console-http-client");
        httpClient.addBean(httpClient.getExecutor());

        WebSocketClient webSockClient = new WebSocketClient(httpClient);

        webSockClient.setMaxTextMessageBufferSize(WS_MAX_BUFFER_SIZE);
        webSockClient.setMaxBinaryMessageBufferSize(WS_MAX_BUFFER_SIZE);

        webSockClient.addBean(httpClient);

        return new JettyWebSocketClient(webSockClient) {
            @Override public void stop() {
                try {
                    webSockClient.stop();
                }
                catch (Exception ex) {
                    throw new IllegalStateException("Failed to stop Jetty WebSocketClient", ex);
                }
            }
        };
    }

    /**
     * @param log Logger.
     * @param cfg Config.
     */
    @SuppressWarnings("deprecation")
    private SslContextFactory createServerSslFactory(IgniteLogger log, ManagementConfiguration cfg) {
        boolean trustAll = Boolean.getBoolean("trust.all");

        if (trustAll && !F.isEmpty(cfg.getConsoleTrustStore())) {
            log.warning("Management configuration contains 'server-trust-store' property and node has system" +
                    " property '-Dtrust.all=true'. Option '-Dtrust.all=true' will be ignored.");

            trustAll = false;
        }

        boolean ssl = trustAll || !F.isEmpty(cfg.getConsoleTrustStore()) || !F.isEmpty(cfg.getConsoleKeyStore());

        if (!ssl)
            return new SslContextFactory();

        return sslContextFactory(
                cfg.getConsoleKeyStore(),
                cfg.getConsoleKeyStorePassword(),
                trustAll,
                cfg.getConsoleTrustStore(),
                cfg.getConsoleTrustStorePassword(),
                cfg.getCipherSuites()
        );
    }

    /**
     * @param content Key store content.
     * @param pwd Password.
     */
    private KeyStore keyStore(String content, String pwd) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("JKS");

        keyStore.load(new ByteArrayInputStream(content.getBytes(UTF_8)), pwd != null ? pwd.toCharArray() : null);

        return keyStore;
    }

    /**
     *
     * @param keyStore Path to key store.
     * @param keyStorePwd Optional key store password.
     * @param trustAll Whether we should trust for self-signed certificate.
     * @param trustStore Path to trust store.
     * @param trustStorePwd Optional trust store password.
     * @param ciphers Optional list of enabled cipher suites.
     * @return SSL context factory.
     */
    @SuppressWarnings("deprecation")
    private SslContextFactory sslContextFactory(
            String keyStore,
            String keyStorePwd,
            boolean trustAll,
            String trustStore,
            String trustStorePwd,
            List<String> ciphers
    ) {
        SslContextFactory sslCtxFactory = new SslContextFactory();

        if (!F.isEmpty(keyStore)) {
            try {
                sslCtxFactory.setKeyStore(keyStore(keyStore, keyStorePwd));
            }
            catch (Exception e) {
                log.warning("Failed to load server keyStore", e);
            }
        }

        if (trustAll) {
            // TODO GG-25519: sslCtxFactory.setHostnameVerifier((hostname, session) -> true); available in Jetty >= 9.4.15.x
            sslCtxFactory.setTrustAll(true);
        }
        else if (!F.isEmpty(trustStore)) {
            try {
                sslCtxFactory.setKeyStore(keyStore(trustStore, trustStorePwd));
            }
            catch (Exception e) {
                log.warning("Failed to load server keyStore", e);
            }
        }

        if (!F.isEmpty(ciphers))
            sslCtxFactory.setIncludeCipherSuites(ciphers.toArray(EMPTY));

        return sslCtxFactory;
    }

    /**
     * @param srvUri Server uri.
     */
    private void configureProxy(IgniteLogger log, HttpClient httpClient, URI srvUri) {
        try {
            boolean secure = "https".equalsIgnoreCase(srvUri.getScheme());

            List<ProxyConfiguration.Proxy> proxies = ProxySelector.getDefault().select(srvUri).stream()
                .filter(p -> !p.equals(NO_PROXY))
                .map(p -> {
                    InetSocketAddress inetAddr = (InetSocketAddress)p.address();

                    Origin.Address addr = new Origin.Address(inetAddr.getHostName(), inetAddr.getPort());

                    if (p.type() == SOCKS)
                        return new Socks4Proxy(addr, secure);

                    return new HttpProxy(addr, secure);
                })
                .collect(toList());

            httpClient.getProxyConfiguration().getProxies().addAll(proxies);

            addAuthentication(httpClient, proxies);
        }
        catch (Exception e) {
            log.warning("Failed to configure proxy.", e);
        }
    }

    /**
     * @param httpClient Http client.
     * @param proxies Proxies.
     */
    private void addAuthentication(HttpClient httpClient, List<ProxyConfiguration.Proxy> proxies) {
        proxies.forEach(p -> {
            String user, pwd;

            if (p instanceof HttpProxy) {
                String scheme = p.getURI().getScheme();

                user = System.getProperty(scheme + ".proxyUsername");

                pwd = System.getProperty(scheme + ".proxyPassword");
            }
            else {
                user = System.getProperty("java.net.socks.username");

                pwd = System.getProperty("java.net.socks.password");
            }

            httpClient.getAuthenticationStore().addAuthentication(
                new BasicAuthentication(p.getURI(), ANY_REALM, user, pwd)
            );
        });
    }
}
