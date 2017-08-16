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

package org.apache.ignite.spi.discovery.tcp.ipfinder.kubernetes;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * IP finder for automatic lookup of Ignite nodes running in Kubernetes environment. All Ignite nodes have to deployed
 * as Kubernetes pods in order to be discovered. An application that uses Ignite client nodes as a gateway to the
 * cluster is required to be containerized as well. Applications and Ignite nodes running outside of Kubernetes will
 * not be able to reach the containerized counterparts.
 * <p>
 * The implementation is based on a distinct Kubernetes service that has to be created and should be deployed prior
 * Ignite nodes startup. The service will maintain a list of all endpoints (internal IP addresses) of all containerized
 * Ignite pods running so far. The name of the service must be equal to {@link #setServiceName(String)} which is
 * `ignite` by default.
 * <p>
 * As for Ignite pods, it's recommended to label them in such a way that the service will use the label in its selector
 * configuration excluding endpoints of irrelevant Kubernetes pods running in parallel.
 * <p>
 * The IP finder, in its turn, will call this service to retrieve Ignite pods IP addresses. The port will be
 * either the one that is set with {@link TcpDiscoverySpi#setLocalPort(int)} or {@link TcpDiscoverySpi#DFLT_PORT}.
 * Make sure that all Ignite pods occupy a similar discovery port, otherwise they will not be able to discover each
 * other using this IP finder.
 * <h2 class="header">Optional configuration</h2>
 * <ul>
 *      <li>The Kubernetes service name for IP addresses lookup (see {@link #setServiceName(String)})</li>
 *      <li>The Kubernetes service namespace for IP addresses lookup (see {@link #setNamespace(String)}</li>
 *      <li>The host name of the Kubernetes API server (see {@link #setMasterUrl(String)})</li>
 *      <li>Path to the service token (see {@link #setAccountToken(String)}</li>
 * </ul>
 * <p>
 * Both {@link #registerAddresses(Collection)} and {@link #unregisterAddresses(Collection)} have no effect.
 * <p>
 * Note, this IP finder is only workable when it used in Kubernetes environment.
 * Choose another implementation of {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder} for local
 * or home network tests.
 */
public class TcpDiscoveryKubernetesIpFinder extends TcpDiscoveryIpFinderAdapter {
    /** Grid logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Init routine guard. */
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Init routine latch. */
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /** Trust manager. */
    private TrustManager[] trustAll = new TrustManager[] {
        new X509TrustManager() {
            public void checkServerTrusted(X509Certificate[] certs, String authType) {}
            public void checkClientTrusted(X509Certificate[] certs, String authType) {}
            public X509Certificate[] getAcceptedIssuers() { return null; }
        }
    };

    /** Host verifier. */
    private HostnameVerifier trustAllHosts = new HostnameVerifier() {
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }
    };

    /** Ignite's Kubernetes Service name. */
    private String serviceName = "ignite";

    /** Ignite Pod setNamespace name. */
    private String namespace = "default";

    /** Kubernetes API server URL in a string form. */
    private String master = "https://kubernetes.default.svc.cluster.local:443";

    /** Account token location. */
    private String accountToken = "/var/run/secrets/kubernetes.io/serviceaccount/token";

    /** Kubernetes API server URL. */
    private URL url;

    /** SSL context */
    private SSLContext ctx;

    /**
     * Creates an instance of Kubernetes IP finder.
     */
    public TcpDiscoveryKubernetesIpFinder() {
        setShared(true);
    }

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        init();

        Collection<InetSocketAddress> addrs = new ArrayList<>();

        try {
            if (log.isDebugEnabled())
                log.debug("Getting Apache Ignite endpoints from: " + url);

            HttpsURLConnection conn = (HttpsURLConnection)url.openConnection();

            conn.setHostnameVerifier(trustAllHosts);

            conn.setSSLSocketFactory(ctx.getSocketFactory());
            conn.addRequestProperty("Authorization", "Bearer " + serviceAccountToken(accountToken));

            // Sending the request and processing a response.
            ObjectMapper mapper = new ObjectMapper();

            Endpoints endpoints = mapper.readValue(conn.getInputStream(), Endpoints.class);

            if (endpoints != null) {
                if (endpoints.subsets != null && !endpoints.subsets.isEmpty()) {
                    for (Subset subset : endpoints.subsets) {

                        if (subset.addresses != null && !subset.addresses.isEmpty()) {
                            for (Address address : subset.addresses) {
                                addrs.add(new InetSocketAddress(address.ip, 0));

                                if (log.isDebugEnabled())
                                    log.debug("Added an address to the list: " + address.ip);
                            }
                        }
                    }
                }
            }
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to retrieve Ignite pods IP addresses.", e);
        }

        return addrs;
    }

    /** {@inheritDoc} */
    @Override public void registerAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        // No-op
    }

    /**
     * Sets the name of Kubernetes service for Ignite pods' IP addresses lookup. The name of the service must be equal
     * to the name set in service's Kubernetes configuration. If this parameter is not changed then the name of the
     * service has to be set to 'ignite' in the corresponding Kubernetes configuration.
     *
     * @param service Kubernetes service name for IP addresses lookup. If it's not set then 'ignite' is used by default.
     */
    public void setServiceName(String service) {
        this.serviceName = service;
    }

    /**
     * Sets the namespace the Kubernetes service belongs to. By default, it's supposed that the service is running under
     * Kubernetes `default` namespace.
     *
     * @param namespace The Kubernetes service namespace for IP addresses lookup.
     */
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    /**
     * Sets the host name of the Kubernetes API server. By default the following host name is used:
     * 'https://kubernetes.default.svc.cluster.local:443'.
     *
     * @param master The host name of the Kubernetes API server.
     */
    public void setMasterUrl(String master) {
        this.master = master;
    }

    /**
     * Specifies the path to the service token file. By default the following account token is used:
     * '/var/run/secrets/kubernetes.io/serviceaccount/token'.
     *
     * @param accountToken The path to the service token file.
     */
    public void setAccountToken(String accountToken) {
        this.accountToken = accountToken;
    }

    /**
     * Kubernetes IP finder initialization.
     *
     * @throws IgniteSpiException In case of error.
     */
    private void init() throws IgniteSpiException {
        if (initGuard.compareAndSet(false, true)) {

            if (serviceName == null || serviceName.isEmpty() ||
                namespace == null || namespace.isEmpty() ||
                master == null || master.isEmpty() ||
                accountToken == null || accountToken.isEmpty()) {
                throw new IgniteSpiException(
                    "One or more configuration parameters are invalid [setServiceName=" +
                        serviceName + ", setNamespace=" + namespace + ", setMasterUrl=" +
                        master + ", setAccountToken=" + accountToken + "]");
            }

            try {
                // Preparing the URL and SSL context to be used for connection purposes.
                String path = String.format("/api/v1/namespaces/%s/endpoints/%s", namespace, serviceName);

                url = new URL(master + path);

                ctx = SSLContext.getInstance("SSL");

                ctx.init(null, trustAll, new SecureRandom());
            }
            catch (Exception e) {
                throw new IgniteSpiException("Failed to connect to Ignite's Kubernetes Service.", e);
            }
            finally {
                initLatch.countDown();
            }
        }
        else {
            try {
                U.await(initLatch);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IgniteSpiException("Thread has been interrupted.", e);
            }

            if (url == null || ctx == null)
                throw new IgniteSpiException("IP finder has not been initialized properly.");
        }
    }

    /**
     * Reads content of the service account token file.
     *
     * @param file The path to the service account token.
     * @return Service account token.
     */
    private String serviceAccountToken(String file)  {
        try {
            return new String(Files.readAllBytes(Paths.get(file)));
        } catch (IOException e) {
            throw new IgniteSpiException("Failed to load services account token [setAccountToken= " + file + "]", e);
        }
    }

    /**
     * Object used by Jackson for processing of Kubernetes lookup service's response.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class Address {
        /** */
        public String ip;
    }

    /**
     * Object used by Jackson for processing of Kubernetes lookup service's response.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class Subset {
        /** */
        public List<Address> addresses;
    }

    /**
     * Object used by Jackson for processing of Kubernetes lookup service's response.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class Endpoints {
        /** */
        public List<Subset> subsets;
    }
}
