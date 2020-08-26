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

package org.apache.ignite.kubernetes;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
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
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * The class is responsible to fetch list of IP address for all pods that runs the specified kubernetes service.
 */
public class KubernetesServiceAddressResolver {
    /** Kubernetes Service name. */
    private final String srvcName;

    /** Pod namespace name. */
    private final String namespace;

    /** Kubernetes API server URL in a string form. */
    private final String master;

    /** Account token location. */
    private final String accountToken;

    /** Kubernetes API server URL. */
    private URL url;

    /** SSL context */
    private SSLContext ctx;

    /** Whether addresses of pods in not-ready state should be included. */
    private final boolean includeNotReadyAddresses;

    /** Init routine guard. */
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Init routine latch. */
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /** Trust manager. */
    private final TrustManager[] trustAll = new TrustManager[] {
        new X509TrustManager() {
            @Override public void checkServerTrusted(X509Certificate[] certs, String authType) {}

            @Override public void checkClientTrusted(X509Certificate[] certs, String authType) {}

            @Override public X509Certificate[] getAcceptedIssuers() { return null; }
        }
    };

    /** Host verifier. */
    private final HostnameVerifier trustAllHosts = new HostnameVerifier() {
        @Override public boolean verify(String hostname, SSLSession ses) {
            return true;
        }
    };

    /** Constructor. */
    public KubernetesServiceAddressResolver(String srvcName, String namespace, String master, String accountToken,
                               boolean includeNotReadyAddresses) {
        this.srvcName = srvcName;
        this.namespace = namespace;
        this.master = master;
        this.accountToken = accountToken;
        this.includeNotReadyAddresses = includeNotReadyAddresses;
    }

    /** Return IP addresses of pods that runs the service. */
    public Collection<InetAddress> getServiceAddresses() {
        init();

        Collection<InetAddress> addrs = new ArrayList<>();

        try {
            HttpsURLConnection conn = (HttpsURLConnection)url.openConnection();

            conn.setHostnameVerifier(trustAllHosts);

            conn.setSSLSocketFactory(ctx.getSocketFactory());
            conn.addRequestProperty("Authorization", "Bearer " + serviceAccountToken(accountToken));

            // Sending the request and processing a response.
            ObjectMapper mapper = new ObjectMapper();

            Endpoints endpoints = mapper.readValue(conn.getInputStream(), Endpoints.class);

            if (endpoints != null && endpoints.subsets != null && !endpoints.subsets.isEmpty()) {
                for (Subset subset : endpoints.subsets) {
                    addrs.addAll(parseAddresses(subset.addresses));

                    if (includeNotReadyAddresses)
                        addrs.addAll(parseAddresses(subset.notReadyAddresses));
                }
            }
        }
        catch (Exception e) {
            throw new IgniteException("Failed to retrieve Ignite pods IP addresses.", e);
        }

        return addrs;
    }

    /**
     * Prepare url and ssl context to request Kubernetes API server.
     */
    private void init() {
        if (initGuard.compareAndSet(false, true)) {

            if (srvcName == null || srvcName.isEmpty() ||
                namespace == null || namespace.isEmpty() ||
                master == null || master.isEmpty() ||
                accountToken == null || accountToken.isEmpty()) {
                throw new IgniteException(
                    "One or more configuration parameters are invalid [setServiceName=" +
                        srvcName + ", setNamespace=" + namespace + ", setMasterUrl=" +
                        master + ", setAccountToken=" + accountToken + "]");
            }

            try {
                // Preparing the URL and SSL context to be used for connection purposes.
                String path = String.format("/api/v1/namespaces/%s/endpoints/%s", namespace, srvcName);

                url = new URL(master + path);

                ctx = SSLContext.getInstance("SSL");

                ctx.init(null, trustAll, new SecureRandom());
            }
            catch (Exception e) {
                throw new IgniteException("Failed to connect to Ignite's Kubernetes Service.", e);
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
                throw new IgniteException("Thread has been interrupted.", e);
            }

            if (url == null || ctx == null)
                throw new IgniteException("IP finder has not been initialized properly.");
        }
    }

    /**
     * Convert response of Kubernetes API server to collection of IP addresses.
     */
    private Collection<InetAddress> parseAddresses(List<Address> addresses) {
        Collection<InetAddress> addrs = new ArrayList<>();
        if (addresses != null && !addresses.isEmpty()) {
            for (Address address : addresses) {
                try {
                    addrs.add(InetAddress.getByName(address.ip));
                } catch (UnknownHostException ignore) {
                    throw new IgniteException("Kubernetes Address is not valid IP address");
                }
            }
        }
        return addrs;
    }

    /**
     * Reads content of the service account token file.
     *
     * @param file The path to the service account token.
     * @return Service account token.
     */
    private String serviceAccountToken(String file) {
        try {
            return new String(Files.readAllBytes(Paths.get(file)));
        } catch (IOException e) {
            throw new IgniteException("Failed to load services account token [setAccountToken= " + file + "]", e);
        }
    }

    /**
     * Object used by Jackson for processing of Kubernetes lookup service's response.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class Address {
        /** */
        String ip;
    }

    /**
     * Object used by Jackson for processing of Kubernetes lookup service's response.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class Subset {
        /** */
        List<Address> addresses;

        /** */
        List<Address> notReadyAddresses;
    }

    /**
     * Object used by Jackson for processing of Kubernetes lookup service's response.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class Endpoints {
        /** */
        List<Subset> subsets;
    }
}
