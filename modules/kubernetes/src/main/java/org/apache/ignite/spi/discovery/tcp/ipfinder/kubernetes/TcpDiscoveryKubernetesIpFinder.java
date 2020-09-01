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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.ignite.kubernetes.KubernetesConnectorConfigurator;
import org.apache.ignite.kubernetes.KubernetesConnectorDefaults;
import org.apache.ignite.kubernetes.KubernetesServiceAddressResolver;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;

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
 *      <li>To include not-ready pods (see {@link #includeNotReadyAddresses(boolean)}</li>
 * </ul>
 * <p>
 * Both {@link #registerAddresses(Collection)} and {@link #unregisterAddresses(Collection)} have no effect.
 * <p>
 * Note, this IP finder is only workable when it used in Kubernetes environment.
 * Choose another implementation of {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder} for local
 * or home network tests.
 */
public class TcpDiscoveryKubernetesIpFinder extends TcpDiscoveryIpFinderAdapter
    implements KubernetesConnectorConfigurator {
    /** Ignite's Kubernetes Service name. */
    private String srvcName = KubernetesConnectorDefaults.SRVC_NAME;

    /** Ignite Pod setNamespace name. */
    private String namespace = KubernetesConnectorDefaults.NAMESPACE;

    /** Kubernetes API server URL in a string form. */
    private String master = KubernetesConnectorDefaults.MASTER;

    /** Account token location. */
    private String accountToken = KubernetesConnectorDefaults.ACCOUNT_TOKEN;

    /** Whether addresses of pods in not-ready state should be included. */
    private boolean includeNotReadyAddresses = KubernetesConnectorDefaults.INCLUDE_NOT_READY_ADDR;

    /**
     * Creates an instance of Kubernetes IP finder.
     */
    public TcpDiscoveryKubernetesIpFinder() {
        setShared(true);
    }

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        try {
            return new KubernetesServiceAddressResolver(srvcName, namespace, master, accountToken, includeNotReadyAddresses)
                .getServiceAddresses()
                .stream().map(addr -> new InetSocketAddress(addr, 0))
                .collect(Collectors.toCollection(ArrayList::new));
        } catch (Exception e) {
            throw new IgniteSpiException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void registerAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void setServiceName(String service) {
        this.srvcName = service;
    }

    /** {@inheritDoc} */
    @Override public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    /** {@inheritDoc} */
    @Override public void setMasterUrl(String master) {
        this.master = master;
    }

    /** {@inheritDoc} */
    @Override public void setAccountToken(String accountToken) {
        this.accountToken = accountToken;
    }

    /** {@inheritDoc} */
    @Override public void includeNotReadyAddresses(boolean includeNotReadyAddresses) {
        this.includeNotReadyAddresses = includeNotReadyAddresses;
    }
}
