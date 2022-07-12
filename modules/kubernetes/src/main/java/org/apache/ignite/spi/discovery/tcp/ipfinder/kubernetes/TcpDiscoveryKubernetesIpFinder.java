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
import org.apache.ignite.internal.kubernetes.connection.KubernetesServiceAddressResolver;
import org.apache.ignite.kubernetes.configuration.KubernetesConnectionConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;

/**
 * IP finder for automatic lookup of Ignite nodes running in Kubernetes environment. All Ignite nodes have to deployed
 * as Kubernetes pods in order to be discovered. An application that uses Ignite client nodes as a gateway to the
 * cluster is required to be containerized as well. Applications and Ignite nodes running outside of Kubernetes will
 * not be able to reach the containerized counterparts.
 * <p>
 * The implementation is based on a distinct Kubernetes service. The name of the service must be set with
 * {@code KubernetesConnectionConfiguration}. As for Ignite pods, it's recommended to label them in such way
 * that the service will target only nodes that are able to be discovered, i.e. server and thick client nodes.
 * <p>
 * The IP finder, in its turn, will call this service to retrieve Ignite pods IP addresses. The port will be
 * either the one that is set with {@link TcpDiscoverySpi#setLocalPort(int)} or {@link TcpDiscoverySpi#DFLT_PORT}.
 * Make sure that all Ignite pods occupy a similar discovery port, otherwise they will not be able to discover each
 * other using this IP finder.
 * <p>
 * Both {@link #registerAddresses(Collection)} and {@link #unregisterAddresses(Collection)} have no effect.
 * <p>
 * Note, this IP finder is only workable when it used in Kubernetes environment.
 * Choose another implementation of {@link org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder} for local
 * or home network tests.
 */
public class TcpDiscoveryKubernetesIpFinder extends TcpDiscoveryIpFinderAdapter {
    /** Kubernetes connection configuration */
    private final KubernetesConnectionConfiguration cfg;

    /**
     * Creates an instance of Kubernetes IP finder.
     */
    public TcpDiscoveryKubernetesIpFinder() {
        this(new KubernetesConnectionConfiguration());
    }

    /**
     * Creates an instance of Kubernetes IP finder.
     *
     * @param cfg Kubernetes IP finder configuration.
     */
    public TcpDiscoveryKubernetesIpFinder(KubernetesConnectionConfiguration cfg) {
        setShared(true);
        this.cfg = cfg;
    }

    /** {@inheritDoc} */
    @Override public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
        try {
            return new KubernetesServiceAddressResolver(cfg)
                .getServiceAddresses()
                .stream().map(addr -> new InetSocketAddress(addr, cfg.getDiscoveryPort()))
                .collect(Collectors.toCollection(ArrayList::new));
        }
        catch (Exception e) {
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

    /**
     * @param service Kubernetes service name.
     * @deprecated set parameters with {@code KubernetesConnectionConfiguration} instead.
     */
    @Deprecated
    public void setServiceName(String service) {
        cfg.setServiceName(service);
    }

    /**
     * @param namespace Namespace of the Kubernetes service.
     * @deprecated set parameters with {@code KubernetesConnectionConfiguration} instead.
     */
    @Deprecated
    public void setNamespace(String namespace) {
        cfg.setNamespace(namespace);
    }

    /**
     * @param master Host name of the Kubernetes API server.
     * @deprecated set parameters with {@code KubernetesConnectionConfiguration} instead.
     */
    @Deprecated
    public void setMasterUrl(String master) {
        cfg.setMasterUrl(master);
    }

    /**
     * @param accountToken Path to the service token file.
     * @deprecated set parameters with {@code KubernetesConnectionConfiguration} instead.
     */
    @Deprecated
    public void setAccountToken(String accountToken) {
        cfg.setAccountToken(accountToken);
    }

    /**
     * @param includeNotReadyAddresses Whether addresses of not-ready pods should be included.
     * @deprecated set parameters with {@code KubernetesConnectionConfiguration} instead.
     */
    @Deprecated
    public void includeNotReadyAddresses(boolean includeNotReadyAddresses) {
        cfg.setIncludeNotReadyAddresses(includeNotReadyAddresses);
    }
}
