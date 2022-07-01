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

package org.apache.ignite.client.kubernetes;

import org.apache.ignite.client.ClientAddressFinder;
import org.apache.ignite.internal.kubernetes.connection.KubernetesServiceAddressResolver;
import org.apache.ignite.kubernetes.configuration.KubernetesConnectionConfiguration;

import static org.apache.ignite.configuration.ClientConnectorConfiguration.DFLT_PORT;

/**
 * Address finder for automatic lookup of Ignite server nodes running in Kubernetes environment. All Ignite nodes have
 * to be deployed as Kubernetes pods in order to be found. Applications and Ignite nodes running outside of Kubernetes
 * will not be able to reach the containerized counterparts.
 * <p>
 * The implementation is based on a distinct Kubernetes service. The name of the service must be set with
 * {@code KubernetesConnectionConfiguration}. As for Ignite pods, it's recommended to label them in such a way that
 * the service will target only server nodes.
 * <p>
 * The address finder, in its turn, will call this service to retrieve Ignite pods IP addresses. The port is set with
 * value passed to constructor. Make sure that all Ignite pods occupy a similar ClientConnector port,
 * otherwise they will not be able to connect each other using this address finder.
 * <p>
 */
public class ThinClientKubernetesAddressFinder implements ClientAddressFinder {
    /** Kubernetes service address resolver. */
    private final KubernetesServiceAddressResolver resolver;

    /**
     * Port that Ignite uses to listen client connections.
     *
     * {@link org.apache.ignite.configuration.ClientConnectorConfiguration#setPort(int)}
     */
    private final int port;

    /** @param cfg Kubernetes connection configuration. */
    public ThinClientKubernetesAddressFinder(KubernetesConnectionConfiguration cfg) {
        this(cfg, DFLT_PORT);
    }

    /**
     * @param cfg Kubernetes connection configuration.
     * @param port Port that Ignite uses to listen client connections.
     */
    public ThinClientKubernetesAddressFinder(KubernetesConnectionConfiguration cfg, int port) {
        resolver = new KubernetesServiceAddressResolver(cfg);
        this.port = port;
    }

    /** {@inheritDoc} */
    @Override public String[] getAddresses() {
        return resolver
            .getServiceAddresses()
            .stream().map(a -> a.getHostAddress() + ":" + port)
            .toArray(String[]::new);
    }
}
