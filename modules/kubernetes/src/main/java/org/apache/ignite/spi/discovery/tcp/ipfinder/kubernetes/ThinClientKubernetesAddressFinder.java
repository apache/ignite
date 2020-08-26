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

import java.net.InetAddress;
import java.util.function.Supplier;
import org.apache.ignite.kubernetes.KubernetesServiceAddressResolver;
import org.apache.ignite.kubernetes.KubernetesConnectorConfigurator;
import org.apache.ignite.kubernetes.KubernetesConnectorDefaults;

/**
 * Address finder for automatic lookup of Ignite nodes running in Kubernetes environment. All Ignite nodes have to
 * deployed as Kubernetes pods in order to be found. Applications and Ignite nodes running outside of Kubernetes
 * will not be able to reach the containerized counterparts.
 * <p>
 * The implementation is based on a distinct Kubernetes service that has to be created and should be deployed prior
 * Ignite nodes startup. The service will maintain a list of all endpoints (internal IP addresses) of all containerized
 * Ignite pods running so far. The name of the service must be equal to {@link #setServiceName(String)} which is
 * `ignite` by default.
 * <p>
 * As for Ignite pods, it's recommended to label them in such a way that the service will use the label in its selector
 * configuration excluding endpoints of irrelevant Kubernetes pods running in parallel.
 * <p>
 * The address finder, in its turn, will call this service to retrieve Ignite pods IP addresses. The port will be
 * set with {@link ReliableChannel#parseAddresses(String[])}. Make sure that all Ignite pods occupy a similar
 * ClientConnector port, otherwise they will not be able to connect each other using this address finder.
 * <h2 class="header">Optional configuration</h2>
 * <ul>
 *      <li>The Kubernetes service name for IP addresses lookup (see {@link #setServiceName(String)})</li>
 *      <li>The Kubernetes service namespace for IP addresses lookup (see {@link #setNamespace(String)}</li>
 *      <li>The host name of the Kubernetes API server (see {@link #setMasterUrl(String)})</li>
 *      <li>Path to the service token (see {@link #setAccountToken(String)}</li>
 *      <li>To include not-ready pods (see {@link #includeNotReadyAddresses(boolean)}</li>
 * </ul>
 * <p>
 */
public class ThinClientKubernetesAddressFinder implements Supplier<String[]>, KubernetesConnectorConfigurator {
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

    /** */
    @Override public String[] get() {
        return new KubernetesServiceAddressResolver(srvcName, namespace, master, accountToken, includeNotReadyAddresses)
            .getServiceAddresses()
            .stream().map(InetAddress::getHostAddress)
            .toArray(String[]::new);
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
