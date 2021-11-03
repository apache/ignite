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

package org.apache.ignite.kubernetes.configuration;

import org.apache.ignite.spi.IgniteSpiException;

/**
 * Configuration for Kubernetes connection.
 */
public class KubernetesConnectionConfiguration {
    /** Kubernetes service name */
    private String srvcName = "ignite";

    /** Kubernetes namespace name. */
    private String namespace = "default";

    /** Kubernetes API server URL in a string form. */
    private String master = "https://kubernetes.default.svc.cluster.local:443";

    /** Account token location. */
    private String accountToken = "/var/run/secrets/kubernetes.io/serviceaccount/token";

    /** Whether addresses of pods in not-ready state should be included. */
    private boolean includeNotReadyAddresses = false;

    /** Port to use for discovery **/
    private int discoveryPort = 0;
    
    /**
     * Sets the name of Kubernetes service for Ignite pods' IP addresses lookup. The name of the service must be equal
     * to the name set in service's Kubernetes configuration. If this parameter is not changed then the name of the
     * service has to be set to 'ignite' in the corresponding Kubernetes configuration.
     *
     * @param service Kubernetes service name for IP addresses lookup. If it's not set then 'ignite' is used by default.
     * @return {@code this} for chaining.
     */
    public KubernetesConnectionConfiguration setServiceName(String service) {
        srvcName = service;

        return this;
    }

    /**
     * @return Kubernetes service name.
     */
    public String getServiceName() {
        return srvcName;
    }

    /**
     * Sets the namespace the Kubernetes service belongs to. By default, it's supposed that the service is running under
     * Kubernetes `default` namespace.
     *
     * @param namespace The Kubernetes service namespace for IP addresses lookup.
     * @return {@code this} for chaining.
     */
    public KubernetesConnectionConfiguration setNamespace(String namespace) {
        this.namespace = namespace;

        return this;
    }

    /**
     * @return Kubernetes namespace.
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Sets the host name of the Kubernetes API server. By default the following host name is used:
     * 'https://kubernetes.default.svc.cluster.local:443'.
     *
     * @param master The host name of the Kubernetes API server.
     * @return {@code this} for chaining.
     */
    public KubernetesConnectionConfiguration setMasterUrl(String master) {
        this.master = master;

        return this;
    }

    /**
     * @return Kubernetes master url.
     */
    public String getMaster() {
        return master;
    }

    /**
     * Specifies the path to the service token file. By default the following account token is used:
     * '/var/run/secrets/kubernetes.io/serviceaccount/token'.
     *
     * @param accountToken The path to the service token file.
     * @return {@code this} for chaining.
     */
    public KubernetesConnectionConfiguration setAccountToken(String accountToken) {
        this.accountToken = accountToken;

        return this;
    }

    /**
     * @return Kubernetes account token.
     */
    public String getAccountToken() {
        return accountToken;
    }

    /**
     * Determines whether addresses of not-ready pods should be included. Default is false.
     *
     * @param includeNotReadyAddresses Flag to include not-ready pods.
     * @return {@code this} for chaining.
     */
    public KubernetesConnectionConfiguration setIncludeNotReadyAddresses(boolean includeNotReadyAddresses) {
        this.includeNotReadyAddresses = includeNotReadyAddresses;

        return this;
    }

    /**
     * @return Flag include not ready addresses.
     */
    public boolean getIncludeNotReadyAddresses() {
        return includeNotReadyAddresses;
    }

    /**
     * Specifies the port which is returned to the caller to use for service discovery.
     * Defaults to 0.
     * @param discoveryPort Port to use for Kubernetes IP Finder
     * @return {@code this} for chaining.
     */
    public KubernetesConnectionConfiguration setDiscoveryPort(int discoveryPort) {
        this.discoveryPort = discoveryPort;
        
        return this;
    }

    /**
     * @return Kubernetes IP Finder port.
     */
    public int getDiscoveryPort() {
        return discoveryPort;
    }

    /**
     * Verify that configuration is valid.
     */
    public void verify() {
        if (paramIsNotSet(srvcName) || paramIsNotSet(namespace)
            || paramIsNotSet(master) || paramIsNotSet(accountToken))
            throw new IgniteSpiException(
                "One or more configuration parameters are invalid [setServiceName=" +
                    srvcName + ", setNamespace=" + namespace + ", setMasterUrl=" +
                    master + ", setAccountToken=" + accountToken + "]");
    }

    /**
     * Check that param value is not set.
     */
    private boolean paramIsNotSet(String param) {
        return param == null || param.isEmpty();
    }
}
