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

/**
 * Interface provide methods to configure Kubernetes connection.
 */
public interface KubernetesConnectorConfigurator {
    /**
     * Sets the name of Kubernetes service for Ignite pods' IP addresses lookup. The name of the service must be equal
     * to the name set in service's Kubernetes configuration. If this parameter is not changed then the name of the
     * service has to be set to 'ignite' in the corresponding Kubernetes configuration.
     *
     * @param service Kubernetes service name for IP addresses lookup. If it's not set then 'ignite' is used by default.
     */
    public void setServiceName(String service);

    /**
     * Sets the namespace the Kubernetes service belongs to. By default, it's supposed that the service is running under
     * Kubernetes `default` namespace.
     *
     * @param namespace The Kubernetes service namespace for IP addresses lookup.
     */
    public void setNamespace(String namespace);

    /**
     * Sets the host name of the Kubernetes API server. By default the following host name is used:
     * 'https://kubernetes.default.svc.cluster.local:443'.
     *
     * @param master The host name of the Kubernetes API server.
     */
    public void setMasterUrl(String master);

    /**
     * Specifies the path to the service token file. By default the following account token is used:
     * '/var/run/secrets/kubernetes.io/serviceaccount/token'.
     *
     * @param accountToken The path to the service token file.
     */
    public void setAccountToken(String accountToken);

    /**
     * Determines whether addresses of not-ready pods should be included. Default is false.
     *
     * @param includeNotReadyAddresses Flag to include not-ready pods.
     */
    public void includeNotReadyAddresses(boolean includeNotReadyAddresses);
}
