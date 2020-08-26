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
 * Default values for Kubernetes connector configuration.
 */
public class KubernetesConnectorDefaults {
    /** Ignite's Kubernetes Service name. */
    public static final String SRVC_NAME = "ignite";

    /** Ignite Pod setNamespace name. */
    public static final String NAMESPACE = "default";

    /** Kubernetes API server URL in a string form. */
    public static final String MASTER = "https://kubernetes.default.svc.cluster.local:443";

    /** Account token location. */
    public static final String ACCOUNT_TOKEN = "/var/run/secrets/kubernetes.io/serviceaccount/token";

    /** Whether addresses of pods in not-ready state should be included. */
    public static final boolean INCLUDE_NOT_READY_ADDR = false;
}
