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

package org.apache.ignite.internal.processors.service;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;

/**
 * Services deployments actions to process from deployment worker.
 */
public class ServicesDeploymentActions {
    /** Whenever it's necessary to deactivate service processor. */
    private boolean deactivate;

    /** Services info to deploy. */
    private Map<IgniteUuid, ServiceInfo> servicesToDeploy;

    /** Services info to undeploy. */
    private Map<IgniteUuid, ServiceInfo> servicesToUndeploy;

    /** Services deployment topologies. */
    private Map<IgniteUuid, HashMap<UUID, Integer>> depTops;

    /** Services deployment errors. */
    private Map<IgniteUuid, Collection<byte[]>> depErrors;

    /**
     * @param servicesToDeploy Services info to deploy.
     */
    public void servicesToDeploy(@NotNull Map<IgniteUuid, ServiceInfo> servicesToDeploy) {
        this.servicesToDeploy = servicesToDeploy;
    }

    /**
     * @return Services info to deploy.
     */
    @NotNull public Map<IgniteUuid, ServiceInfo> servicesToDeploy() {
        return servicesToDeploy != null ? servicesToDeploy : Collections.emptyMap();
    }

    /**
     * @param servicesToUndeploy Services info to undeploy.
     */
    public void servicesToUndeploy(@NotNull Map<IgniteUuid, ServiceInfo> servicesToUndeploy) {
        this.servicesToUndeploy = servicesToUndeploy;
    }

    /**
     * @return Services info to undeploy.
     */
    @NotNull public Map<IgniteUuid, ServiceInfo> servicesToUndeploy() {
        return servicesToUndeploy != null ? servicesToUndeploy : Collections.emptyMap();
    }

    /**
     * @param deactivate Whenever it's necessary to deactivate service processor.
     */
    public void deactivate(boolean deactivate) {
        this.deactivate = deactivate;
    }

    /**
     * @return Whenever it's necessary to deactivate service processor.
     */
    public boolean deactivate() {
        return deactivate;
    }

    /**
     * @return Deployment topologies.
     */
    @NotNull public Map<IgniteUuid, HashMap<UUID, Integer>> deploymentTopologies() {
        return depTops != null ? depTops : Collections.emptyMap();
    }

    /**
     * @param depTops Deployment topologies.
     */
    public void deploymentTopologies(@NotNull Map<IgniteUuid, HashMap<UUID, Integer>> depTops) {
        this.depTops = depTops;
    }

    /**
     * @return Deployment errors.
     */
    @NotNull public Map<IgniteUuid, Collection<byte[]>> deploymentErrors() {
        return depErrors != null ? depErrors : Collections.emptyMap();
    }

    /**
     * @param depErrors Deployment errors.
     */
    public void deploymentErrors(@NotNull Map<IgniteUuid, Collection<byte[]>> depErrors) {
        this.depErrors = depErrors;
    }
}
