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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.ServiceConfiguration;
import org.jetbrains.annotations.NotNull;

/**
 * Actions of change service state to be processed in the service deployment process.
 */
public class ServiceDeploymentActions {
    /** Whenever it's necessary to deactivate service processor. */
    private boolean deactivate;

    /** Services info to deploy. */
    private Map<IgniteUuid, ServiceInfo> servicesToDeploy;

    /** Services info to undeploy. */
    private Map<IgniteUuid, ServiceInfo> servicesToUndeploy;

    /** Services deployment topologies. */
    private Map<IgniteUuid, Map<UUID, Integer>> depTops;

    /** Services deployment errors. */
    private Map<IgniteUuid, Collection<byte[]>> depErrors;

    /** Current platform */
    private final String platform;

    /**
     * @param ctx Kernal context.
     */
    public ServiceDeploymentActions(GridKernalContext ctx) {
        platform = ctx.platform().hasContext() ? ctx.platform().context().platform() : "";
    }

    /**
     * @param servicesToDeploy Services info to deploy.
     */
    public void servicesToDeploy(@NotNull Map<IgniteUuid, ServiceInfo> servicesToDeploy) {
        Map<IgniteUuid, ServiceInfo> res = new HashMap<>();

        for (Map.Entry<IgniteUuid, ServiceInfo> kv: servicesToDeploy.entrySet()) {
            ServiceInfo dsc = kv.getValue();

            ServiceConfiguration cfg = dsc.configuration();

            String svcPlatform = PlatformUtils.servicePlatform(cfg);

            if (!svcPlatform.isEmpty() && !platform.equals(svcPlatform))
                continue;

            res.put(kv.getKey(), dsc);
        }

        this.servicesToDeploy = Collections.unmodifiableMap(res);
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
        this.servicesToUndeploy = Collections.unmodifiableMap(new HashMap<>(servicesToUndeploy));
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
    @NotNull public Map<IgniteUuid, Map<UUID, Integer>> deploymentTopologies() {
        return depTops != null ? depTops : Collections.emptyMap();
    }

    /**
     * @param depTops Deployment topologies.
     */
    public void deploymentTopologies(@NotNull Map<IgniteUuid, Map<UUID, Integer>> depTops) {
        this.depTops = Collections.unmodifiableMap(new HashMap<>(depTops));
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
        this.depErrors = Collections.unmodifiableMap(new HashMap<>(depErrors));
    }
}
