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

import java.io.Serializable;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.ServiceConfiguration;
import org.jetbrains.annotations.NotNull;

/**
 * Service change request.
 */
public class DynamicServiceChangeRequest implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Deploy service flag mask. */
    private static final byte SERVICE_DEPLOY_FLAG_MASK = 0x01;

    /** Undeploy service flag mask. */
    private static final byte SERVICE_UNDEPLOY_FLAG_MASK = 0x02;

    /** Service id. */
    private final IgniteUuid srvcId;

    /** Service configuration. May be {@code null} in case of undeploy. */
    private ServiceConfiguration cfg;

    /** Flags. */
    private byte flags;

    /**
     * @param srvcId Service id.
     */
    protected DynamicServiceChangeRequest(@NotNull IgniteUuid srvcId) {
        this.srvcId = srvcId;
    }

    /**
     * Creates deployment request with given id an configuration.
     *
     * @param srvcId Service id.
     * @param cfg Service configuration.
     * @return Deployment request.
     */
    public static DynamicServiceChangeRequest deploymentRequest(@NotNull IgniteUuid srvcId,
        @NotNull ServiceConfiguration cfg) {
        DynamicServiceChangeRequest req = new DynamicServiceChangeRequest(srvcId);

        req.configuration(cfg);
        req.markDeploy();

        return req;
    }

    /**
     * Creates undeployment request for service with given id.
     *
     * @param srvcId Service id.
     * @return Undeployment request.
     */
    public static DynamicServiceChangeRequest undeploymentRequest(@NotNull IgniteUuid srvcId) {
        DynamicServiceChangeRequest req = new DynamicServiceChangeRequest(srvcId);

        req.markUndeploy();

        return req;
    }

    /**
     * @return Service id.
     */
    public IgniteUuid serviceId() {
        return srvcId;
    }

    /**
     * @return Service configuration.
     */
    public ServiceConfiguration configuration() {
        return cfg;
    }

    /**
     * @param cfg New service configuration.
     */
    void configuration(ServiceConfiguration cfg) {
        this.cfg = cfg;
    }

    /**
     * Marks the message's action as deploy service request.
     */
    void markDeploy() {
        flags |= SERVICE_DEPLOY_FLAG_MASK;
    }

    /**
     * @return If this is deployment request.
     */
    public boolean deploy() {
        return (flags & SERVICE_DEPLOY_FLAG_MASK) != 0;
    }

    /**
     * Marks the message's action as undeploy service request.
     */
    void markUndeploy() {
        flags |= SERVICE_UNDEPLOY_FLAG_MASK;
    }

    /**
     * @return If this is undeployment request.
     */
    public boolean undeploy() {
        return (flags & SERVICE_UNDEPLOY_FLAG_MASK) != 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DynamicServiceChangeRequest.class, this);
    }
}
