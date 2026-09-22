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

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceDescriptor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.service.ServiceTopology.EMPTY;

/** Service's information container. */
public class ServiceInfo implements ServiceDescriptor, Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** Context. */
    private transient volatile GridKernalContext ctx;

    /** Origin node ID. */
    @Order(0)
    UUID originNodeId;

    /** Service id. */
    @Order(1)
    IgniteUuid srvcId;

    /** Service configuration. */
    private LazyServiceConfiguration cfg;

    /** Service configuration message. */
    @Order(2)
    transient LazyServiceConfigurationMessage cfgMsg;

    /** Statically configured flag. */
    @Order(3)
    boolean staticCfg;

    /** Topology snapshot. */
    @Order(4)
    @GridToStringInclude
    volatile ServiceTopology top = EMPTY;

    /** Service class. */
    private transient volatile Class<? extends Service> srvcCls;

    /** Default constructor for {@link MessageFactory}. */
    public ServiceInfo() {
        // No-op.
    }

    /**
     * @param originNodeId Initiating node id.
     * @param srvcId Service id.
     * @param cfg Service configuration.
     */
    public ServiceInfo(@NotNull UUID originNodeId, @NotNull IgniteUuid srvcId, @NotNull LazyServiceConfiguration cfg) {
        this(originNodeId, srvcId, cfg, false);
    }

    /**
     * @param originNodeId Initiating node id.
     * @param srvcId Service id.
     * @param cfg Service configuration.
     * @param staticCfg Statically configured flag.
     */
    public ServiceInfo(@NotNull UUID originNodeId, @NotNull IgniteUuid srvcId, @NotNull LazyServiceConfiguration cfg,
        boolean staticCfg) {
        this.originNodeId = originNodeId;
        this.srvcId = srvcId;
        this.cfg = cfg;
        this.staticCfg = staticCfg;

        cfgMsg = new LazyServiceConfigurationMessage(cfg);
    }

    /**
     * Sets kernal context.
     *
     * @param ctx Context.
     */
    public void context(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /**
     * Sets service's new topology snapshot.
     *
     * @param top Topology snapshot.
     */
    public void updateServiceTopology(@NotNull ServiceTopology top) {
        this.top = top;
    }

    /** @return Service Topology. */
    public ServiceTopology serviceTopology() {
        return top;
    }

    /** @return Service configuration. */
    public LazyServiceConfiguration configuration() {
        if (cfg == null && cfgMsg != null)
            cfg = cfgMsg.toConfiguration();

        return cfg;
    }

    /** @return {@code true} if statically configured. */
    public boolean staticallyConfigured() {
        return staticCfg;
    }

    /** @return Service id. */
    public IgniteUuid serviceId() {
        return srvcId;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return configuration().getName();
    }

    /** {@inheritDoc} */
    @Override public Class<? extends Service> serviceClass() {
        if (srvcCls != null)
            return srvcCls;

        String clsName = configuration().serviceClassName();

        try {
            srvcCls = (Class<? extends Service>)Class.forName(clsName);

            return srvcCls;
        }
        catch (ClassNotFoundException e) {
            if (ctx != null) {
                GridDeployment srvcDep = ctx.deploy().getDeployment(clsName);

                if (srvcDep != null) {
                    srvcCls = (Class<? extends Service>)srvcDep.deployedClass(clsName).get1();

                    if (srvcCls != null)
                        return srvcCls;
                }
            }

            throw new IgniteException("Failed to find service class: " + clsName, e);
        }
    }

    /** {@inheritDoc} */
    @Override public int totalCount() {
        return configuration().getTotalCount();
    }

    /** {@inheritDoc} */
    @Override public int maxPerNodeCount() {
        return configuration().getMaxPerNodeCount();
    }

    /** {@inheritDoc} */
    @Nullable @Override public String cacheName() {
        return configuration().getCacheName();
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K> K affinityKey() {
        return (K)configuration().getAffinityKey();
    }

    /** {@inheritDoc} */
    @Override public UUID originNodeId() {
        return originNodeId;
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, Integer> topologySnapshot() {
        return top.snapshot();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServiceInfo.class, this);
    }
}
