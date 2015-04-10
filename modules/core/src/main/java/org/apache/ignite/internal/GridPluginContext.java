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

package org.apache.ignite.internal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.plugin.*;
import org.apache.ignite.spi.*;

import java.util.*;

/**
 *
 */
public class GridPluginContext implements PluginContext {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteConfiguration igniteCfg;

    /**
     * @param ctx Kernal context.
     * @param igniteCfg Ignite configuration.
     */
    public GridPluginContext(GridKernalContext ctx, IgniteConfiguration igniteCfg) {
        this.ctx = ctx;
        this.igniteCfg = igniteCfg;
    }

    /** {@inheritDoc} */
    @Override public IgniteConfiguration igniteConfiguration() {
        return igniteCfg;
    }

    /** {@inheritDoc} */
    @Override public Ignite grid() {
        return ctx.grid();
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> nodes() {
        return ctx.discovery().allNodes();
    }

    /** {@inheritDoc} */
    @Override public ClusterNode localNode() {
        return ctx.discovery().localNode();
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log(Class<?> cls) {
        return ctx.log(cls);
    }

    /** {@inheritDoc} */
    @Override public void registerPort(int port, IgnitePortProtocol proto, Class<?> cls) {
        ctx.ports().registerPort(port, proto, cls);
    }

    /** {@inheritDoc} */
    @Override public void deregisterPort(int port, IgnitePortProtocol proto, Class<?> cls) {
        ctx.ports().deregisterPort(port, proto, cls);
    }

    /** {@inheritDoc} */
    @Override public void deregisterPorts(Class<?> cls) {
        ctx.ports().deregisterPorts(cls);
    }
}
