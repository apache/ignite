/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal;

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.spi.IgnitePortProtocol;

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
    @Override public MarshallerContext marshallerContext() {
        return ctx.marshallerContext();
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