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

package org.apache.ignite.spi.discovery.tcp;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.jetbrains.annotations.Nullable;

/**
 * Creates TestReconnectProcessor.
 */
public class TestReconnectPluginProvider implements PluginProvider {
    /** */
    private GridKernalContext igniteCtx;

    /** */
    public static volatile boolean enabled;

    /** {@inheritDoc} */
    @Override public String name() {
        return "TestReconnectPlugin";
    }

    /** {@inheritDoc} */
    @Override public String version() {
        return "1.0";
    }

    /** {@inheritDoc} */
    @Override public String copyright() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
        igniteCtx = ((IgniteKernal)ctx.grid()).context();
    }

    /** {@inheritDoc} */
    @Override public void start(PluginContext ctx) throws IgniteCheckedException {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStart() throws IgniteCheckedException {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStop(boolean cancel) {
        // No-op
    }

    /** {@inheritDoc} */
    @Nullable @Override public Serializable provideDiscoveryData(UUID nodeId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void receiveDiscoveryData(UUID nodeId, Serializable data) {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void validateNewNode(ClusterNode node) throws PluginValidationException {
        // No-op
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object createComponent(PluginContext ctx, Class cls) {
        if (enabled && GridSecurityProcessor.class.equals(cls))
            return new TestReconnectProcessor(igniteCtx);

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgnitePlugin plugin() {
        return new IgnitePlugin() {};
    }

    /** {@inheritDoc} */
    @Override public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
        return null;
    }
}
