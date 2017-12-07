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

package org.apache.ignite.compatibility.testframework.plugins;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.nodevalidation.DiscoveryNodeValidationProcessor;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.jetbrains.annotations.Nullable;

/**
 * Creates {@link DisabledValidationProcessor} to allow for nodes with different
 * versions {@link IgniteNodeAttributes#ATTR_BUILD_VER} join to topology.
 */
public class TestCompatibilityPluginProvider implements PluginProvider {
    /** */
    private GridKernalContext kCtx;

    /** {@inheritDoc} */
    @Override public String name() {
        return "TestCompatibilityPlugin";
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
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) throws IgniteCheckedException {
        kCtx = ((IgniteKernal)ctx.grid()).context();
    }

    /** {@inheritDoc} */
    @Override public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void start(PluginContext ctx) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStart() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStop(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public Serializable provideDiscoveryData(UUID nodeId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void receiveDiscoveryData(UUID nodeId, Serializable data) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void validateNewNode(ClusterNode node) throws PluginValidationException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object createComponent(PluginContext ctx, Class cls) {
        if (DiscoveryNodeValidationProcessor.class == cls)
            return new DisabledValidationProcessor(kCtx);

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgnitePlugin plugin() {
        return new IgnitePlugin() {
            // No-op.
        };
    }
}
