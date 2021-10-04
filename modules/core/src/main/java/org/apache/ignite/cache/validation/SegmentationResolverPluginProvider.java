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

package org.apache.ignite.cache.validation;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.jetbrains.annotations.Nullable;

import static java.lang.Boolean.TRUE;

/**
 * Represents plugin that provides segmentation resolver extension.
 *
 * @see ClusterStateChangeSegmentationResolver
 */
public class SegmentationResolverPluginProvider implements PluginProvider<PluginConfiguration> {
    /** Name of th segmentation resolver enabled attribute. */
    private static final String ATTR_SEG_RESOLVE_ENABLED = "org.apache.ignite.cache.segmentation.resolver.enabled";

    /** Ignite kernal context. */
    private GridKernalContext ctx;

    /** */
    private ClusterStateChangeSegmentationResolver segResolver;

    /** {@inheritDoc} */
    @Override public String name() {
        return "Segmentation Resolver";
    }

    /** {@inheritDoc} */
    @Override public String version() {
        return "1.0";
    }

    /** {@inheritDoc} */
    @Override public <T extends IgnitePlugin> T plugin() {
        return (T) new IgnitePlugin() {
            // No-op.
        };
    }

    /** {@inheritDoc} */
    @Override public String copyright() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
        this.ctx = ((IgniteEx)ctx.grid()).context();

        segResolver = new ClusterStateChangeSegmentationResolver(this.ctx);

        registry.registerExtension(PluggableSegmentationResolver.class, segResolver);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T createComponent(PluginContext ctx, Class<T> cls) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void start(PluginContext pluginCtx) throws IgniteCheckedException {
        ctx.addNodeAttribute(ATTR_SEG_RESOLVE_ENABLED, true);

        ctx.cache().context().exchange().registerExchangeAwareComponent(segResolver);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStart() {
        for (ClusterNode srv : ctx.discovery().aliveServerNodes()) {
            if (!TRUE.equals(srv.attribute(ATTR_SEG_RESOLVE_ENABLED))) {
                throw new IgniteException("The segmentation resolver plugin is not configured for the cluster the" +
                    " current server node is trying to join. Since the segmentation resolver is only applicable if" +
                    " all server nodes in the cluster have one, the current node will be stopped [nodeId=" +
                    ctx.localNodeId() + ']');
            }
        }
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
        String errMsg = null;

        if (!node.isClient() && !TRUE.equals(node.attribute(ATTR_SEG_RESOLVE_ENABLED))) {
            errMsg = "The segmentation resolver plugin is not configured for the server node that is" +
                " trying to join the cluster. Since the segmentation resolver is only applicable if all server nodes" +
                " in the cluster have one, node join request will be rejected [nodeId=" + node.id() + ']';
        }

        if (!segResolver.isValidSegment())
            errMsg = "The node cannot join the cluster because the cluster was marked as segmented. Resolve" +
                " segmentation and retry join attempt [nodeId=" + node.id() + ']';

        if (errMsg != null)
            throw new PluginValidationException(errMsg, errMsg, node.id());
    }
}
