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

package org.apache.ignite.internal.processors.rest;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Tests REST processor configuration via Ignite plugins functionality.
 */
public class RestProcessorInitializationTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testDefaultRestProcessorInitialization() throws Exception {
        IgniteEx ignite = startGrid(0);

        assertEquals(ignite.context().rest().getClass(), GridRestProcessor.class);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testCustomRestProcessorInitialization() throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));

        cfg.setPluginProviders(new TestRestProcessorProvider());

        IgniteEx ignite = startGrid(cfg);

        assertEquals(ignite.context().rest().getClass(), TestGridRestProcessorImpl.class);
    }

    /**
     * Test implementation of {@link PluginProvider} for obtaining {@link TestGridRestProcessorImpl}.
     */
    private static class TestRestProcessorProvider implements PluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "TEST_REST_PROCESSOR";
        }

        /** {@inheritDoc} */
        @Override public String version() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String copyright() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
            // No-op.
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
        @Override public void onIgniteStart() {
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
            if (cls.equals(IgniteRestProcessor.class))
                return new TestGridRestProcessorImpl(((IgniteEx)ctx.grid()).context());

            return null;
        }

        /** {@inheritDoc} */
        @Override public IgnitePlugin plugin() {
            return new IgnitePlugin() {
                // No-op.
            };
        }
    }

    /**
     * Test no-op implementation of {@link IgniteRestProcessor}.
     */
    private static class TestGridRestProcessorImpl extends GridProcessorAdapter implements IgniteRestProcessor {
        /**
         * @param ctx Kernal context.
         */
        protected TestGridRestProcessorImpl(GridKernalContext ctx) {
            super(ctx);
        }
    }
}
