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

package org.apache.ignite.internal.processor.security;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.jetbrains.annotations.Nullable;

/**
 * Security processor provider for tests.
 */
public class TestSecurityProcessorProvider implements PluginProvider {
    /** Default test security processor class name. */
    public static String DFLT_TEST_SECURITY_PROCESSOR_CLS_NAME =
        "org.apache.ignite.internal.processors.security.os.GridOsSecurityProcessor";

    /** {@inheritDoc} */
    @Override public String name() {
        return "TestSecurityProcessorProvider";
    }

    /** {@inheritDoc} */
    @Override public String version() {
        return "1.0";
    }

    /** {@inheritDoc} */
    @Override public String copyright() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgnitePlugin plugin() {
        return new IgnitePlugin() {
        };
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public @Nullable Object createComponent(PluginContext ctx, Class cls) {
        if (cls.isAssignableFrom(GridSecurityProcessor.class)) {
            String secProcCls = securityProcessorClass(ctx);

            try {
                Class implCls = Class.forName(secProcCls);

                if (implCls == null)
                    throw new IgniteException("Failed to find component implementation: " + cls.getName());

                if (!GridSecurityProcessor.class.isAssignableFrom(implCls))
                    throw new IgniteException("Component implementation does not implement component interface " +
                        "[component=" + cls.getName() + ", implementation=" + implCls.getName() + ']');

                Constructor constructor;

                try {
                    constructor = implCls.getConstructor(GridKernalContext.class);
                }
                catch (NoSuchMethodException e) {
                    throw new IgniteException("Component does not have expected constructor: " + implCls.getName(), e);
                }

                try {
                    return constructor.newInstance(((IgniteEx)ctx.grid()).context());
                }
                catch (ReflectiveOperationException e) {
                    throw new IgniteException("Failed to create component [component=" + cls.getName() +
                        ", implementation=" + implCls.getName() + ']', e);
                }
            }
            catch (ClassNotFoundException e) {
                throw new IgniteException("Failed to load class [cls=" + secProcCls + "]", e);
            }
        }

        return null;
    }

    /**
     * Getting security processor class name.
     *
     * @param ctx Context.
     */
    private String securityProcessorClass(PluginContext ctx){
        IgniteConfiguration igniteCfg = ctx.igniteConfiguration();

        if (igniteCfg.getPluginConfigurations() != null) {
            for (PluginConfiguration pluginCfg : igniteCfg.getPluginConfigurations()) {
                if (pluginCfg instanceof TestSecurityPluginConfiguration)
                    return ((TestSecurityPluginConfiguration)pluginCfg).getSecurityProcessorClass();
            }
        }

        return DFLT_TEST_SECURITY_PROCESSOR_CLS_NAME;
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
    @Override public @Nullable Serializable provideDiscoveryData(UUID nodeId) {
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
}
