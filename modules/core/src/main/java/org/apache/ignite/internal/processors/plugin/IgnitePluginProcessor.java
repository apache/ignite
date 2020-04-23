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

package org.apache.ignite.internal.processors.plugin;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridPluginContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.Extension;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoveryDataBag.GridDiscoveryData;
import org.apache.ignite.spi.discovery.DiscoveryDataBag.JoiningNodeDiscoveryData;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.PLUGIN;

/**
 *
 */
public class IgnitePluginProcessor extends GridProcessorAdapter {
    /** */
    private final Map<String, PluginProvider> plugins = new LinkedHashMap<>();

    /** */
    private final Map<PluginProvider, GridPluginContext> pluginCtxMap = new IdentityHashMap<>();

    /** */
    private volatile Map<Class<?>, Object[]> extensions;

    /**
     *
     * @param ctx Kernal context.
     * @param cfg Ignite configuration.
     * @param providers Plugin providers.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    public IgnitePluginProcessor(GridKernalContext ctx, IgniteConfiguration cfg, List<PluginProvider> providers)
        throws IgniteCheckedException {
        super(ctx);

        ExtensionRegistryImpl registry = new ExtensionRegistryImpl();

        for (PluginProvider provider : providers) {
            GridPluginContext pluginCtx = new GridPluginContext(ctx, cfg);

            if (F.isEmpty(provider.name()))
                throw new IgniteException("Plugin name can not be empty.");

            if (plugins.containsKey(provider.name()))
                throw new IgniteException("Duplicated plugin name: " + provider.name());

            plugins.put(provider.name(), provider);

            pluginCtxMap.put(provider, pluginCtx);

            provider.initExtensions(pluginCtx, registry);

            if (provider.plugin() == null)
                throw new IgniteException("Plugin is null.");
        }

        extensions = registry.createExtensionMap();
    }

    /**
     * @param extensionItf Extension interface class.
     * @return Returns implementation for provided extension from all plugins.
     */
    @Nullable public <T extends Extension> T[] extensions(Class<T> extensionItf) {
        Map<Class<?>, Object[]> extensions = this.extensions;

        return (T[])extensions.get(extensionItf);
    }

    /**
     * @param name Plugin name.
     * @return Plugin provider.
     */
    @Nullable public <T extends PluginProvider> T pluginProvider(String name) {
        return (T)plugins.get(name);
    }

    /**
     * @return All plugin providers.
     */
    public Collection<PluginProvider> allProviders() {
        return plugins.values();
    }

    /**
     * @param provider Plugin context.
     * @return Plugin context.
     */
    public <T extends PluginContext> T pluginContextForProvider(PluginProvider provider) {
        return (T)pluginCtxMap.get(provider);
    }

    /**
     * @param cls Component class.
     * @param <T> Component type.
     * @return Component class instance or {@code null} if no one plugin override this component.
     */
    public <T> T createComponent(Class<T> cls) {
        for (PluginProvider plugin : plugins.values()) {
            PluginContext ctx = pluginContextForProvider(plugin);

            T comp = (T)plugin.createComponent(ctx, cls);

            if (comp != null)
                return comp;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ackPluginsInfo();
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryDataExchangeType discoveryDataType() {
        return PLUGIN;
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        Serializable pluginsData = getDiscoveryData(dataBag.joiningNodeId());

        if (pluginsData != null)
            dataBag.addJoiningNodeData(PLUGIN.ordinal(), pluginsData);
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        Serializable pluginsData = getDiscoveryData(dataBag.joiningNodeId());

        if (pluginsData != null)
            dataBag.addNodeSpecificData(PLUGIN.ordinal(), pluginsData);
    }

    /**
     * @param joiningNodeId Joining node id.
     */
    private Serializable getDiscoveryData(UUID joiningNodeId) {
        HashMap<String, Serializable> pluginsData = null;

        for (Map.Entry<String, PluginProvider> e : plugins.entrySet()) {
            Serializable data = e.getValue().provideDiscoveryData(joiningNodeId);

            if (data != null) {
                if (pluginsData == null)
                    pluginsData = new HashMap<>();

                pluginsData.put(e.getKey(), data);
            }
        }

        return pluginsData;
    }

    /** {@inheritDoc} */
    @Override public void onJoiningNodeDataReceived(JoiningNodeDiscoveryData data) {
        if (data.hasJoiningNodeData()) {
            Map<String, Serializable> pluginsData = (Map<String, Serializable>) data.joiningNodeData();

            applyPluginsData(data.joiningNodeId(), pluginsData);
        }
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(GridDiscoveryData data) {
        Map<UUID, Serializable> nodeSpecificData = data.nodeSpecificData();

        if (nodeSpecificData != null) {
            UUID joiningNodeId = data.joiningNodeId();

            for (Serializable v : nodeSpecificData.values()) {
                if (v != null) {
                    Map<String, Serializable> pluginsData = (Map<String, Serializable>) v;

                    applyPluginsData(joiningNodeId, pluginsData);
                }
            }
        }
    }

    /**
     * @param nodeId Node id.
     * @param pluginsData Plugins data.
     */
    private void applyPluginsData(UUID nodeId, Map<String, Serializable> pluginsData) {
        for (Map.Entry<String, Serializable> e : pluginsData.entrySet()) {
            PluginProvider provider = plugins.get(e.getKey());

            if (provider != null)
                provider.receiveDiscoveryData(nodeId, e.getValue());
            else
                U.warn(log, "Received discovery data for unknown plugin: " + e.getKey());
        }
    }

    /**
     * Print plugins information.
     */
    private void ackPluginsInfo() {
        U.quietAndInfo(log, "Configured plugins:");

        if (plugins.isEmpty()) {
            U.quietAndInfo(log, "  ^-- None");
            U.quietAndInfo(log, "");
        }
        else {
            for (PluginProvider plugin : plugins.values()) {
                U.quietAndInfo(log, "  ^-- " + plugin.name() + " " + plugin.version());
                U.quietAndInfo(log, "  ^-- " + plugin.copyright());
                U.quietAndInfo(log, "");
            }
        }
    }

    /**
     *
     */
    private static class ExtensionRegistryImpl implements ExtensionRegistry {
        /** */
        private final Map<Class<?>, List<Object>> extensionsCollector = new HashMap<>();

        /** {@inheritDoc} */
        @Override public <T extends Extension> void registerExtension(Class<T> extensionItf, T extensionImpl) {
            List<Object> list = extensionsCollector.get(extensionItf);

            if (list == null) {
                list = new ArrayList<>();

                extensionsCollector.put(extensionItf, list);
            }

            list.add(extensionImpl);
        }

        /**
         * @return Map extension interface to array of implementation.
         */
        Map<Class<?>, Object[]> createExtensionMap() {
            Map<Class<?>, Object[]> extensions = new HashMap<>(extensionsCollector.size() * 2, 0.5f);

            for (Map.Entry<Class<?>, List<Object>> entry : extensionsCollector.entrySet()) {
                Class<?> extensionItf = entry.getKey();

                List<Object> implementations = entry.getValue();

                Object[] implArr = (Object[])Array.newInstance(extensionItf, implementations.size());

                implArr = implementations.toArray(implArr);

                extensions.put(extensionItf, implArr);
            }

            return extensions;
        }
    }
}
