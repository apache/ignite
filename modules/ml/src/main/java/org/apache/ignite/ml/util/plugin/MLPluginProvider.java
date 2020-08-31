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

package org.apache.ignite.ml.util.plugin;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.ml.inference.storage.descriptor.ModelDescriptorStorageFactory;
import org.apache.ignite.ml.inference.storage.model.ModelStorageFactory;
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
 * Machine learning inference plugin provider.
 */
public class MLPluginProvider implements PluginProvider<MLPluginConfiguration> {
    /** Plugin name. */
    private static final String ML_INFERENCE_PLUGIN_NAME = "ml-inference-plugin";

    /** Plugin version/ */
    private static final String ML_INFERENCE_PLUGIN_VERSION = "1.0.0";

    /** Default number of model storage backups. */
    private static final int MODEL_STORAGE_DEFAULT_BACKUPS = 1;

    /** Default number of model descriptor storage backups. */
    private static final int MODEL_DESCRIPTOR_STORAGE_DEFAULT_BACKUPS = 1;

    /** Plugin configuration. */
    private MLPluginConfiguration cfg;

    /** Ignite instance. */
    private Ignite ignite;

    /** Ignite logger. */
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public String name() {
        return ML_INFERENCE_PLUGIN_NAME;
    }

    /** {@inheritDoc} */
    @Override public String version() {
        return ML_INFERENCE_PLUGIN_VERSION;
    }

    /** {@inheritDoc} */
    @Override public String copyright() {
        return null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T extends IgnitePlugin> T plugin() {
        return (T)new MLPlugin();
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
        IgniteConfiguration igniteCfg = ctx.igniteConfiguration();

        this.ignite = ctx.grid();
        this.log = ctx.log(this.getClass());

        if (igniteCfg.getPluginConfigurations() != null) {
            for (PluginConfiguration pluginCfg : igniteCfg.getPluginConfigurations()) {
                if (pluginCfg instanceof MLPluginConfiguration) {
                    cfg = (MLPluginConfiguration)pluginCfg;
                    break;
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void start(PluginContext ctx) {
        // Do nothing.
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        // Do nothing.
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStart() {
        if (ignite == null || log == null)
            throw new RuntimeException("Plugin provider has not been initialized");

        if (cfg != null) {
            if (cfg.isWithMdlStorage())
                startModelStorage(cfg);

            if (cfg.isWithMdlDescStorage())
                startModelDescriptorStorage(cfg);
        }
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStop(boolean cancel) {

    }

    /** {@inheritDoc} */
    @Nullable @Override public Serializable provideDiscoveryData(UUID nodeId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void receiveDiscoveryData(UUID nodeId, Serializable data) {
        // Do nothing.
    }

    /** {@inheritDoc} */
    @Override public void validateNewNode(ClusterNode node) throws PluginValidationException {
        // Do nothing.
    }

    /**
     * Starts model storage.
     */
    private void startModelStorage(MLPluginConfiguration cfg) {
        CacheConfiguration<String, byte[]> storageCfg = new CacheConfiguration<>();

        storageCfg.setName(ModelStorageFactory.MODEL_STORAGE_CACHE_NAME);
        storageCfg.setCacheMode(CacheMode.PARTITIONED);
        storageCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        if (cfg.getMdlStorageBackups() == null)
            storageCfg.setBackups(MODEL_STORAGE_DEFAULT_BACKUPS);
        else
            storageCfg.setBackups(cfg.getMdlStorageBackups());

        ignite.getOrCreateCache(storageCfg);

        if (log.isInfoEnabled())
            log.info("ML model storage is ready");
    }

    /**
     * Starts model descriptor storage.
     */
    private void startModelDescriptorStorage(MLPluginConfiguration cfg) {
        CacheConfiguration<String, byte[]> storageCfg = new CacheConfiguration<>();

        storageCfg.setName(ModelDescriptorStorageFactory.MODEL_DESCRIPTOR_STORAGE_CACHE_NAME);
        storageCfg.setCacheMode(CacheMode.PARTITIONED);
        storageCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        if (cfg.getMdlDescStorageBackups() == null)
            storageCfg.setBackups(MODEL_DESCRIPTOR_STORAGE_DEFAULT_BACKUPS);

        ignite.getOrCreateCache(storageCfg);

        if (log.isInfoEnabled())
            log.info("ML model descriptor storage is ready");
    }
}
