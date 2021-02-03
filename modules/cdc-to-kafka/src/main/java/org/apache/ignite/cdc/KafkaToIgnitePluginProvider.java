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

package org.apache.ignite.cdc;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Error message.
 */
public class KafkaToIgnitePluginProvider implements PluginProvider<PluginConfiguration> {
    /** Error message. */
    public static final String ERR_MSG = "Please, provide path to the plugin properties file in \"propertiesPath\" property.";

    /** Plugin. */
    private KafkaToIgnitePlugin plugin;

    /** Path to the plugin properties file. */
    private String propertiesPath;

    /** Cache namest to handle. */
    private String[] groups;

    /** {@inheritDoc} */
    @Override public String name() {
        return "CDCKafkaToIgnitePlugin";
    }

    /** {@inheritDoc} */
    @Override public String version() {
        return "0.0.1";
    }

    /** {@inheritDoc} */
    @Override public String copyright() {
        return "Sberbank Technology";
    }

    /** {@inheritDoc} */
    @Override public IgnitePlugin plugin() {
        return new IgnitePlugin() { /** */ };
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) throws IgniteCheckedException {
        try {
            Set<Integer> grps = Arrays.stream(requireNonNull(this.groups, "Please, provide group names to handle!"))
                .mapToInt(CU::cacheId)
                .boxed()
                .collect(Collectors.toSet());

            plugin = new KafkaToIgnitePlugin(ctx, Utils.properties(propertiesPath, ERR_MSG), grps);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStart() throws IgniteCheckedException {
        try {
            plugin.start();
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStop(boolean cancel) {
        plugin.stop();
    }

    /** @param propertiesPath Sets path to the property. */
    public void setPropertiesPath(String propertiesPath) {
        this.propertiesPath = propertiesPath;
    }

    /** @param groups Cache names to handle. */
    public void setGroups(String... groups) {
        this.groups = groups;
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
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
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
