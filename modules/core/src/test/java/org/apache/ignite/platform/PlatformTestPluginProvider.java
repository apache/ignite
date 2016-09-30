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

package org.apache.ignite.platform;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.UUID;

/**
 * Test plugin provider.
 */
public class PlatformTestPluginProvider implements PluginProvider<PluginConfiguration> {
    @Override public String name() {
        return "PlatformTestPlugin";
    }

    @Override public String version() {
        return "1.2.3";
    }

    @Override public String copyright() {
        return "copyleft";
    }

    @Override public <T extends IgnitePlugin> T plugin() {
        return (T)new PlatformTestPlugin();
    }

    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {

    }

    @Nullable @Override public <T> T createComponent(PluginContext ctx, Class<T> cls) {
        return null;
    }

    @Override public void start(PluginContext ctx) throws IgniteCheckedException {

    }

    @Override public void stop(boolean cancel) throws IgniteCheckedException {

    }

    @Override public void onIgniteStart() throws IgniteCheckedException {

    }

    @Override public void onIgniteStop(boolean cancel) {

    }

    @Nullable @Override public Serializable provideDiscoveryData(UUID nodeId) {
        return null;
    }

    @Override public void receiveDiscoveryData(UUID nodeId, Serializable data) {

    }

    @Override public void validateNewNode(ClusterNode node) throws PluginValidationException {

    }

    public static class PlatformTestPlugin implements IgnitePlugin {

    }
}
