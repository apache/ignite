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

package org.apache.ignite.plugin;

import java.io.Serializable;
import java.util.ServiceLoader;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 * Pluggable Ignite component.
 * <p>
 * Ignite plugins are loaded using JDK {@link ServiceLoader}.
 * First method called to initialize plugin is {@link PluginProvider#initExtensions(PluginContext, ExtensionRegistry)}.
 * If plugin requires configuration it can be set in {@link IgniteConfiguration} using
 * {@link IgniteConfiguration#setPluginConfigurations(PluginConfiguration...)}.
 *
 * @see IgniteConfiguration#setPluginConfigurations(PluginConfiguration...)
 * @see PluginContext
 */
public interface PluginProvider<C extends PluginConfiguration> {
    /**
     * @return Plugin name.
     */
    public String name();

    /**
     * @return Plugin version.
     */
    public String version();

    /**
     * @return Copyright.
     */
    public String copyright();

    /**
     * @return Plugin API.
     */
    public <T extends IgnitePlugin> T plugin();

    /**
     * Registers extensions.
     *
     * @param ctx Plugin context.
     * @param registry Extension registry.
     */
    public void initExtensions(PluginContext ctx, ExtensionRegistry registry);

    /**
     * Creates Ignite component.
     *
     * @param ctx Plugin context.
     * @param cls Ignite component class.
     * @return Ignite component or {@code null} if component is not supported.
     */
    @Nullable public <T> T createComponent(PluginContext ctx, Class<T> cls);

    /**
     * Starts grid component.
     *
     * @param ctx Plugin context.
     * @throws IgniteCheckedException Throws in case of any errors.
     */
    public void start(PluginContext ctx) throws IgniteCheckedException;

    /**
     * Stops grid component.
     *
     * @param cancel If {@code true}, then all ongoing tasks or jobs for relevant
     *      components need to be cancelled.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void stop(boolean cancel) throws IgniteCheckedException;

    /**
     * Callback that notifies that Ignite has successfully started,
     * including all internal components.
     *
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void onIgniteStart() throws IgniteCheckedException;

    /**
     * Callback to notify that Ignite is about to stop.
     *
     * @param cancel Flag indicating whether jobs should be canceled.
     */
    public void onIgniteStop(boolean cancel);

    /**
     * Gets plugin discovery data object that will be sent to the new node
     * during discovery process.
     *
     * @param nodeId ID of new node that joins topology.
     * @return Discovery data object or {@code null} if there is nothing
     *      to send for this component.
     */
    @Nullable public Serializable provideDiscoveryData(UUID nodeId);

    /**
     * Receives plugin discovery data object from remote nodes (called
     * on new node during discovery process). This data is provided by
     * {@link #provideDiscoveryData(UUID)} method on the other nodes.
     *
     * @param nodeId Remote node ID.
     * @param data Discovery data object or {@code null} if nothing was
     *      sent for this component.
     */
    public void receiveDiscoveryData(UUID nodeId, Serializable data);

    /**
     * Validates that new node can join grid topology, this method is called on coordinator
     * node before new node joins topology.
     *
     * @param node Joining node.
     * @throws PluginValidationException If cluster-wide plugin validation failed.
     */
    public void validateNewNode(ClusterNode node) throws PluginValidationException;
}