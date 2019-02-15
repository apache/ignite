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

package org.apache.ignite.plugin;

import java.io.Serializable;
import java.util.ServiceLoader;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
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
    public void initExtensions(PluginContext ctx, ExtensionRegistry registry) throws IgniteCheckedException;

    /**
     * Creates Ignite component.
     *
     * @param ctx Plugin context.
     * @param cls Ignite component class.
     * @return Ignite component or {@code null} if component is not supported.
     */
    @Nullable public <T> T createComponent(PluginContext ctx, Class<T> cls);

    /**
     * Creates cache plugin provider.
     *
     * @return Cache plugin provider class.
     * @param ctx Plugin context.
     */
    public CachePluginProvider createCacheProvider(CachePluginContext ctx);

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
     *
     * @deprecated Use {@link #validateNewNode(ClusterNode, Serializable)} instead.
     */
    @Deprecated
    public void validateNewNode(ClusterNode node) throws PluginValidationException;

    /**
     * Validates that new node can join grid topology, this method is called on coordinator
     * node before new node joins topology.
     *
     * @param node Joining node.
     * @param data Discovery data object or {@code null} if nothing was
     * sent for this component.
     * @throws PluginValidationException If cluster-wide plugin validation failed.
     */
    public default void validateNewNode(ClusterNode node, Serializable data)  {
        validateNewNode(node);
    }
}