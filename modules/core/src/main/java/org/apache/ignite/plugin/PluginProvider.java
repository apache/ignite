// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.plugin;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Pluggable ignite component.
 *
 * @author @java.author
 * @version @java.version
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
     * @return Plugin API.
     */
    public <T extends IgnitePlugin> T plugin();

    /**
     * TODO remove.
     *
     * @param cls Ignite component class.
     * @return Ignite component or {@code null} if component is not supported.
     */
    @Nullable public <T> T createComponent(Class<T> cls);

    /**
     * Register extensions.
     * @param ctx Plugin context.
     * @param registry Extension registry.
     */
    public void initExtensions(PluginContext ctx, IgniteExtensionRegistry registry);

    /**
     * Starts grid component.
     *
     * @param ctx Plugin context.
     * @param attrs Attributes.
     * @throws IgniteCheckedException Throws in case of any errors.
     */
    public void start(PluginContext ctx, Map<String, Object> attrs) throws IgniteCheckedException;

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
    @Nullable public Object provideDiscoveryData(UUID nodeId);

    /**
     * Receives plugin discovery data object from remote nodes (called
     * on new node during discovery process). This data is provided by
     * {@link #provideDiscoveryData(UUID)} method on the other nodes.
     *
     * @param data Discovery data object or {@code null} if nothing was
     *      sent for this component.
     */
    public void receiveDiscoveryData(Object data);

    /**
     * Validates that new node can join grid topology, this method is called on coordinator
     * node before new node joins topology.
     *
     * @param node Joining node.
     * @throws PluginValidationException If cluster-wide plugin validation failed.
     */
    public void validateNewNode(ClusterNode node) throws PluginValidationException;
}
