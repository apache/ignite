// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design.plugin;

import org.gridgain.grid.*;
import org.gridgain.grid.design.*;
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
     * Starts grid component.
     *
     * @throws IgniteException Throws in case of any errors.
     */
    public void start(PluginContext ctx, C cfg) throws IgniteException;

    /**
     * Stops grid component.
     *
     * @param cancel If {@code true}, then all ongoing tasks or jobs for relevant
     *      components need to be cancelled.
     * @throws IgniteException Thrown in case of any errors.
     */
    public void stop(boolean cancel) throws IgniteException;

    /**
     * Callback that notifies that Ignite has successfully started,
     * including all internal components.
     *
     * @throws IgniteException Thrown in case of any errors.
     */
    public void onIgniteStart() throws IgniteException;

    /**
     * Callback to notify that Ignite is about to stop.
     *
     * @param cancel Flag indicating whether jobs should be canceled.
     */
    public void onIgniteStop(boolean cancel);

    /**
     * Gets discovery data object that will be sent to new node
     * during discovery process.
     *
     * @param nodeId ID of new node that joins topology.
     * @return Discovery data object or {@code null} if there is nothing
     *      to send for this component.
     */
    @Nullable public Object collectDiscoveryData(UUID nodeId);

    /**
     * Receives discovery data object from remote nodes (called
     * on new node during discovery process).
     *
     * @param data Discovery data object or {@code null} if nothing was
     *      sent for this component.
     */
    public void onDiscoveryDataReceived(Object data);

    /**
     * Prints memory statistics (sizes of internal structures, etc.).
     *
     * NOTE: this method is for testing and profiling purposes only.
     */
    public void printMemoryStats();

    /**
     * Validates that new node can join grid topology, this method is called on coordinator
     * node before new node joins topology.
     *
     * @param node Joining node.
     * @throws PluginValidationException If cluster-wide plugin validation failed.
     */
    public void validateNode(GridNode node) throws PluginValidationException;
}
