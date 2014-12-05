/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.cluster.*;
import org.apache.ignite.plugin.*;
import org.gridgain.grid.*;
import org.gridgain.grid.spi.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 *
 */
public class GridPluginComponent implements GridComponent {
    /** */
    private final PluginProvider plugin;

    /**
     * @param plugin Plugin provider.
     */
    public GridPluginComponent(PluginProvider plugin) {
        this.plugin = plugin;
    }

    /**
     * @return Plugin instance.
     */
    public PluginProvider plugin() {
        return plugin;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void start() throws GridException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws GridException {
        plugin.stop(cancel);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
        plugin.onIgniteStart();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        plugin.onIgniteStop(cancel);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object collectDiscoveryData(UUID nodeId) {
        return plugin.provideDiscoveryData(nodeId);
    }

    /** {@inheritDoc} */
    @Override public void onDiscoveryDataReceived(Object data) {
        plugin.receiveDiscoveryData(data);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteSpiNodeValidationResult validateNode(ClusterNode node) {
        try {
            plugin.validateNewNode(node);

            return null;
        }
        catch (PluginValidationException e) {
            return new IgniteSpiNodeValidationResult(e.nodeId(), e.getMessage(), e.remoteMessage());
        }
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        // No-op.
    }
}
