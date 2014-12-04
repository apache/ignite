/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.spi.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * No-op {@link GridManager} adapter.
 */
public class GridNoopManagerAdapter implements GridManager {
    /**
     * @param ctx Kernal context.
     */
    public GridNoopManagerAdapter(GridKernalContext ctx) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void addSpiAttributes(Map<String, Object> attrs) throws GridException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws GridException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object collectDiscoveryData(UUID nodeId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void onDiscoveryDataReceived(Object data) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridNodeValidationResult validateNode(ClusterNode node) {
        return null;
    }
}
