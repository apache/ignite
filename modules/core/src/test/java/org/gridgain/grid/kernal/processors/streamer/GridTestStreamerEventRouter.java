/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.streamer;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.streamer.*;

import java.util.*;

/**
 * Test router.
 */
class GridTestStreamerEventRouter extends StreamerEventRouterAdapter {
    /** Route table. */
    private Map<String, UUID> routeTbl = new HashMap<>();

    /**
     * @param stageName Stage name.
     * @param nodeId Node id.
     */
    public void put(String stageName, UUID nodeId) {
        routeTbl.put(stageName, nodeId);
    }

    /** {@inheritDoc} */
    @Override public <T> ClusterNode route(StreamerContext ctx, String stageName, T evt) {
        UUID nodeId = routeTbl.get(stageName);

        if (nodeId == null)
            return null;

        return ctx.projection().node(nodeId);
    }
}
