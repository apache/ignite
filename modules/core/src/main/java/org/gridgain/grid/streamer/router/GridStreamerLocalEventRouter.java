/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.streamer.router;

import org.gridgain.grid.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.streamer.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

/**
 * Local router. Always routes event to local node.
 */
public class GridStreamerLocalEventRouter implements GridStreamerEventRouter {
    /** Grid instance. */
    @GridInstanceResource
    private Grid grid;

    /** {@inheritDoc} */
    @Override public <T> GridNode route(GridStreamerContext ctx, String stageName, T evt) {
        return grid.cluster().localNode();
    }

    /** {@inheritDoc} */
    @Override public <T> Map<GridNode, Collection<T>> route(GridStreamerContext ctx, String stageName,
        Collection<T> evts) {
        return F.asMap(grid.cluster().localNode(), evts);
    }
}
