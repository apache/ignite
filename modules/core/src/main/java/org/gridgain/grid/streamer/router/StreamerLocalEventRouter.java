/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.streamer.router;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.streamer.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

/**
 * Local router. Always routes event to local node.
 */
public class StreamerLocalEventRouter implements StreamerEventRouter {
    /** Grid instance. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override public <T> ClusterNode route(StreamerContext ctx, String stageName, T evt) {
        return ignite.cluster().localNode();
    }

    /** {@inheritDoc} */
    @Override public <T> Map<ClusterNode, Collection<T>> route(StreamerContext ctx, String stageName,
        Collection<T> evts) {
        return F.asMap(ignite.cluster().localNode(), evts);
    }
}
