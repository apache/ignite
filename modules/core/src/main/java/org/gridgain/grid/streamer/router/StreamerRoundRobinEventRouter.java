/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.streamer.router;

import org.apache.ignite.cluster.*;
import org.apache.ignite.streamer.*;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Round robin router.
 */
public class StreamerRoundRobinEventRouter extends StreamerEventRouterAdapter {
    /** */
    private final AtomicLong lastOrder = new AtomicLong();

    /** {@inheritDoc} */
    @Override public ClusterNode route(StreamerContext ctx, String stageName, Object evt) {
        Collection<ClusterNode> nodes = ctx.projection().nodes();

        int idx = (int)(lastOrder.getAndIncrement() % nodes.size());

        int i = 0;

        Iterator<ClusterNode> iter = nodes.iterator();

        while (true) {
            if (!iter.hasNext())
                iter = nodes.iterator();

            ClusterNode node = iter.next();

            if (idx == i++)
                return node;
        }
    }
}
