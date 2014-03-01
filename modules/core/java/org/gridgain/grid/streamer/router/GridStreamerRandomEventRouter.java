/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.streamer.router;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.streamer.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Random router. Routes event to random node.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridStreamerRandomEventRouter extends GridStreamerEventRouterAdapter {
    /** Optional predicates to exclude nodes from routing. */
    private GridPredicate<GridNode>[] predicates;

    /**
     * Empty constructor for spring.
     */
    public GridStreamerRandomEventRouter() {
        this((GridPredicate<GridNode>[])null);
    }

    /**
     * Constructs random event router with optional set of filters to apply to streamer projection.
     *
     * @param predicates Node predicates.
     */
    public GridStreamerRandomEventRouter(@Nullable GridPredicate<GridNode>... predicates) {
        this.predicates = predicates;
    }

    /**
     * Constructs random event router with optional set of filters to apply to streamer projection.
     *
     * @param predicates Node predicates.
     */
    @SuppressWarnings("unchecked")
    public GridStreamerRandomEventRouter(Collection<GridPredicate<GridNode>> predicates) {
        if (!F.isEmpty(predicates)) {
            this.predicates = new GridPredicate[predicates.size()];

            predicates.toArray(this.predicates);
        }
    }

    /** {@inheritDoc} */
    @Override public GridNode route(GridStreamerContext ctx, String stageName, Object evt) {
        Collection<GridNode> nodes = F.view(ctx.projection().nodes(), predicates);

        int idx = ThreadLocalRandom8.current().nextInt(nodes.size());

        int i = 0;

        Iterator<GridNode> iter = nodes.iterator();

        while (true) {
            if (!iter.hasNext())
                iter = nodes.iterator();

            GridNode node = iter.next();

            if (idx == i++)
                return node;
        }
    }
}
