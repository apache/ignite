// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.metrics;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.*;
import org.gridgain.grid.spi.metrics.*;

/**
 * This class defines a grid local metric manager.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridLocalMetricsManager extends GridManagerAdapter<GridLocalMetricsSpi> {
    /**
     * @param ctx Grid kernal context.
     */
    public GridLocalMetricsManager(GridKernalContext ctx) {
        super(ctx, ctx.config().getMetricsSpi());
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        if (ctx.config().isDaemon())
            return;

        startSpi();

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws GridException {
        if (ctx.config().isDaemon())
            return;

        stopSpi();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * Gets local VM metrics.
     *
     * @return Local VM metrics.
     */
    public GridLocalMetrics metrics() {
        return getSpi().getMetrics();
    }
}
