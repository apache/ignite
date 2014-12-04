/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testframework.junits;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Test context.
 */
public class GridTestKernalContext extends GridKernalContextImpl {
    /**
     *
     */
    public GridTestKernalContext() {
        super(null,
            new GridKernal(null),
            new IgniteConfiguration(),
            new GridKernalGatewayImpl(null),
            null,
            false);
    }

    /**
     * @param log Logger to use in context config.
     */
    public GridTestKernalContext(GridLogger log) {
        this();

        config().setGridLogger(log);
    }

    /**
     * Starts everything added (in the added order).
     *
     * @throws GridException If failed
     */
    public void start() throws GridException {
        for (GridComponent comp : this)
            comp.start();
    }

    /**
     * Stops everything added.
     *
     * @param cancel Cancel parameter.
     * @throws GridException If failed.
     */
    public void stop(boolean cancel) throws GridException {
        List<GridComponent> comps = components();

        for (ListIterator<GridComponent> it = comps.listIterator(comps.size()); it.hasPrevious();) {
            GridComponent comp = it.previous();

            comp.stop(cancel);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTestKernalContext.class, this, super.toString());
    }
}
