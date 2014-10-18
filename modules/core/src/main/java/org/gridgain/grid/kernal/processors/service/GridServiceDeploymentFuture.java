// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.service.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Service deployment future.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridServiceDeploymentFuture extends GridFutureAdapter<Object> {
    /** */
    private final GridServiceConfiguration cfg;

    /**
     * @param ctx Context.
     * @param cfg Configuration.
     */
    public GridServiceDeploymentFuture(GridKernalContext ctx, GridServiceConfiguration cfg) {
        super(ctx);

        this.cfg = cfg;
    }

    /**
     * @return Service configuration.
     */
    GridServiceConfiguration configuration() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridServiceDeploymentFuture.class, this);
    }
}
