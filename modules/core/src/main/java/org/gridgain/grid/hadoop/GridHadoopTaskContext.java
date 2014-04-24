/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;

/**
 * Task context.
 */
public class GridHadoopTaskContext {
    /** Kernal context. */
    private GridKernalContext ctx;

    /**
     * @param ctx Kernal context.
     */
    public GridHadoopTaskContext(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /**
     * Gets task output.
     *
     * @return Task output.
     */
    public GridHadoopTaskOutput output() {
        return null;
    }

    /**
     * Gets task input.
     *
     * @return Task input.
     */
    public GridHadoopTaskInput input() {
        return null;
    }

    /**
     * Gets local grid instance.
     *
     * @return Grid instance.
     */
    public Grid grid() {
        return ctx.grid();
    }

    public GridHadoopJob job() {
        return null;
    }
}
