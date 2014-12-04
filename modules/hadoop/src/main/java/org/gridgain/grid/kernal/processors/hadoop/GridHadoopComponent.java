/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.ignite.*;
import org.gridgain.grid.*;

/**
 * Abstract class for all hadoop components.
 */
public abstract class GridHadoopComponent {
    /** Hadoop context. */
    protected GridHadoopContext ctx;

    /** Logger. */
    protected IgniteLogger log;

    /**
     * @param ctx Hadoop context.
     */
    public void start(GridHadoopContext ctx) throws GridException {
        this.ctx = ctx;

        log = ctx.kernalContext().log(getClass());
    }

    /**
     * Stops manager.
     */
    public void stop(boolean cancel) {
        // No-op.
    }

    /**
     * Callback invoked when all grid components are started.
     */
    public void onKernalStart() throws GridException {
        // No-op.
    }

    /**
     * Callback invoked before all grid components are stopped.
     */
    public void onKernalStop(boolean cancel) {
        // No-op.
    }
}
