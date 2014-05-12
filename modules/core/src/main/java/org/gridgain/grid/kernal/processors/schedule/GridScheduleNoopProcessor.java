/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.schedule;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.scheduler.*;

import java.util.concurrent.*;

/**
 * No-op implementation of {@link GridScheduleProcessorAdapter}, throws exception on usage attempt.
 */
public class GridScheduleNoopProcessor extends GridScheduleProcessorAdapter {
    /**
     * @param ctx Kernal context.
     */
    public GridScheduleNoopProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public GridSchedulerFuture<?> schedule(Runnable c, String pattern) {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Override public <R> GridSchedulerFuture<R> schedule(Callable<R> c, String pattern) {
        throw processorException();
    }

    /**
     * @return No-op processor usage exception;
     */
    private GridRuntimeException processorException() {
        // TODO 8242.
        throw new GridRuntimeException("No-op processor does not support scheduling.");
    }
}
