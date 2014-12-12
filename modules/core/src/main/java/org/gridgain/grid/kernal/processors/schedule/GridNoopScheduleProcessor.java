/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.schedule;

import org.apache.ignite.*;
import org.apache.ignite.scheduler.*;
import org.gridgain.grid.kernal.*;

import java.util.concurrent.*;

/**
 * No-op implementation of {@link GridScheduleProcessorAdapter}, throws exception on usage attempt.
 */
public class GridNoopScheduleProcessor extends GridScheduleProcessorAdapter {
    /**
     * @param ctx Kernal context.
     */
    public GridNoopScheduleProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public SchedulerFuture<?> schedule(Runnable c, String pattern) {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Override public <R> SchedulerFuture<R> schedule(Callable<R> c, String pattern) {
        throw processorException();
    }

    /**
     * @return No-op processor usage exception;
     */
    private IgniteException processorException() {
        return new IgniteException("Current GridGain configuration does not support schedule functionality " +
            "(consider adding gridgain-schedule module to classpath).");
    }
}
