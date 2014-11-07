/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.schedule;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.scheduler.*;

import java.util.concurrent.*;

/**
 * Schedules cron-based execution of grid tasks and closures. Abstract class was introduced to
 * avoid mandatory runtime dependency on cron library.
 */
public abstract class GridScheduleProcessorAdapter extends GridProcessorAdapter {
    /**
     * @param ctx Kernal context.
     */
    protected GridScheduleProcessorAdapter(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param c Closure to schedule to run as a background cron-based job.
     * @param pattern Scheduling pattern in UNIX cron format with prefix "{n1, n2} " where n1 is delay of scheduling
     *      and n2 is the number of task calls.
     * @return Descriptor of the scheduled execution.
     */
    public abstract GridSchedulerFuture<?> schedule(final Runnable c, String pattern);

    /**
     * @param c Closure to schedule to run as a background cron-based job.
     * @param pattern Scheduling pattern in UNIX cron format with prefix "{n1, n2} " where n1 is delay of scheduling
     *      and n2 is the number of task calls.
     * @return Descriptor of the scheduled execution.
     */
    public abstract <R> GridSchedulerFuture<R> schedule(Callable<R> c, String pattern);
}
