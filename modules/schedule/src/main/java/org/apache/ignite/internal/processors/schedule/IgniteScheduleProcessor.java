/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.schedule;

import it.sauronsoftware.cron4j.Scheduler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.scheduler.SchedulerFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Schedules cron-based execution of grid tasks and closures.
 */
public class IgniteScheduleProcessor extends IgniteScheduleProcessorAdapter {
    /** Cron scheduler. */
    private Scheduler sched;

    /** Schedule futures. */
    private Set<SchedulerFuture<?>> schedFuts = new GridConcurrentHashSet<>();

    /**
     * @param ctx Kernal context.
     */
    public IgniteScheduleProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public SchedulerFuture<?> schedule(final Runnable c, String ptrn) {
        assert c != null;
        assert ptrn != null;

        ScheduleFutureImpl<Object> fut = new ScheduleFutureImpl<>(sched, ctx, ptrn);

        fut.schedule(new IgniteCallable<Object>() {
            @Nullable @Override public Object call() {
                c.run();

                return null;
            }
        });

        return fut;
    }

    /** {@inheritDoc} */
    @Override public <R> SchedulerFuture<R> schedule(Callable<R> c, String pattern) {
        assert c != null;
        assert pattern != null;

        ScheduleFutureImpl<R> fut = new ScheduleFutureImpl<>(sched, ctx, pattern);

        fut.schedule(c);

        return fut;
    }

    /**
     *
     * @return Future objects of currently scheduled active(not finished) tasks.
     */
    public Collection<SchedulerFuture<?>> getScheduledFutures() {
        return Collections.unmodifiableList(new ArrayList<>(schedFuts));
    }

    /**
     * Removes future object from the collection of scheduled futures.
     *
     * @param fut Future object.
     */
    void onDescheduled(SchedulerFuture<?> fut) {
        assert fut != null;

        schedFuts.remove(fut);
    }

    /**
     * Adds future object to the collection of scheduled futures.
     *
     * @param fut Future object.
     */
    void onScheduled(SchedulerFuture<?> fut) {
        assert fut != null;

        schedFuts.add(fut);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        sched = new Scheduler();

        sched.start();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (sched.isStarted())
            sched.stop();

        sched = null;
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> Schedule processor memory stats [igniteInstanceName=" + ctx.igniteInstanceName() + ']');
        X.println(">>>   schedFutsSize: " + schedFuts.size());
    }
}