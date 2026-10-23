/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.managers.communication;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * The semaphore which releases the acquired permits when the configured period of time ends.
 * <p>
 * In the opposite to {@link Semaphore#acquire(int)} which acquires the given number of permits
 * from the semaphore and block method called until all permits are available, the TimedSemaphore
 * will allow to make a progress during 1-sec period configured amout of permits is not enough.
 */
public class TimedSemaphore {
    /** The constant which represents unlimited number of permits being acquired (value is {@code -1}). */
    public static final int UNLIMITED_PERMITS = -1;

    /** The time period to release all available permits when it ends. */
    private static final int DFLT_TIME_PERIOD_SEC = 1;

    /** The service to release permits during the configured time of time. */
    private final ScheduledExecutorService scheduler;

    /** A future object representing the timer task. */
    private ScheduledFuture<?> timerFut;

    /** The maximum number of permits available per second. */
    private long permitsPerSec;

    /** The current acquired permits during the configured period of time. */
    private int acquireCnt;

    /** If the semaphore has been shutdowned. */
    private boolean shutdown;

    /**
     * @param permitsPerSec The number of permits per second.
     */
    public TimedSemaphore(long permitsPerSec) {
        assert permitsPerSec >= 0;

        this.permitsPerSec = permitsPerSec;

        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

        this.scheduler = scheduler;
    }

    /**
     * @return The maximum number of available permits.
     */
    public synchronized long permitsPerSec() {
        return permitsPerSec;
    }

    /**
     * Sets the maximum number of available permits. In the other words, the number of times the
     * {@link TimedSemaphore#tryAcquire(int, int, TimeUnit)} method can be called within the given
     * time time without being blocked. To disbale the limit, set {@link #UNLIMITED_PERMITS}.
     *
     * @param permitsPerSec The maximum number of available permits per second.
     */
    public synchronized void permitsPerSec(final long permitsPerSec) {
        this.permitsPerSec = permitsPerSec;
    }

    /**
     * Stop the current semaphore.
     */
    public synchronized void shutdown() {
        if (!shutdown) {
            shutdown = true;

            scheduler.shutdownNow();

            timerFut = null;
        }
    }

    /**
     * This method will be blocked if the limit of permits for the current time period has been reached.
     * At the first call of this method the timer will be started to monitor permits during current period.
     *
     * @param permits The total number of permits to acquire.
     * @param timeout The maximum time to wait for the permits.
     * @param unit The time unit of the {@code timeout} argument.
     * @return {@code true} if all permits were acquired and {@code false} if the waiting time elapsed
     * before all permits were acquired.
     * @throws InterruptedException If the thread gets interrupted.
     */
    public synchronized boolean tryAcquire(int permits, int timeout, TimeUnit unit) throws InterruptedException {
        assert permits > 0;
        assert timeout >= 0;

        if (shutdown)
            throw new IgniteException("The semaphore has been stopped.");

        initTimePeriod();

        long waitMillis = unit.toMillis(timeout);
        long endTime = timeout == 0 ? Long.MAX_VALUE : U.currentTimeMillis() + waitMillis;

        for (int i = 0; i < permits; ) {
            if (endTime - U.currentTimeMillis() <= 0)
                return false;

            if (acquirePermit())
                i++;
            else
                wait();
        }

        return true;
    }

    /**
     * @return {@code true} if permit has been successfully acquired.
     */
    private boolean acquirePermit() {
        if (acquireCnt < permitsPerSec || UNLIMITED_PERMITS == permitsPerSec) {
            acquireCnt++;

            return true;
        }

        return false;
    }

    /**
     * Reset the permits counter and releases all the threads waiting for it.
     */
    synchronized void release() {
        acquireCnt = 0;

        notifyAll();
    }

    /**
     * @return The current number of acquired permits during this time.
     */
    public synchronized int acquireCnt() {
        return acquireCnt;
    }

    /**
     * @return The current number of available permits during the current time
     * or {@link #UNLIMITED_PERMITS} if there is to permits limit.
     */
    public synchronized long availablePermits() {
        return permitsPerSec == UNLIMITED_PERMITS ? permitsPerSec : permitsPerSec - acquireCnt;
    }

    /**
     * @return The future represens scheduled time timer.
     */
    synchronized ScheduledFuture<?> scheduleTimePeriod() {
        // The time time ends - release all permits.
        return scheduler.scheduleAtFixedRate(this::release,
            DFLT_TIME_PERIOD_SEC,
            DFLT_TIME_PERIOD_SEC,
            TimeUnit.SECONDS);
    }

    /**
     * Checks if the semaphore can be used and starts the internal process for time monitoring.
     */
    private void initTimePeriod() {
        if (timerFut == null)
            timerFut = scheduleTimePeriod();
    }
}
