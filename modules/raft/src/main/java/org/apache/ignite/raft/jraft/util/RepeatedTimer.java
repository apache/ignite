/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.raft.jraft.util.timer.HashedWheelTimer;
import org.apache.ignite.raft.jraft.util.timer.Timeout;
import org.apache.ignite.raft.jraft.util.timer.Timer;
import org.apache.ignite.raft.jraft.util.timer.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Repeatable timer based on java.util.Timer.
 */
public abstract class RepeatedTimer implements Describer {

    public static final Logger LOG = LoggerFactory.getLogger(RepeatedTimer.class);

    private final Lock lock = new ReentrantLock();
    private final Timer timer;
    private Timeout timeout;
    private boolean stopped;
    private volatile boolean running;
    private volatile boolean destroyed;
    private volatile boolean invoking;
    private volatile int timeoutMs;
    private final String name;

    public int getTimeoutMs() {
        return this.timeoutMs;
    }

    public RepeatedTimer(final String name, final int timeoutMs) {
        this(name, timeoutMs, new HashedWheelTimer(new NamedThreadFactory(name, true), 1, TimeUnit.MILLISECONDS, 2048));
    }

    public RepeatedTimer(final String name, final int timeoutMs, final Timer timer) {
        super();
        this.name = name;
        this.timeoutMs = timeoutMs;
        this.stopped = true;
        this.timer = Requires.requireNonNull(timer, "timer");
    }

    /**
     * Subclasses should implement this method for timer trigger.
     */
    protected abstract void onTrigger();

    /**
     * Adjust timeoutMs before every scheduling.
     *
     * @param timeoutMs timeout millis
     * @return timeout millis
     */
    protected int adjustTimeout(final int timeoutMs) {
        return timeoutMs;
    }

    public void run() {
        this.invoking = true;
        try {
            onTrigger();
        }
        catch (final Throwable t) {
            LOG.error("Run timer failed.", t);
        }
        boolean invokeDestroyed = false;
        this.lock.lock();
        try {
            this.invoking = false;
            if (this.stopped) {
                this.running = false;
                invokeDestroyed = this.destroyed;
            }
            else {
                this.timeout = null;
                schedule();
            }
        }
        finally {
            this.lock.unlock();
        }
        if (invokeDestroyed) {
            onDestroy();
        }
    }

    /**
     * Run the timer at once, it will cancel the timer and re-schedule it.
     */
    public void runOnceNow() {
        this.lock.lock();
        try {
            if (this.timeout != null && this.timeout.cancel()) {
                this.timeout = null;
                run();
            }
        }
        finally {
            this.lock.unlock();
        }
    }

    /**
     * Called after destroy timer.
     */
    protected void onDestroy() {
        LOG.info("Destroy timer: {}.", this);
    }

    /**
     * Start the timer.
     */
    public void start() {
        this.lock.lock();
        try {
            if (this.destroyed) {
                return;
            }
            if (!this.stopped) {
                return;
            }
            this.stopped = false;
            if (this.running) {
                return;
            }
            this.running = true;
            schedule();
        }
        finally {
            this.lock.unlock();
        }
    }

    /**
     * Restart the timer. It will be started if it's stopped, and it will be restarted if it's running.
     */
    public void restart() {
        this.lock.lock();
        try {
            if (this.destroyed) {
                return;
            }
            this.stopped = false;
            this.running = true;
            schedule();
        }
        finally {
            this.lock.unlock();
        }
    }

    private void schedule() {
        if (this.timeout != null) {
            this.timeout.cancel();
        }
        final TimerTask timerTask = timeout -> {
            try {
                RepeatedTimer.this.run();
            }
            catch (final Throwable t) {
                LOG.error("Run timer task failed, taskName={}.", RepeatedTimer.this.name, t);
            }
        };
        this.timeout = this.timer.newTimeout(timerTask, adjustTimeout(this.timeoutMs), TimeUnit.MILLISECONDS);
    }

    /**
     * Reset timer with new timeoutMs.
     *
     * @param timeoutMs timeout millis
     */
    public void reset(final int timeoutMs) {
        this.lock.lock();
        this.timeoutMs = timeoutMs;
        try {
            if (this.stopped) {
                return;
            }
            if (this.running) {
                schedule();
            }
        }
        finally {
            this.lock.unlock();
        }
    }

    /**
     * Reset timer with current timeoutMs
     */
    public void reset() {
        this.lock.lock();
        try {
            reset(this.timeoutMs);
        }
        finally {
            this.lock.unlock();
        }
    }

    /**
     * Destroy timer
     */
    public void destroy() {
        boolean invokeDestroyed = false;
        this.lock.lock();
        try {
            if (this.destroyed) {
                return;
            }
            this.destroyed = true;
            if (!this.running) {
                invokeDestroyed = true;
            }
            if (this.stopped) {
                return;
            }
            this.stopped = true;
            if (this.timeout != null) {
                if (this.timeout.cancel()) {
                    invokeDestroyed = true;
                    this.running = false;
                }
                this.timeout = null;
            }
        }
        finally {
            this.lock.unlock();
            this.timer.stop();
            if (invokeDestroyed) {
                onDestroy();
            }
        }
    }

    /**
     * Stop timer
     */
    public void stop() {
        this.lock.lock();
        try {
            if (this.stopped) {
                return;
            }
            this.stopped = true;
            if (this.timeout != null) {
                this.timeout.cancel();
                this.running = false;
                this.timeout = null;
            }
        }
        finally {
            this.lock.unlock();
        }
    }

    @Override
    public void describe(final Printer out) {
        final String _describeString;
        this.lock.lock();
        try {
            _describeString = toString();
        }
        finally {
            this.lock.unlock();
        }
        out.print("  ") //
            .println(_describeString);
    }

    @Override
    public String toString() {
        return "RepeatedTimer{" + "timeout=" + this.timeout + ", stopped=" + this.stopped + ", running=" + this.running
            + ", destroyed=" + this.destroyed + ", invoking=" + this.invoking + ", timeoutMs=" + this.timeoutMs
            + ", name='" + this.name + '\'' + '}';
    }
}
