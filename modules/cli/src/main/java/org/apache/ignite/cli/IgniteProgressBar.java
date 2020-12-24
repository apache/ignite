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

package org.apache.ignite.cli;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import me.tongfei.progressbar.ConsoleProgressBarConsumer;
import me.tongfei.progressbar.IgniteProgressBarRenderer;
import me.tongfei.progressbar.ProgressBar;

/**
 * Basic implementation of a progress bar. Based on
 * <a href="https://github.com/ctongfei/progressbar">https://github.com/ctongfei/progressbar</a>.
 */
public class IgniteProgressBar implements AutoCloseable {
    private final ProgressBar impl;

    private ScheduledExecutorService exec;

    /**
     * Creates a new progress bar.
     *
     * @param initialMax Initial maximum number of steps.
     */
    public IgniteProgressBar(long initialMax) {
        impl = new ProgressBar(
            null,
            initialMax,
            10,
            0,
            Duration.ZERO,
            new IgniteProgressBarRenderer(),
            new ConsoleProgressBarConsumer(System.out, 150)
        );
    }

    /**
     * Performs a single step.
     */
    public void step() {
        impl.step();
    }

    /**
     * Performs a single step every N milliseconds.
     *
     * @param interval Interval in milliseconds.
     */
    public synchronized void stepPeriodically(long interval) {
        if (exec == null)
            exec = Executors.newSingleThreadScheduledExecutor();

        exec.scheduleAtFixedRate(impl::step, interval, interval, TimeUnit.MILLISECONDS);
    }

    /**
     * Updates maximum number of steps.
     *
     * @param newMax New maximum.
     */
    public void setMax(long newMax) {
        impl.maxHint(newMax);
    }

    @Override public void close() {
        while (impl.getCurrent() < impl.getMax()) {
            try {
                Thread.sleep(10);
            }
            catch (InterruptedException ignored) {
                break;
            }

            step();
        }

        impl.close();
    }
}
