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

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import org.apache.ignite.lang.IgniteLogger;

/**
 * A {@link java.util.concurrent.ThreadPoolExecutor} that can additionally schedule commands to run after a given delay
 * with a logger witch can print error message for failed execution.
 */
public class LogScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {
    private static final IgniteLogger LOG = IgniteLogger.forClass(LogScheduledThreadPoolExecutor.class);

    private final String name;

    public LogScheduledThreadPoolExecutor(int corePoolSize, String name) {
        super(corePoolSize);
        this.name = name;
    }

    public LogScheduledThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory, String name) {
        super(corePoolSize, threadFactory);
        this.name = name;
    }

    public LogScheduledThreadPoolExecutor(int corePoolSize, RejectedExecutionHandler handler, String name) {
        super(corePoolSize, handler);
        this.name = name;
    }

    public LogScheduledThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory,
        RejectedExecutionHandler handler, String name) {
        super(corePoolSize, threadFactory, handler);
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        if (t == null && r instanceof Future<?>) {
            try {
                final Future<?> f = (Future<?>) r;
                if (f.isDone()) {
                    f.get();
                }
            }
            catch (final CancellationException ce) {
                // ignored
            }
            catch (final ExecutionException ee) {
                t = ee.getCause();
            }
            catch (final InterruptedException ie) {
                Thread.currentThread().interrupt(); // ignore/reset
            }
        }
        if (t != null) {
            LOG.error("Uncaught exception in pool: {}, {}.", t, this.name, super.toString());
        }
    }

    @Override
    protected void terminated() {
        super.terminated();
        LOG.info("ThreadPool is terminated: {}, {}.", this.name, super.toString());
    }
}
