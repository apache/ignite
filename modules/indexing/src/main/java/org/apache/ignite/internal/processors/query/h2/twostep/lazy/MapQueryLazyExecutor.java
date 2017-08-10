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

package org.apache.ignite.internal.processors.query.h2.twostep.lazy;

import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME;

/**
 * Pool of workers for lazy query execution.
 */
public class MapQueryLazyExecutor {
    /** Executor responsible for shared task dispatch. */
    private final IgniteThreadPoolExecutor exec;

    /** Threads. */
    private final ConcurrentHashMap<Long, MapQueryLazyIgniteThread> threads = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param poolSize Thread pool size.
     */
    public MapQueryLazyExecutor(String igniteInstanceName, int poolSize) {
        assert poolSize > 0;

        exec = new IgniteThreadPoolExecutor(
            "query-lazy",
            igniteInstanceName,
            poolSize,
            poolSize,
            DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<Runnable>(),
            GridIoPolicy.SYSTEM_POOL);
    }

    /**
     * Submit a command to a dedicated worker, or enqueue in case no workers are available.
     *
     * @param cmd Shared command to be executed.
     */
    public void submitShared(final Runnable cmd) {
        Runnable r = new Runnable() {
            @Override public void run() {
                // Register thread in a shared map.
                MapQueryLazyIgniteThread thread = (MapQueryLazyIgniteThread)Thread.currentThread();

                long threadKey = thread.index();

                threads.put(threadKey, thread);

                try {
                    cmd.run();
                }
                finally {
                    threads.remove(threadKey);
                }
            }
        };

        exec.submit(r);
    }

    /**
     * Submit a task for specific thread.
     *
     * @param idx Index.
     * @param cmd Command to be executed.
     */
    public void submit(long idx, Runnable cmd) {
        MapQueryLazyIgniteThread thread = threads.get(idx);

        if (thread != null)
            thread.submit(cmd);
    }

    /**
     * Shutdown the cluster.
     */
    public void shutdown() {
        exec.shutdown();

        for (MapQueryLazyIgniteThread thread : threads.values())
            thread.shutdown();
    }
}
