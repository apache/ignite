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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.lang.IgniteLogger;

/**
 *
 */
public final class ExecutorServiceHelper {

    private static final IgniteLogger LOG = IgniteLogger.forClass(ExecutorServiceHelper.class);

    /**
     * @see #shutdownAndAwaitTermination(ExecutorService, long)
     */
    public static boolean shutdownAndAwaitTermination(final ExecutorService pool) {
        return shutdownAndAwaitTermination(pool, 1000);
    }

    /**
     * The following method shuts down an {@code ExecutorService} in two phases, first by calling {@code shutdown} to
     * reject incoming tasks, and then calling {@code shutdownNow}, if necessary, to cancel any lingering tasks.
     */
    public static boolean shutdownAndAwaitTermination(final ExecutorService pool, final long timeoutMillis) {
        if (pool == null) {
            return true;
        }
        // disable new tasks from being submitted
        pool.shutdown();
        final TimeUnit unit = TimeUnit.MILLISECONDS;
        final long phaseOne = timeoutMillis / 5;
        try {
            // wait a while for existing tasks to terminate
            if (pool.awaitTermination(phaseOne, unit)) {
                return true;
            }
            pool.shutdownNow();
            // wait a while for tasks to respond to being cancelled
            if (pool.awaitTermination(timeoutMillis - phaseOne, unit)) {
                return true;
            }
            LOG.warn("Fail to shutdown pool: {}.", pool);
        }
        catch (final InterruptedException e) {
            // (Re-)cancel if current thread also interrupted
            pool.shutdownNow();
            // preserve interrupt status
            Thread.currentThread().interrupt();
        }
        return false;
    }
}
