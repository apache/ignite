/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.compatibility.sql.runner;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Executor that restricts the number of submitted tasks.
 */
public class BlockingExecutor {
    /** */
    private final Semaphore semaphore;

    /** */
    private final ExecutorService executor;

    /** */
    public BlockingExecutor(final int taskLimit) {
        semaphore = new Semaphore(taskLimit);
        executor = new ThreadPoolExecutor(
            taskLimit,
            taskLimit,
            10,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(taskLimit));
    }

    /** */
    public void execute(final Runnable r) {
        try {
            semaphore.acquire();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            return;
        }

        final Runnable semaphoredRunnable = () -> {
            try {
                r.run();
            }
            finally {
                semaphore.release();
            }
        };

        executor.execute(semaphoredRunnable);
    }

    /** */
    public void stopAndWaitForTermination(long timeout) throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(timeout, TimeUnit.MILLISECONDS);
    }
}
