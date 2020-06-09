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

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Benchmark runner.
 */
public class QueryDuelBenchmark {
    /** Workers count. */
    private final int workersCnt;

    /** Pool of JDBC connections to the old Ignite version. */
    private final SimpleConnectionPool oldConnPool;

    /** Pool of JDBC connections to the new Ignite version. */
    private final SimpleConnectionPool newConnPool;

    /** */
    public QueryDuelBenchmark(int workersCnt, SimpleConnectionPool oldConnPool,
        SimpleConnectionPool newConnPool) {
        this.workersCnt = workersCnt;
        this.oldConnPool = oldConnPool;
        this.newConnPool = newConnPool;
    }

    /**
     * Starts task that compare query execution time in the new and old Ignite versions.
     *
     * @param timeout Test duration.
     * @param qrySupplier Sql queries generator.
     * @return Suspicious queries collection.
     * @throws InterruptedException If interrupted.
     */
    public Collection<QueryDuelResult> runBenchmark(
        final long timeout,
        Supplier<String> qrySupplier,
        int successCnt,
        int attemptsCnt
    ) throws InterruptedException {
        Collection<QueryDuelResult> suspiciousQrys = Collections.newSetFromMap(new ConcurrentHashMap<>());

        final long end = System.currentTimeMillis() + timeout;

        AtomicBoolean stop = new AtomicBoolean();

        BlockingExecutor exec = new BlockingExecutor(workersCnt);

        while (System.currentTimeMillis() < end && !stop.get()) {
            QueryDuelRunner runner = new QueryDuelRunner(oldConnPool, newConnPool,
                stop, qrySupplier, suspiciousQrys, successCnt, attemptsCnt);

            exec.execute(runner);
        }

        exec.stopAndWaitForTermination(30_000);

        if (stop.get())
            throw new AssertionError("Failed to run some queries: " + suspiciousQrys);

        return suspiciousQrys;
    }
}
