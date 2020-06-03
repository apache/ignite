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
package org.apache.ignite.compatibility.sql.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Class that runs queries in different Ignite version and compares their execution times.
 */
public class QueryDuelRunner implements Runnable {
    /** JDBC connection pool of the old Ignite version. */
    private final SimpleConnectionPool oldConnPool;

    /** JDBC connection pool of the new Ignite version. */
    private final SimpleConnectionPool newConnPool;

    /** Query producer. */
    private final Supplier<String> qrySupplier;

    /** Collection of suspicious queries. */
    private final Collection<QueryDuelResult> suspiciousQrs;

    /** */
    private final AtomicBoolean stop;

    /** Number of success runs where oldExecTime <= newExecTime to pass the duel duel successfully. */
    private int successCnt;

    /**
     * Number of duel attempts. Duel is successful when it
     * made {@link #successCnt} successful runs with {@link #attemptsCnt} attempts.
     */
    private int attemptsCnt;

    /** */
    public QueryDuelRunner(
        SimpleConnectionPool oldConnPool,
        SimpleConnectionPool newConnPool,
        AtomicBoolean stop,
        Supplier<String> qrySupplier,
        Collection<QueryDuelResult> suspiciousQrs,
        int successCnt,
        int attemptsCnt
    ) {
        assert successCnt <= attemptsCnt : "successCnt=" + successCnt + ", attemptsCnt=" + attemptsCnt;

        this.oldConnPool = oldConnPool;
        this.newConnPool = newConnPool;
        this.qrySupplier = qrySupplier;
        this.suspiciousQrs = suspiciousQrs;
        this.stop = stop;
        this.successCnt = successCnt;
        this.attemptsCnt = attemptsCnt;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        String qry = qrySupplier.get();
        List<Long> oldQryExecTimes = new ArrayList<>(attemptsCnt);
        List<Long> newQryExecTimes = new ArrayList<>(attemptsCnt);
        List<Exception> exceptions = new ArrayList<>(attemptsCnt);

        while (attemptsCnt-- > 0) {
            try {
                QueryExecutionTimer oldVerRun = new QueryExecutionTimer(qry, oldConnPool);
                QueryExecutionTimer newVerRun = new QueryExecutionTimer(qry, newConnPool);

                CompletableFuture<Long> oldVerFut = CompletableFuture.supplyAsync(oldVerRun);
                CompletableFuture<Long> newVerFut = CompletableFuture.supplyAsync(newVerRun);

                CompletableFuture.allOf(oldVerFut, newVerFut).get();

                Long oldRes = oldVerFut.get();
                Long newRes = newVerFut.get();

                oldQryExecTimes.add(oldRes);
                newQryExecTimes.add(newRes);

                if (isSuccessfulRun(oldRes, newRes))
                    successCnt--;

                if (successCnt == 0)
                    break;
            }
            catch (Exception e) {
                e.printStackTrace();

                exceptions.add(e);

                stop.set(true);

                break;
            }
        }

        if (successCnt > 0)
            suspiciousQrs.add(new QueryDuelResult(qry, oldQryExecTimes, newQryExecTimes, exceptions));
    }

    /**
     * @param oldRes Query execution time in the old engine.
     * @param newRes Query execution time int current engine.
     * @return {@code True} if a query execution time in the new engine is not much longer than in the old one.
     */
    public boolean isSuccessfulRun(Long oldRes, Long newRes) {
        final double epsilon = 10.0; // Let's say 10 ms is about statistical error.

        if (oldRes < newRes && (oldRes > epsilon || newRes > epsilon)) {
            double newD = Math.max(newRes, epsilon);
            double oldD = Math.max(oldRes, epsilon);

            if (newD / oldD > 2)
                return false;
        }

        return true;
    }
}
