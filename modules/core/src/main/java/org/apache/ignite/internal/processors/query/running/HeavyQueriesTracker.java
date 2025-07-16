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

package org.apache.ignite.internal.processors.query.running;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

/**
 * Class to track heavy queries (long-running or producing a big result-set).
 */
public final class HeavyQueriesTracker {
    /** Check period in ms. */
    private static final long CHECK_PERIOD = 1_000;

    /**
     * Default threshold result's row count, when count of fetched rows is bigger than the threshold
     * warning will be printed.
     */
    private static final long DFLT_FETCHED_SIZE_THRESHOLD = 100_000;

    /** Message about the long execution of the query. */
    public static final String LONG_QUERY_EXEC_MSG = "Query execution is too long";

    /** */
    public static final String LONG_QUERY_FINISHED_MSG = "Long running query is finished";

    /** */
    public static final String LONG_QUERY_ERROR_MSG = "Long running query is finished with error: ";

    /** */
    public static final String BIG_RESULT_SET_MSG = "Query produced big result set.";

    /** Queries collection. Sorted collection isn't used to reduce 'put' time. */
    private final ConcurrentHashMap<TrackableQuery, TimeoutChecker> qrys = new ConcurrentHashMap<>();

    /** Check worker. */
    private final GridWorker checkWorker;

    /** Logger. */
    private final IgniteLogger log;

    /** Long query timeout milliseconds. */
    private volatile long timeout;

    /**
     * Long query timeout multiplier. The warning will be printed after:
     * - timeout;
     * - timeout * multiplier;
     * - timeout * multiplier * multiplier;
     * - etc...
     *
     * If the multiplier <= 1, the warning message is printed once.
     */
    private volatile int timeoutMult = 2;

    /** Query result set size threshold. */
    private volatile long rsSizeThreshold = DFLT_FETCHED_SIZE_THRESHOLD;

    /**
     * Result set size threshold multiplier. The warning will be printed after:
     * - size of result set > threshold;
     * - size of result set > threshold * multiplier;
     * - size of result set > threshold * multiplier * multiplier;
     * - etc.
     *
     * If the multiplier <= 1, the warning message is printed once.
     */
    private volatile int rsSizeThresholdMult = 2;

    /**
     * @param ctx Kernal context.
     */
    public HeavyQueriesTracker(GridKernalContext ctx) {
        log = ctx.log(HeavyQueriesTracker.class);

        checkWorker = new GridWorker(ctx.igniteInstanceName(), "long-qry", log) {
            @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
                while (true) {
                    checkLongRunning();

                    U.sleep(CHECK_PERIOD);
                }
            }
        };

        timeout = ctx.config().getSqlConfiguration().getLongQueryWarningTimeout();

        IgniteThread thread = new IgniteThread(checkWorker);

        thread.setDaemon(true);
        thread.start();
    }

    /**
     *
     */
    public void stop() {
        checkWorker.cancel();

        qrys.clear();
    }

    /**
     * @param qryInfo Query info to register.
     */
    public void startTracking(TrackableQuery qryInfo) {
        assert qryInfo != null;

        final long timeout0 = timeout;

        if (timeout0 > 0)
            qrys.put(qryInfo, new TimeoutChecker(timeout0, timeoutMult));
    }

    /**
     * @param qryInfo Query info to remove.
     * @param err Exception if query executed with error.
     */
    public void stopTracking(TrackableQuery qryInfo, @Nullable Throwable err) {
        assert qryInfo != null;

        if (qrys.remove(qryInfo) != null && qryInfo.time() > timeout) {
            if (err == null)
                LT.warn(log, LONG_QUERY_FINISHED_MSG + qryInfo.queryInfo(null));
            else
                LT.warn(log, LONG_QUERY_ERROR_MSG + err.getMessage() + qryInfo.queryInfo(null));
        }
    }

    /**
     * Creates new result-set checker.
     * @param qryInfo Query info.
     */
    public ResultSetChecker resultSetChecker(TrackableQuery qryInfo) {
        return new ResultSetChecker(log, qryInfo, rsSizeThreshold, rsSizeThresholdMult);
    }

    /**
     *
     */
    private void checkLongRunning() {
        for (Map.Entry<TrackableQuery, TimeoutChecker> e : qrys.entrySet()) {
            TrackableQuery qinfo = e.getKey();

            if (e.getValue().checkTimeout(qinfo.time())) {
                LT.warn(log, LONG_QUERY_EXEC_MSG + qinfo.queryInfo(null));

                if (e.getValue().timeoutMult <= 1)
                    qrys.remove(qinfo);
            }
        }
    }

    /**
     * @return Timeout in milliseconds after which long query warning will be printed.
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * Sets timeout in milliseconds after which long query warning will be printed.
     *
     * @param timeout Timeout in milliseconds after which long query warning will be printed.
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * @return Long query timeout multiplier.
     */
    public int getTimeoutMultiplier() {
        return timeoutMult;
    }

    /**
     * Sets long query timeout multiplier. The warning will be printed after:
     * - timeout;
     * - timeout * multiplier;
     * - timeout * multiplier * multiplier;
     * - etc...
     * If the multiplier <= 1, the warning message is printed once.
     *
     * @param timeoutMult Long query timeout multiplier.
     */
    public void setTimeoutMultiplier(int timeoutMult) {
        this.timeoutMult = timeoutMult;
    }

    /**
     * @return Threshold result's row count, when count of fetched rows is bigger than the threshold
     *      warning will be printed.
     */
    public long getResultSetSizeThreshold() {
        return rsSizeThreshold;
    }

    /**
     * Sets threshold result's row count, when count of fetched rows is bigger than the threshold
     *      warning will be printed.
     *
     * @param rsSizeThreshold Threshold result's row count, when count of fetched rows is bigger than the threshold
     *      warning will be printed.
     */
    public void setResultSetSizeThreshold(long rsSizeThreshold) {
        this.rsSizeThreshold = rsSizeThreshold;
    }

    /**
     * Gets result set size threshold multiplier. The warning will be printed after:
     *  - size of result set > threshold;
     *  - size of result set > threshold * multiplier;
     *  - size of result set > threshold * multiplier * multiplier;
     *  - etc.
     * If the multiplier <= 1, the warning message is printed once.
     * @return Result set size threshold multiplier.
     */
    public int getResultSetSizeThresholdMultiplier() {
        return rsSizeThresholdMult;
    }

    /**
     * Sets result set size threshold multiplier.
     *
     * @param rsSizeThresholdMult Result set size threshold multiplier
     */
    public void setResultSetSizeThresholdMultiplier(int rsSizeThresholdMult) {
        this.rsSizeThresholdMult = rsSizeThresholdMult <= 1 ? 1 : rsSizeThresholdMult;
    }

    /** */
    public Set<TrackableQuery> getQueries() {
        return qrys.keySet();
    }

    /**
     * Holds timeout settings for the specified query.
     */
    private static class TimeoutChecker {
        /** */
        private long timeout;

        /** */
        private final int timeoutMult;

        /**
         * @param timeout Initial timeout.
         * @param timeoutMult Timeout multiplier.
         */
        public TimeoutChecker(long timeout, int timeoutMult) {
            this.timeout = timeout;
            this.timeoutMult = timeoutMult;
        }

        /**
         * @return {@code true} if timeout occurred.
         */
        public boolean checkTimeout(long time) {
            if (time > timeout) {
                if (timeoutMult > 1)
                    timeout *= timeoutMult;

                return true;
            }
            else
                return false;
        }
    }

    /**
     * Holds result-set limit settings for the specified query.
     */
    public static class ResultSetChecker {
        /** Logger. */
        private final IgniteLogger log;

        /** Query info. */
        private final TrackableQuery qryInfo;

        /** Result set size threshold. */
        private long threshold;

        /** Result set size threshold multiplier. */
        private final int thresholdMult;

        /** Fetched count of rows. */
        private long fetchedSize;

        /** Big results flag. */
        private boolean bigResults;

        /** Ctor. */
        private ResultSetChecker(IgniteLogger log, TrackableQuery qryInfo, long threshold, int thresholdMult) {
            this.log = log;
            this.qryInfo = qryInfo;
            this.threshold = threshold;
            this.thresholdMult = thresholdMult;
        }

        /**
         * Print warning message to log when query result size fetch count is bigger than specified threshold.
         * Threshold may be recalculated with multiplier.
         */
        public void checkOnFetchNext() {
            ++fetchedSize;

            if (threshold > 0 && fetchedSize >= threshold) {
                LT.warn(log, BIG_RESULT_SET_MSG + qryInfo.queryInfo("fetched=" + fetchedSize));

                if (thresholdMult > 1)
                    threshold *= thresholdMult;
                else
                    threshold = 0;

                bigResults = true;
            }
        }

        /** */
        public void checkOnClose() {
            if (bigResults)
                LT.warn(log, BIG_RESULT_SET_MSG + qryInfo.queryInfo("fetched=" + fetchedSize));
        }

        /** */
        public long fetchedSize() {
            return fetchedSize;
        }
    }
}
