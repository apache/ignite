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

package org.apache.ignite.internal.processors.query.h2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;

/**
 * Long running query manager.
 */
public class LongRunningQueryManager {
    /** Check period in ms. */
    private static final long CHECK_PERIOD = 1_000;

    /**
     * Default threshold result's row count, when count of fetched rows is bigger than the threshold
     * warning will be printed.
     */
    private static final long DFLT_FETCHED_SIZE_THRESHOLD = 100_000;

    /** Connection manager. */
    private final ConnectionManager connMgr;

    /** Queries collection. Sorted collection isn't used to reduce 'put' time. */
    private final ConcurrentHashMap<AbstractH2QueryInfo, TimeoutChecker> qrys = new ConcurrentHashMap<>();

    /** Check long query task. */
    private final GridTimeoutProcessor.CancelableTask checkLongQryTask;

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
    public LongRunningQueryManager(GridKernalContext ctx) {
        connMgr = ((IgniteH2Indexing)ctx.query().getIndexing()).connections();

        log = ctx.log(LongRunningQueryManager.class);

        checkLongQryTask = ctx.timeout().schedule(this::checkLongRunning, CHECK_PERIOD, CHECK_PERIOD);

        timeout = ctx.config().getLongQueryWarningTimeout();
    }

    /**
     *
     */
    public void stop() {
        checkLongQryTask.close();

        qrys.clear();
    }

    /**
     * @param qryInfo Query info to register.
     */
    public void registerQuery(AbstractH2QueryInfo qryInfo) {
        if (timeout > 0 && qryInfo != null)
            qrys.put(qryInfo, new TimeoutChecker(timeout, timeoutMult));
    }

    /**
     * @param qryInfo Query info to remove.
     */
    public void unregisterQuery(AbstractH2QueryInfo qryInfo) {
        if (qryInfo != null)
            qrys.remove(qryInfo);
    }

    /**
     *
     */
    private void checkLongRunning() {
        for (Map.Entry<AbstractH2QueryInfo, TimeoutChecker> e : qrys.entrySet()) {
            AbstractH2QueryInfo qinfo = e.getKey();

            if (e.getValue().checkTimeout(qinfo.time())) {
                qinfo.printLogMessage(log, connMgr, "Query execution is too long");

                if (timeoutMult <= 1)
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

        if (timeout <= 0)
            qrys.clear();
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
        this.rsSizeThresholdMult = rsSizeThresholdMult;
    }

    /**
     * Holds timeout settings for the specified query.
     */
    private static class TimeoutChecker {
        /** */
        private long timeout;

        /** */
        private int timeoutMult;

        /**
         * @param timeout Initial timeout.
         * @param timeoutMult Timeout multiplier.
         */
        public TimeoutChecker(long timeout, int timeoutMult) {
            this.timeout = timeout;
            this.timeoutMult = timeoutMult;
        }

        /**
         * @param time Query execution time.
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
}
