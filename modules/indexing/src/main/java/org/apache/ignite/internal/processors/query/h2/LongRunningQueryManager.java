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

import java.sql.PreparedStatement;
import java.util.Collection;
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
    private final ConcurrentHashMap<IgniteH2QueryInfo, Boolean> qrys = new ConcurrentHashMap<>();

    /** Check long query task. */
    private final GridTimeoutProcessor.CancelableTask checkLongQryTask;

    /** Logger. */
    private final IgniteLogger log;

    /** Query timeout milliseconds. */
    private volatile long longQryWarnTimeout;

    /** Query result set size threshold. */
    private volatile long rsSizeThreshold = DFLT_FETCHED_SIZE_THRESHOLD;

    /**
     * @param ctx Kernal context.
     */
    public LongRunningQueryManager(GridKernalContext ctx) {
        connMgr = ((IgniteH2Indexing)ctx.query().getIndexing()).connections();

        log = ctx.log(LongRunningQueryManager.class);

        checkLongQryTask = ctx.timeout().schedule(this::checkLongRunning, CHECK_PERIOD, CHECK_PERIOD);

        longQryWarnTimeout = ctx.config().getLongQueryWarningTimeout();
    }

    /**
     *
     */
    public void stop() {
        checkLongQryTask.close();

        qrys.clear();
    }

    /**
     * @param stmt Query statement.
     * @param sql Query statement.
     * @param params Query parameters.
     * @return Registered info.
     */
    public IgniteH2QueryInfo registerQuery(PreparedStatement stmt, String sql, Collection<Object> params) {
        IgniteH2QueryInfo info = IgniteH2QueryInfo.collectInfo(stmt, sql, params);

        qrys.put(info, true);

        return info;
    }

    /**
     * @param qryInfo Query info to remove.
     */
    public void unregisterQuery(IgniteH2QueryInfo qryInfo) {
        qrys.remove(qryInfo);
    }

    /**
     *
     */
    private void checkLongRunning() {
        for (IgniteH2QueryInfo qinfo : qrys.keySet()) {
            if (qinfo.time() > longQryWarnTimeout) {
                qinfo.printLogMessage(log, connMgr, "Query execution is too long");

                qrys.remove(qinfo);
            }
        }
    }

    /**
     * @return Timeout in milliseconds after which long query warning will be printed.
     */
    public long getLongQueryWarningTimeout() {
        return longQryWarnTimeout;
    }

    /**
     * Sets timeout in milliseconds after which long query warning will be printed.
     *
     * @param timeout Timeout in milliseconds after which long query warning will be printed.
     */
    public void setLongQueryWarningTimeout(long timeout) {
        longQryWarnTimeout = timeout;
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
}
