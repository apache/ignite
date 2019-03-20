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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.GridConcurrentSkipListSet;

/**
 * Long running query manager.
 */
public class LongRunningQueryManager {
    /** Check period. */
    private static final long CHECK_PERIOD = 1_000;

    /** Query timeout milliseconds. */
    public static long QUERY_TIMEOUT_MS = 1_000;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Connection manager. */
    private final ConnectionManager connMgr;

    /** Queries collection sorted by begin time. */
    private final GridConcurrentSkipListSet<IgniteH2QueryInfo> qrys = new GridConcurrentSkipListSet<>();

    /** Check long query task. */
    private final GridTimeoutProcessor.CancelableTask checkLongQueryTask;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ctx Kernal context.
     */
    public LongRunningQueryManager(GridKernalContext ctx) {
        this.ctx = ctx;

        connMgr = ((IgniteH2Indexing)ctx.query().getIndexing()).connections();

        log = ctx.log(LongRunningQueryManager.class);

        checkLongQueryTask = ctx.timeout().schedule(this::checkLongRunning, CHECK_PERIOD, CHECK_PERIOD);
    }

    /**
     *
     */
    public void stop() {
        checkLongQueryTask.close();

        qrys.clear();
    }

    /**
     * @param stmt Query statement.
     * @param sql Query statement.
     * @param params Query parameters.
     */
    public void registerQuery(PreparedStatement stmt, String sql, Collection<Object> params) {
        try {
            String schema = stmt.getConnection().getSchema();

            qrys.add(new IgniteH2QueryInfo(stmt, schema, sql, params));
        }
        catch (SQLException e) {
            throw new IgniteSQLException("Cannot track query", IgniteQueryErrorCode.UNKNOWN, e);
        }
    }

    /**
     * @param stmt Query statement.
     */
    public void unregisterQuery(PreparedStatement stmt) {
        qrys.remove(new IgniteH2QueryInfo(stmt));
    }

    /**
     *
     */
    private void checkLongRunning() {
        for (IgniteH2QueryInfo qinfo : qrys) {
            if (qinfo.isLong(QUERY_TIMEOUT_MS))
                qinfo.printLogMessage(log, connMgr);
        }
    }
}
