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
package org.apache.ignite.internal.processors.query.calcite.exec.ddl;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.processors.query.GridRunningQueryManager;
import org.apache.ignite.internal.processors.query.QueryHistory;
import org.apache.ignite.internal.processors.query.QueryHistoryKey;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper of running query manager.
 */
public class RunningQueryManagerWrapper implements GridRunningQueryManager {
    /** Running query manager supplier. */
    private final Supplier<GridRunningQueryManager> qryMgrSupp;

    /**
     * @param ctx Grid kernal context.
     */
    public RunningQueryManagerWrapper(GridKernalContext ctx) {
        qryMgrSupp = () -> {
            GridQueryProcessor queryProc = ctx.query();
            if (queryProc == null)
                return null;

            IgniteH2Indexing idx = (IgniteH2Indexing)queryProc.getIndexing();
            if (idx == null)
                return null;

            return idx.runningQueryManager();
        };
    }

    /** {@inheritDoc} */
    @Override @Nullable public GridRunningQueryInfo runningQueryInfo(Long qryId) {
        GridRunningQueryManager qryMgr = qryMgrSupp.get();
        if (qryMgr == null)
            return null;

        return qryMgr.runningQueryInfo(qryId);
    }

    /** {@inheritDoc} */
    @Override public Long register(
        String qry,
        GridCacheQueryType qryType,
        String schemaName,
        boolean loc,
        @Nullable GridQueryCancel cancel,
        String qryInitiatorId
    ) {
        GridRunningQueryManager qryMgr = qryMgrSupp.get();
        if (qryMgr == null)
            return -1L;

        return qryMgr.register(qry, qryType, schemaName, loc, cancel, qryInitiatorId);
    }

    /** {@inheritDoc} */
    @Override public void unregister(Long qryId, @Nullable Throwable failReason) {
        GridRunningQueryManager qryMgr = qryMgrSupp.get();
        if (qryMgr == null)
            return;

        qryMgr.unregister(qryId, failReason);
    }

    /** {@inheritDoc} */
    @Override public void cancelQuery(long queryId, @Nullable UUID nodeId, boolean async) {
        GridRunningQueryManager qryMgr = qryMgrSupp.get();
        if (qryMgr == null)
            return;

        qryMgr.cancelQuery(queryId, nodeId, async);
    }

    /** {@inheritDoc} */
    @Override public void trackRequestId(long reqId) {
        GridRunningQueryManager qryMgr = qryMgrSupp.get();
        if (qryMgr == null)
            return;

        qryMgr.trackRequestId(reqId);
    }

    /** {@inheritDoc} */
    @Override public Map<QueryHistoryKey, QueryHistory> queryHistoryMetrics() {
        GridRunningQueryManager qryMgr = qryMgrSupp.get();
        if (qryMgr == null)
            return Collections.emptyMap();

        return qryMgr.queryHistoryMetrics();
    }

    /** {@inheritDoc} */
    @Override public void resetQueryHistoryMetrics() {
        GridRunningQueryManager qryMgr = qryMgrSupp.get();
        if (qryMgr == null)
            return;

        qryMgr.resetQueryHistoryMetrics();
    }
}
