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

package org.apache.ignite.internal;

import java.util.UUID;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.query.VisorContinuousQueryCancelTask;
import org.apache.ignite.internal.visor.query.VisorContinuousQueryCancelTaskArg;
import org.apache.ignite.internal.visor.query.VisorQueryCancelTask;
import org.apache.ignite.internal.visor.query.VisorQueryCancelTaskArg;
import org.apache.ignite.internal.visor.query.VisorScanQueryCancelTask;
import org.apache.ignite.internal.visor.query.VisorScanQueryCancelTaskArg;
import org.apache.ignite.mxbean.QueryMXBean;

import static org.apache.ignite.internal.sql.command.SqlKillQueryCommand.parseGlobalQueryId;

/**
 * QueryMXBean implementation.
 */
public class QueryMXBeanImpl implements QueryMXBean {
    /** */
    public static final String EXPECTED_GLOBAL_QRY_ID_FORMAT = "Global query id should have format " +
        "'{node_id}_{query_id}', e.g. '6fa749ee-7cf8-4635-be10-36a1c75267a7_54321'";

    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteLogger log;

    /**
     * @param ctx Context.
     */
    public QueryMXBeanImpl(GridKernalContext ctx) {
        this.ctx = ctx;
        this.log = ctx.log(QueryMXBeanImpl.class);
    }

    /** {@inheritDoc} */
    @Override public void cancelContinuous(String routineId) {
        A.notNull(routineId, "routineId");

        cancelContinuous(UUID.fromString(routineId));
    }

    /** {@inheritDoc} */
    @Override public void cancelSQL(String id) {
        A.notNull(id, "id");

        if (log.isInfoEnabled())
            log.info("Killing sql query[id=" + id + ']');

        try {
            IgniteClusterImpl cluster = ctx.cluster().get();

            T2<UUID, Long> ids = parseGlobalQueryId(id);

            if (ids == null)
                throw new IllegalArgumentException("Expected global query id. " + EXPECTED_GLOBAL_QRY_ID_FORMAT);

            cluster.compute().execute(new VisorQueryCancelTask(),
                new VisorTaskArgument<>(ids.get1(), new VisorQueryCancelTaskArg(ids.get1(), ids.get2()), false));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void cancelScan(String originNodeId, String cacheName, Long id) {
        A.notNullOrEmpty(originNodeId, "originNodeId");
        A.notNullOrEmpty(cacheName, "cacheName");
        A.notNull(id, "id");

        if (log.isInfoEnabled())
            log.info("Killing scan query[id=" + id + ",originNodeId=" + originNodeId + ']');

        cancelScan(UUID.fromString(originNodeId), cacheName, id);
    }

    /**
     * @param originNodeId
     * @param cacheName
     * @param id
     */
    public void cancelScan(UUID originNodeId, String cacheName, long id) {
        try {
            IgniteClusterImpl cluster = ctx.cluster().get();

            UUID nid = cluster.nodes().iterator().next().id();

            IgniteCompute compute = cluster.compute();

            boolean res = compute.execute(new VisorScanQueryCancelTask(),
                new VisorTaskArgument<>(nid,
                    new VisorScanQueryCancelTaskArg(originNodeId, cacheName, id), false));

            if (!res) {
                log.warning("Query not found[originNodeId=" + originNodeId +
                    ",cacheName=" + cacheName + ",qryId=" + id + ']');
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param routineId
     */
    public void cancelContinuous(UUID routineId) {
        if (log.isInfoEnabled())
            log.info("Killing continuous query[routineId=" + routineId + ']');

        try {
            IgniteClusterImpl cluster = ctx.cluster().get();

            IgniteCompute compute = cluster.compute();

            UUID nid = cluster.nodes().iterator().next().id();

            compute.execute(new VisorContinuousQueryCancelTask(),
                new VisorTaskArgument<>(nid,
                    new VisorContinuousQueryCancelTaskArg(routineId), false));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
