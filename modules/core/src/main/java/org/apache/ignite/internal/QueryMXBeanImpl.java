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

import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.mxbean.QueryMXBean;
import org.apache.ignite.resources.IgniteInstanceResource;

import static org.apache.ignite.internal.sql.command.SqlKillQueryCommand.parseGlobalQueryId;

/**
 * QueryMXBean implementation.
 */
public class QueryMXBeanImpl implements QueryMXBean {
    /** Global query id format. */
    public static final String EXPECTED_GLOBAL_QRY_ID_FORMAT = "Global query id should have format " +
        "'{node_id}_{query_id}', e.g. '6fa749ee-7cf8-4635-be10-36a1c75267a7_54321'";

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ctx Context.
     */
    public QueryMXBeanImpl(GridKernalContext ctx) {
        this.ctx = ctx;
        this.log = ctx.log(QueryMXBeanImpl.class);
    }

    /** {@inheritDoc} */
    @Override public void cancelContinuous(String originNodeId, String routineId) {
        A.notNullOrEmpty(originNodeId, "originNodeId");
        A.notNullOrEmpty(routineId, "routineId");

        if (log.isInfoEnabled())
            log.info("Killing continuous query[routineId=" + routineId + ",originNodeId=" + originNodeId + ']');

        cancelContinuous(UUID.fromString(originNodeId), UUID.fromString(routineId));
    }

    /** {@inheritDoc} */
    @Override public void cancelSQL(String id) {
        A.notNull(id, "id");

        if (log.isInfoEnabled())
            log.info("Killing sql query[id=" + id + ']');

        try {
            T2<UUID, Long> ids = parseGlobalQueryId(id);

            if (ids == null)
                throw new IllegalArgumentException("Expected global query id. " + EXPECTED_GLOBAL_QRY_ID_FORMAT);

            cancelSQL(ids.get1(), ids.get2());
        }
        catch (IgniteException e) {
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
     * Executes scan query cancel on all cluster nodes.
     *
     * @param originNodeId Originating node id.
     *
     */
    public void cancelSQL(UUID originNodeId, long qryId) {
        ctx.grid().compute(ctx.grid().cluster().forNodeId(originNodeId))
            .broadcast(new CancelSQLOnInitiator(), qryId);
    }

    /**
     * Executes scan query cancel on all cluster nodes.
     *
     * @param originNodeId Originating node id.
     * @param cacheName Cache name.
     * @param id Scan query id.
     */
    public void cancelScan(UUID originNodeId, String cacheName, long id) {
        ctx.grid().compute(ctx.grid().cluster())
            .broadcast(new CancelScan(), new T3<>(originNodeId, cacheName, id));
    }

    /**
     * Kills continuous query by the identifier.
     *
     * @param originNodeId Originating node id.
     * @param routineId Routine id.
     */
    public void cancelContinuous(UUID originNodeId, UUID routineId) {
        ctx.grid().compute(ctx.grid().cluster().forNodeId(originNodeId))
            .broadcast(new CancelContinuousOnInitiator(), routineId);
    }

    /**
     * Cancel scan closure.
     */
    private static class CancelScan implements IgniteClosure<T3<UUID, String, Long>, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /** {@inheritDoc} */
        @Override public Void apply(T3<UUID, String, Long> arg) {
            IgniteLogger log = ignite.log().getLogger(getClass());

            int cacheId = CU.cacheId(arg.get2());

            GridCacheContext<?, ?> ctx = ignite.context().cache().context().cacheContext(cacheId);

            if (ctx == null) {
                log.warning("Cache not found[cacheName=" + arg.get2() + ']');

                return null;
            }

            ctx.queries().removeQueryResult(arg.get1(), arg.get3());

            return null;
        }
    }

    /**
     * Cancel SQL on initiator closure.
     */
    private static class CancelSQLOnInitiator implements IgniteClosure<Long, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /** {@inheritDoc} */
        @Override public Void apply(Long qryId) {
            ignite.context().query().cancelQueries(Collections.singleton(qryId));

            return null;
        }
    }

    /**
     * Cancel continuous on initiator closure.
     */
    private static class CancelContinuousOnInitiator implements IgniteClosure<UUID, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /** {@inheritDoc} */
        @Override public Void apply(UUID routineId) {
            IgniteInternalFuture<?> fut = ignite.context().continuous().stopRoutine(routineId);

            try {
                fut.get();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            return null;
        }
    }
}
