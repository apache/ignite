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

package org.apache.ignite.internal.processors.cache.query.jdbc;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.jdk.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.math.*;
import java.net.*;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.compute.ComputeJobResultPolicy.*;

/**
 * Task for JDBC adapter.
 */
public class GridCacheQueryJdbcTask extends ComputeTaskAdapter<byte[], byte[]> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Marshaller. */
    private static final IgniteMarshaller MARSHALLER = new IgniteJdkMarshaller();

    /** How long to store future (10 minutes). */
    private static final int RMV_DELAY = 10 * 60;

    /** Scheduler. */
    private static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1);

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, byte[] arg) throws IgniteCheckedException {
        assert arg != null;

        Map<String, Object> args = MARSHALLER.unmarshal(arg, null);

        boolean first = true;

        UUID nodeId = (UUID)args.remove("confNodeId");

        if (nodeId == null) {
            nodeId = (UUID)args.remove("nodeId");

            first = nodeId == null;
        }

        if (nodeId != null) {
            for (ClusterNode n : subgrid)
                if (n.id().equals(nodeId))
                    return F.asMap(new JdbcDriverJob(args, first), n);

            throw new IgniteCheckedException("Node doesn't exist or left the grid: " + nodeId);
        }
        else {
            String cache = (String)args.get("cache");

            for (ClusterNode n : subgrid)
                if (U.hasCache(n, cache))
                    return F.asMap(new JdbcDriverJob(args, first), n);

            throw new IgniteCheckedException("Can't find node with cache: " + cache);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
        byte status;
        byte[] bytes;

        ComputeJobResult res = F.first(results);

        if (res.getException() == null) {
            status = 0;

            bytes = MARSHALLER.marshal(res.getData());
        }
        else {
            status = 1;

            bytes = MARSHALLER.marshal(new SQLException(res.getException().getMessage()));
        }

        byte[] packet = new byte[bytes.length + 1];

        packet[0] = status;

        U.arrayCopy(bytes, 0, packet, 1, bytes.length);

        return packet;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteCheckedException {
        return WAIT;
    }

    /**
     * Job for JDBC adapter.
     */
    private static class JdbcDriverJob extends ComputeJobAdapter implements IgniteOptimizedMarshallable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @SuppressWarnings({"NonConstantFieldWithUpperCaseName", "AbbreviationUsage", "UnusedDeclaration"})
        private static Object GG_CLASS_ID;

        /** Arguments. */
        private final Map<String, Object> args;

        /** First execution flag. */
        private final boolean first;

        /** Grid instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Logger. */
        @IgniteLoggerResource
        private IgniteLogger log;

        /**
         * @param args Arguments.
         * @param first First execution flag.
         */
        JdbcDriverJob(Map<String, Object> args, boolean first) {
            assert args != null;
            assert args.size() == (first ? 6 : 3);

            this.args = args;
            this.first = first;
        }

        /** {@inheritDoc} */
        @Override public Object ggClassId() {
            return GG_CLASS_ID;
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteCheckedException {
            String cacheName = argument("cache");
            String sql = argument("sql");
            Long timeout = argument("timeout");
            List<Object> args = argument("args");
            UUID futId = argument("futId");
            Integer pageSize = argument("pageSize");
            Integer maxRows = argument("maxRows");

            assert pageSize != null;
            assert maxRows != null;

            GridTuple4<CacheQueryFuture<List<?>>, Integer, Boolean, Collection<String>> t = null;

            Collection<String> tbls = null;
            Collection<String> cols;
            Collection<String> types = null;

            if (first) {
                assert sql != null;
                assert timeout != null;
                assert args != null;
                assert futId == null;

                Cache<?, ?> cache = ((GridEx) ignite).cachex(cacheName);

                CacheQuery<List<?>> qry =
                    ((GridCacheQueriesEx<?, ?>)cache.queries()).createSqlFieldsQuery(sql, true);

                qry.pageSize(pageSize);
                qry.timeout(timeout);

                // Query local and replicated caches only locally.
                if (cache.configuration().getCacheMode() != PARTITIONED)
                    qry = qry.projection(ignite.cluster().forLocal());

                CacheQueryFuture<List<?>> fut = qry.execute(args.toArray());

                Collection<GridQueryFieldMetadata> meta = ((GridCacheQueryMetadataAware)fut).metadata().get();

                if (meta == null) {
                    // Try to extract initial SQL exception.
                    try {
                        fut.get();
                    }
                    catch (IgniteCheckedException e) {
                        if (e.hasCause(SQLException.class))
                            throw new GridInternalException(e.getCause(SQLException.class).getMessage(), e);
                    }

                    throw new GridInternalException("Query failed on all nodes. Probably you are requesting " +
                        "nonexistent table (check database metadata) or you are trying to join data that is " +
                        "stored in non-collocated mode.");
                }

                tbls = new ArrayList<>(meta.size());
                cols = new ArrayList<>(meta.size());
                types = new ArrayList<>(meta.size());

                for (GridQueryFieldMetadata desc : meta) {
                    tbls.add(desc.typeName());
                    cols.add(desc.fieldName().toUpperCase());
                    types.add(desc.fieldTypeName());
                }

                futId = UUID.randomUUID();

                ignite.cluster().nodeLocalMap().put(futId, t = F.t(fut, 0, false, cols));

                scheduleRemoval(futId);
            }

            assert futId != null;

            if (t == null)
                t = ignite.cluster().<UUID, GridTuple4<CacheQueryFuture<List<?>>, Integer, Boolean,
                    Collection<String>>>nodeLocalMap().get(futId);

            assert t != null;

            cols = t.get4();

            Collection<List<Object>> fields = new LinkedList<>();

            CacheQueryFuture<List<?>> fut = t.get1();

            int pageCnt = 0;
            int totalCnt = t.get2();

            List<?> next;

            while ((next = fut.next()) != null && pageCnt++ < pageSize && (maxRows == 0 || totalCnt++ < maxRows)) {
                fields.add(F.transformList(next, new C1<Object, Object>() {
                    @Override public Object apply(Object val) {
                        if (val != null && !sqlType(val))
                            val = val.toString();

                        return val;
                    }
                }));
            }

            boolean finished = next == null || totalCnt == maxRows;

            if (!finished)
                ignite.cluster().nodeLocalMap().put(futId, F.t(fut, totalCnt, true, cols));
            else
                ignite.cluster().nodeLocalMap().remove(futId);

            return first ? F.asList(ignite.cluster().localNode().id(), futId, tbls, cols, types, fields, finished) :
                F.asList(fields, finished);
        }

        /**
         * Schedules removal of stored future.
         *
         * @param id Future ID.
         */
        private void scheduleRemoval(final UUID id) {
            SCHEDULER.schedule(new CAX() {
                @Override public void applyx() {
                    GridTuple3<CacheQueryFuture<List<?>>, Integer, Boolean> t =
                        ignite.cluster().<UUID, GridTuple3<CacheQueryFuture<List<?>>, Integer, Boolean>>nodeLocalMap().get(id);

                    if (t != null) {
                        // If future was accessed since last scheduling,
                        // set access flag to false and reschedule.
                        if (t.get3()) {
                            t.set3(false);

                            scheduleRemoval(id);
                        }
                        // Remove stored future otherwise.
                        else
                            ignite.cluster().nodeLocalMap().remove(id);
                    }
                }
            }, RMV_DELAY, TimeUnit.SECONDS);
        }

        /**
         * Checks whether type of the object is SQL-complaint.
         *
         * @param obj Object.
         * @return Whether type of the object is SQL-complaint.
         */
        private boolean sqlType(Object obj) {
            return obj instanceof BigDecimal ||
                obj instanceof Boolean ||
                obj instanceof Byte ||
                obj instanceof byte[] ||
                obj instanceof Date ||
                obj instanceof Double ||
                obj instanceof Float ||
                obj instanceof Integer ||
                obj instanceof Long ||
                obj instanceof Short ||
                obj instanceof String ||
                obj instanceof URL;
        }

        /**
         * Gets argument.
         *
         * @param key Key.
         * @return Argument.
         */
        private <T> T argument(String key) {
            return (T)args.get(key);
        }
    }
}
