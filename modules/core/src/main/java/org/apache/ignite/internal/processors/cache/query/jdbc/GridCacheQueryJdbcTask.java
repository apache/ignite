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

import java.math.BigDecimal;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

import static org.apache.ignite.compute.ComputeJobResultPolicy.WAIT;

/**
 * Task for JDBC adapter.
 */
public class GridCacheQueryJdbcTask extends ComputeTaskAdapter<byte[], byte[]> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Marshaller. */
    private static final Marshaller MARSHALLER = new JdkMarshaller();

    /** How long to store future (10 minutes). */
    private static final int RMV_DELAY = 10 * 60 * 1000;

    /** Scheduler. */
    private static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1);

    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, byte[] arg) {
        try {
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

                throw new IgniteException("Node doesn't exist or left the grid: " + nodeId);
            }
            else {
                String cache = (String)args.get("cache");

                GridDiscoveryManager discoMgr = ((IgniteKernal)ignite).context().discovery();

                for (ClusterNode n : subgrid) {
                    if (discoMgr.cacheAffinityNode(n, cache))
                        return F.asMap(new JdbcDriverJob(args, first), n);
                }

                throw new IgniteException("Can't find node with cache: " + cache);
            }
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] reduce(List<ComputeJobResult> results) throws IgniteException {
        try {
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
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        return WAIT;
    }

    /**
     * Job for JDBC adapter.
     */
    private static class JdbcDriverJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** Arguments. */
        private final Map<String, Object> args;

        /** First execution flag. */
        private final boolean first;

        /** Grid instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Logger. */
        @LoggerResource
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
        @Override public Object execute() {
            String cacheName = argument("cache");
            String sql = argument("sql");
            Long timeout = argument("timeout");
            List<Object> args = argument("args");
            UUID futId = argument("futId");
            final int pageSize = argument("pageSize");
            final int maxRows = argument("maxRows");

            assert maxRows >= 0 : maxRows;

            Cursor c = null;

            Collection<String> tbls = null;
            Collection<String> cols = null;
            Collection<String> types = null;

            if (first) {
                assert sql != null;
                assert timeout != null;
                assert args != null;
                assert futId == null;

                IgniteCache<?, ?> cache = ignite.cache(cacheName);

                SqlFieldsQuery qry = new SqlFieldsQuery(sql).setArgs(args.toArray());

                qry.setPageSize(pageSize);

                QueryCursor<List<?>> cursor = cache.query(qry);

                Collection<GridQueryFieldMetadata> meta = ((QueryCursorImpl<List<?>>)cursor).fieldsMeta();

                assert meta != null;

                tbls = new ArrayList<>(meta.size());
                cols = new ArrayList<>(meta.size());
                types = new ArrayList<>(meta.size());

                for (GridQueryFieldMetadata desc : meta) {
                    tbls.add(desc.typeName());
                    cols.add(desc.fieldName().toUpperCase());
                    types.add(desc.fieldTypeName());
                }

                futId = UUID.randomUUID();

                c = new Cursor(cursor, cursor.iterator(), 0, U.currentTimeMillis());
            }

            assert futId != null;

            ConcurrentMap<UUID,Cursor> m = ignite.cluster().nodeLocalMap();

            if (c == null)
                c = m.get(futId);

            if (c == null)
                throw new IgniteException("Cursor was removed due to long inactivity.");

            Collection<List<?>> rows = new ArrayList<>();

            int totalCnt = c.totalCnt;

            boolean finished = true;

            for (List<?> row : c) {
                List<Object> row0 = new ArrayList<>(row.size());

                for (Object val : row)
                    row0.add(sqlType(val) ? val : val.toString());

                rows.add(row0);

                if (++totalCnt == maxRows) // If maxRows is 0 then unlimited
                    break;

                if (rows.size() == pageSize) {
                    finished = false;

                    break;
                }
            }

            if (!finished) {
                if (first) {
                    m.put(futId, c);

                    scheduleRemoval(futId, RMV_DELAY);
                }
                else if (!m.replace(futId, c, new Cursor(c.cursor, c.iter, totalCnt, U.currentTimeMillis())))
                    assert !m.containsKey(futId) : "Concurrent cursor modification.";
            }
            else if (first) // No need to remove.
                c.cursor.close();
            else
                remove(futId, c);

            return first ? F.asList(ignite.cluster().localNode().id(), futId, tbls, cols, types, rows, finished) :
                F.asList(rows, finished);
        }

        /**
         * @param futId Cursor ID.
         * @param c Cursor.
         * @return {@code true} If succeeded.
         */
        private boolean remove(UUID futId, Cursor c) {
            if (ignite.cluster().<UUID,Cursor>nodeLocalMap().remove(futId, c)) {
                c.cursor.close();

                return true;
            }

            return false;
        }

        /**
         * Schedules removal of stored future.
         *
         * @param id Future ID.
         * @param delay Delay in milliseconds.
         */
        private void scheduleRemoval(final UUID id, long delay) {
            SCHEDULER.schedule(new CAX() {
                @Override public void applyx() {
                    for (;;) {
                        Cursor c = ignite.cluster().<UUID,Cursor>nodeLocalMap().get(id);

                        if (c == null)
                            break;

                        // If the cursor was accessed since last scheduling then reschedule.
                        long untouchedTime = U.currentTimeMillis() - c.lastAccessTime;

                        if (untouchedTime < RMV_DELAY) {
                            scheduleRemoval(id, RMV_DELAY - untouchedTime);

                            break;
                        }
                        else if (remove(id, c))
                            break;
                    }
                }
            }, delay, TimeUnit.MILLISECONDS);
        }

        /**
         * Checks whether type of the object is SQL-complaint.
         *
         * @param obj Object.
         * @return Whether type of the object is SQL-complaint.
         */
        private static boolean sqlType(Object obj) {
            return obj == null ||
                obj instanceof BigDecimal ||
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

    /**
     * Cursor.
     */
    private static final class Cursor implements Iterable<List<?>> {
        /** */
        final QueryCursor<List<?>> cursor;

        /** */
        final Iterator<List<?>> iter;

        /** */
        final int totalCnt;

        /** */
        final long lastAccessTime;

        /**
         * @param cursor Cursor.
         * @param iter Iterator.
         * @param totalCnt Total row count already fetched.
         * @param lastAccessTime Last cursor access timestamp.
         */
        private Cursor(QueryCursor<List<?>> cursor, Iterator<List<?>> iter, int totalCnt, long lastAccessTime) {
            this.cursor = cursor;
            this.iter = iter;
            this.totalCnt = totalCnt;
            this.lastAccessTime = lastAccessTime;
        }

        /** {@inheritDoc} */
        @Override public Iterator<List<?>> iterator() {
            return iter;
        }
    }
}