/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query.jdbc;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.query.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.marshaller.jdk.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.indexing.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.math.*;
import java.net.*;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.compute.GridComputeJobResultPolicy.*;

/**
 * Task for JDBC adapter.
 */
public class GridCacheQueryJdbcTask extends GridComputeTaskAdapter<byte[], byte[]> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Marshaller. */
    private static final GridMarshaller MARSHALLER = new GridJdkMarshaller();

    /** How long to store future (10 minutes). */
    private static final int RMV_DELAY = 10 * 60;

    /** Scheduler. */
    private static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1);

    /** {@inheritDoc} */
    @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, byte[] arg) throws GridException {
        assert arg != null;

        Map<String, Object> args = MARSHALLER.unmarshal(arg, null);

        boolean first = true;

        UUID nodeId = (UUID)args.remove("confNodeId");

        if (nodeId == null) {
            nodeId = (UUID)args.remove("nodeId");

            first = nodeId == null;
        }

        if (nodeId != null) {
            for (GridNode n : subgrid)
                if (n.id().equals(nodeId))
                    return F.asMap(new JdbcDriverJob(args, first), n);

            throw new GridException("Node doesn't exist or left the grid: " + nodeId);
        }
        else {
            String cache = (String)args.get("cache");

            for (GridNode n : subgrid)
                if (U.hasCache(n, cache))
                    return F.asMap(new JdbcDriverJob(args, first), n);

            throw new GridException("Can't find node with cache: " + cache);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] reduce(List<GridComputeJobResult> results) throws GridException {
        byte status;
        byte[] bytes;

        GridComputeJobResult res = F.first(results);

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
    @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> rcvd) throws GridException {
        return WAIT;
    }

    /**
     * Job for JDBC adapter.
     */
    private static class JdbcDriverJob extends GridComputeJobAdapter implements GridOptimizedMarshallable {
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
        @GridInstanceResource
        private Grid grid;

        /** Logger. */
        @GridLoggerResource
        private GridLogger log;

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
        @Override public Object execute() throws GridException {
            String cacheName = argument("cache");
            String sql = argument("sql");
            Long timeout = argument("timeout");
            List<Object> args = argument("args");
            UUID futId = argument("futId");
            Integer pageSize = argument("pageSize");
            Integer maxRows = argument("maxRows");

            assert pageSize != null;
            assert maxRows != null;

            GridTuple4<GridCacheQueryFuture<List<?>>, Integer, Boolean, Collection<String>> t = null;

            Collection<String> tbls = null;
            Collection<String> cols;
            Collection<String> types = null;

            if (first) {
                assert sql != null;
                assert timeout != null;
                assert args != null;
                assert futId == null;

                GridCache<?, ?> cache = ((GridEx)grid).cachex(cacheName);

                GridCacheQuery<List<?>> qry =
                    ((GridCacheQueriesEx<?, ?>)cache.queries()).createSqlFieldsQuery(sql, true);

                qry.pageSize(pageSize);
                qry.timeout(timeout);

                // Query local and replicated caches only locally.
                if (cache.configuration().getCacheMode() != PARTITIONED)
                    qry = qry.projection(grid.forLocal());

                GridCacheQueryFuture<List<?>> fut = qry.execute(args.toArray());

                Collection<GridIndexingFieldMetadata> meta = ((GridCacheQueryMetadataAware)fut).metadata().get();

                if (meta == null) {
                    // Try to extract initial SQL exception.
                    try {
                        fut.get();
                    }
                    catch (GridException e) {
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

                for (GridIndexingFieldMetadata desc : meta) {
                    tbls.add(desc.typeName());
                    cols.add(desc.fieldName().toUpperCase());
                    types.add(desc.fieldTypeName());
                }

                futId = UUID.randomUUID();

                grid.nodeLocalMap().put(futId, t = F.t(fut, 0, false, cols));

                scheduleRemoval(futId);
            }

            assert futId != null;

            if (t == null)
                t = grid.<UUID, GridTuple4<GridCacheQueryFuture<List<?>>, Integer, Boolean,
                    Collection<String>>>nodeLocalMap().get(futId);

            assert t != null;

            cols = t.get4();

            Collection<List<Object>> fields = new LinkedList<>();

            GridCacheQueryFuture<List<?>> fut = t.get1();

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
                grid.nodeLocalMap().put(futId, F.t(fut, totalCnt, true, cols));
            else
                grid.nodeLocalMap().remove(futId);

            return first ? F.asList(grid.localNode().id(), futId, tbls, cols, types, fields, finished) :
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
                    GridTuple3<GridCacheQueryFuture<List<?>>, Integer, Boolean> t =
                        grid.<UUID, GridTuple3<GridCacheQueryFuture<List<?>>, Integer, Boolean>>nodeLocalMap().get(id);

                    if (t != null) {
                        // If future was accessed since last scheduling,
                        // set access flag to false and reschedule.
                        if (t.get3()) {
                            t.set3(false);

                            scheduleRemoval(id);
                        }
                        // Remove stored future otherwise.
                        else
                            grid.nodeLocalMap().remove(id);
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
