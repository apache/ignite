/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.query.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.kernal.visor.cmd.dto.*;
import org.gridgain.grid.kernal.visor.cmd.dto.node.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.indexing.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.sql.*;
import java.util.*;

import static org.gridgain.grid.kernal.visor.cmd.tasks.VisorFieldsQueryUtils.*;

/**
 * Executes SCAN or SQL query and get first page of results.
 */
@GridInternal
public class VisorFieldsQueryTask extends VisorOneNodeTask<VisorFieldsQueryTask.VisorFieldsQueryArg,
    VisorFieldsQueryTask.VisorFieldsQueryTaskResult> {
    /**
     * Arguments for {@link VisorFieldsQueryTask}.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorFieldsQueryArg extends VisorOneNodeArg {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final List<UUID> proj;

        /** */
        private final String cacheName;

        /** */
        private final String qryTxt;

        /** */
        private final Integer pageSize;

        /**
         * @param nodeId Node Id.
         * @param proj
         * @param cacheName
         * @param qryTxt
         * @param pageSize
         */
        public VisorFieldsQueryArg(UUID nodeId, List<UUID> proj, String cacheName, String qryTxt, Integer pageSize) {
            super(nodeId);

            this.proj = proj;
            this.cacheName = cacheName;
            this.qryTxt = qryTxt;
            this.pageSize = pageSize;
        }

        /**
         * @return Proj.
         */
        public List<UUID> proj() {
            return proj;
        }

        /**
         * @return Cache name.
         */
        public String cacheName() {
            return cacheName;
        }

        /**
         * @return Query txt.
         */
        public String queryTxt() {
            return qryTxt;
        }

        /**
         * @return Page size.
         */
        public Integer pageSize() {
            return pageSize;
        }
    }

    @SuppressWarnings("PublicInnerClass")
    public static class VisorFieldsQueryTaskResult extends GridBiTuple<VisorFieldsQueryResult, Exception> {
        public VisorFieldsQueryTaskResult(@Nullable VisorFieldsQueryResult res, @Nullable Exception ex) {
            super(res, ex);
        }
    }

    /**
     * Tuple with iterable sql query future, page size and accessed flag.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorSqlStorageValType implements Serializable {
        private final GridCacheQueryFuture<List<Object>> fut;

        private final List<Object> next;

        private final Boolean accessed;

        public VisorSqlStorageValType(GridCacheQueryFuture<List<Object>> fut, List<Object> next, Boolean accessed) {
            this.fut = fut;
            this.next = next;
            this.accessed = accessed;
        }

        /**
         * @return Future.
         */
        public GridCacheQueryFuture<List<Object>> future() {
            return fut;
        }

        /**
         * @return Next.
         */
        public List<Object> next() {
            return next;
        }

        /**
         * @return Accessed.
         */
        public Boolean accessed() {
            return accessed;
        }
    }

    /**
     * Tuple with iterable scan query future, page size and accessed flag.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorScanStorageValType implements Serializable {
        private final GridCacheQueryFuture<Map.Entry<Object, Object>> fut;

        private final Map.Entry<Object, Object> next;

        private final Boolean accessed;

        public VisorScanStorageValType(GridCacheQueryFuture<Map.Entry<Object, Object>> fut, Map.Entry<Object, Object> next, Boolean accessed) {
            this.fut = fut;
            this.next = next;
            this.accessed = accessed;
        }

        /**
         * @return Future.
         */
        public GridCacheQueryFuture<Map.Entry<Object, Object>> future() {
            return fut;
        }

        /**
         * @return Next.
         */
        public Map.Entry<Object, Object> next() {
            return next;
        }

        /**
         * @return Accessed.
         */
        public Boolean accessed() {
            return accessed;
        }
    }

    @SuppressWarnings("PublicInnerClass")
    public static class VisorFieldsQueryJob
        extends VisorOneNodeJob<VisorFieldsQueryArg, VisorFieldsQueryTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         */
        protected VisorFieldsQueryJob(VisorFieldsQueryArg arg) {
            super(arg);
        }

        @Override
        protected VisorFieldsQueryTaskResult run(VisorFieldsQueryArg arg) throws GridException {
            try {
                Boolean scan = arg.qryTxt.toUpperCase().startsWith("SCAN");

                String qryId = (scan ? SCAN_QRY_NAME : SQL_QRY_NAME) + "-" + UUID.randomUUID();

                GridCache<Object, Object> c = g.cachex(arg.cacheName);

                if (scan) {
                    GridCacheQueryFuture fut = c.queries().createScanQuery(null)
                            .pageSize(arg.pageSize)
                            .projection(g.forNodeIds(arg.proj))
                            .execute();

                    GridBiTuple<List<Object[]>, Map.Entry<Object, Object>> nextRows =
                        fetchScanQueryRows(fut, null, arg.pageSize);

                    g.<String, VisorScanStorageValType>nodeLocalMap().put(qryId,
                        new VisorScanStorageValType(fut, nextRows.get2(), false));

                    scheduleScanQueryRemoval(qryId);

                    return new VisorFieldsQueryTaskResult(new VisorFieldsQueryResult(g.localNode().id(), qryId,
                        SCAN_COL_NAMES, nextRows.get1(), nextRows.get2() != null), null);
                }
                else {
                    GridCacheQueryFuture fut = ((GridCacheQueriesEx<?, ?>) c.queries()).createSqlFieldsQuery(arg.qryTxt, true)
                            .pageSize(arg.pageSize)
                            .projection(g.forNodeIds(arg.proj))
                            .execute();

                    List<Object> firstRow = (List<Object>) fut.next();

                    List<GridIndexingFieldMetadata> meta = ((GridCacheQueryMetadataAware) fut).metadata().get();

                    if (meta == null)
                        return new VisorFieldsQueryTaskResult(null, new SQLException("Fail to execute query. No metadata available."));
                    else {
                        VisorFieldsQueryColumn[] names = new VisorFieldsQueryColumn[meta.size()];

                        for (int i = 0; i < meta.size(); i ++) {
                            GridIndexingFieldMetadata col = meta.get(i);

                            names[i] = new VisorFieldsQueryColumn(col.typeName(), col.fieldName());
                        }

                        GridBiTuple<List<Object[]>, List<Object>> nextRows = fetchSqlQueryRows(fut, firstRow, arg.pageSize);

                        g.<String, VisorSqlStorageValType>nodeLocalMap().put(qryId, new VisorSqlStorageValType(
                            fut, nextRows.get2(), false));

                        scheduleSqlQueryRemoval(qryId);

                        return new VisorFieldsQueryTaskResult(new VisorFieldsQueryResult(g.localNode().id(), qryId,
                            names, nextRows.get1(), nextRows.get2() != null), null);
                    }
                }
            }
            catch (Exception e) { return new VisorFieldsQueryTaskResult(null, e); }
        }

        private void scheduleSqlQueryRemoval(final String id) {
            g.scheduler().scheduleLocal(new CAX() {
                @Override public void applyx() {
                    GridNodeLocalMap<String, VisorSqlStorageValType> storage = g.nodeLocalMap();

                    VisorSqlStorageValType t = storage.get(id);

                    if (t != null) {
                        // If future was accessed since last scheduling,
                        // set access flag to false and reschedule.

                        if (t.accessed()) {
                            storage.put(id, new VisorSqlStorageValType(t.future(), t.next(), false));

                            scheduleSqlQueryRemoval(id);
                        }
                        else
                            storage.remove(id); // Remove stored future otherwise.
                    }
                }
            }, "{" + RMV_DELAY + ", 1} * * * * *");
        }

        private void scheduleScanQueryRemoval(final String id) {
            g.scheduler().scheduleLocal(new CAX() {
                @Override public void applyx() {
                    GridNodeLocalMap<String, VisorScanStorageValType> storage = g.nodeLocalMap();

                    VisorScanStorageValType t = storage.get(id);

                    if (t != null) {
                        // If future was accessed since last scheduling,
                        // set access flag to false and reschedule.

                        if (t.accessed()) {
                            storage.put(id, new VisorScanStorageValType(t.future(), t.next(), false));

                            scheduleScanQueryRemoval(id);
                        }
                        else
                            storage.remove(id); // Remove stored future otherwise.
                    }
                }
            }, "{" + RMV_DELAY + ", 1} * * * * *");
        }
    }

    @Override
    protected VisorJob<VisorFieldsQueryArg, VisorFieldsQueryTaskResult> job(VisorFieldsQueryArg arg) {
        return new VisorFieldsQueryJob(arg);
    }
}
