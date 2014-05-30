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

import java.io.*;
import java.sql.*;
import java.util.*;

import static org.gridgain.grid.kernal.visor.cmd.tasks.VisorFieldsQueryUtils.*;

/**
 * Executes SCAN or SQL query and get first page of results.
 */
@GridInternal
public class VisorFieldsQueryTask extends VisorOneNodeTask<VisorFieldsQueryTask.VisorFieldsQueryArg,
    GridBiTuple<? extends Exception, VisorFieldsQueryResultEx>> {
    /**
     * Arguments for {@link VisorFieldsQueryTask}.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorFieldsQueryArg extends VisorOneNodeArg {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final Collection<UUID> proj;

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
        public VisorFieldsQueryArg(UUID nodeId, Collection<UUID> proj, String cacheName, String qryTxt, Integer pageSize) {
            super(nodeId);

            this.proj = proj;
            this.cacheName = cacheName;
            this.qryTxt = qryTxt;
            this.pageSize = pageSize;
        }

        /**
         * @return Proj.
         */
        public Collection<UUID> proj() {
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

    /**
     * Tuple with iterable sql query future, page size and accessed flag.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorSqlStorageValType implements Serializable {
        private final GridCacheQueryFuture<List<?>> fut;

        private final List<?> next;

        private final Boolean accessed;

        public VisorSqlStorageValType(GridCacheQueryFuture<List<?>> fut, List<?> next, Boolean accessed) {
            this.fut = fut;
            this.next = next;
            this.accessed = accessed;
        }

        /**
         * @return Future.
         */
        public GridCacheQueryFuture<List<?>> future() {
            return fut;
        }

        /**
         * @return Next.
         */
        public List<?> next() {
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
        extends VisorOneNodeJob<VisorFieldsQueryArg, GridBiTuple<? extends Exception, VisorFieldsQueryResultEx>> {
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
        protected GridBiTuple<? extends Exception, VisorFieldsQueryResultEx> run(VisorFieldsQueryArg arg) throws GridException {
            try {
                Boolean scan = arg.queryTxt().toUpperCase().startsWith("SCAN");

                String qryId = (scan ? SCAN_QRY_NAME : SQL_QRY_NAME) + "-" + UUID.randomUUID();

                GridCache<Object, Object> c = g.cachex(arg.cacheName());

                if (scan) {
                    GridCacheQueryFuture<Map.Entry<Object, Object>> fut = c.queries().createScanQuery(null)
                            .pageSize(arg.pageSize())
                            .projection(g.forNodeIds(arg.proj()))
                            .execute();

                    GridBiTuple<List<Object[]>, Map.Entry<Object, Object>> rows =
                        fetchScanQueryRows(fut, null, arg.pageSize());

                    Map.Entry<Object, Object> next = rows.get2();

                    g.<String, VisorScanStorageValType>nodeLocalMap().put(qryId,
                        new VisorScanStorageValType(fut, next, false));

                    scheduleScanQueryRemoval(qryId);

                    return new GridBiTuple<>(null, new VisorFieldsQueryResultEx(g.localNode().id(), qryId,
                        SCAN_COL_NAMES, rows.get1(), next != null));
                }
                else {
                    GridCacheQueryFuture<List<?>> fut = ((GridCacheQueriesEx<?, ?>) c.queries()).createSqlFieldsQuery(arg.queryTxt(), true)
                            .pageSize(arg.pageSize())
                            .projection(g.forNodeIds(arg.proj()))
                            .execute();

                    List<Object> firstRow = (List<Object>)fut.next();

                    List<GridIndexingFieldMetadata> meta = ((GridCacheQueryMetadataAware) fut).metadata().get();

                    if (meta == null)
                        return new GridBiTuple<Exception, VisorFieldsQueryResultEx>(new SQLException("Fail to execute query. No metadata available."), null);
                    else {
                        VisorFieldsQueryColumn[] names = new VisorFieldsQueryColumn[meta.size()];

                        for (int i = 0; i < meta.size(); i ++) {
                            GridIndexingFieldMetadata col = meta.get(i);

                            names[i] = new VisorFieldsQueryColumn(col.typeName(), col.fieldName());
                        }

                        GridBiTuple<List<Object[]>, List<?>> nextRows = fetchSqlQueryRows(fut, firstRow, arg.pageSize());

                        g.<String, VisorSqlStorageValType>nodeLocalMap().put(qryId, new VisorSqlStorageValType(
                            fut, nextRows.get2(), false));

                        scheduleSqlQueryRemoval(qryId);

                        return new GridBiTuple<>(null, new VisorFieldsQueryResultEx(g.localNode().id(), qryId,
                            names, nextRows.get1(), nextRows.get2() != null));
                    }
                }
            }
            catch (Exception e) { return new GridBiTuple<>(e, null); }
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
    protected VisorJob<VisorFieldsQueryArg, GridBiTuple<? extends Exception, VisorFieldsQueryResultEx>> job(VisorFieldsQueryArg arg) {
        return new VisorFieldsQueryJob(arg);
    }
}
