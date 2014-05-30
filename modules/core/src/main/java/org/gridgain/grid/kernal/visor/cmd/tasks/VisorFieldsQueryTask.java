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
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.query.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.processors.timeout.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.kernal.visor.cmd.dto.*;
import org.gridgain.grid.kernal.visor.cmd.dto.node.*;
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
    T2<? extends Exception, VisorFieldsQueryResultEx>> {
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
     * ResultSet future holder.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorFutureResultSetHolder<R> implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final GridCacheQueryFuture<R> fut;

        private final R next;

        private Boolean accessed;

        public VisorFutureResultSetHolder(GridCacheQueryFuture<R> fut, R next, Boolean accessed) {
            this.fut = fut;
            this.next = next;
            this.accessed = accessed;
        }

        /**
         * @return Future.
         */
        public GridCacheQueryFuture<R> future() {
            return fut;
        }

        /**
         * @return Next.
         */
        public R next() {
            return next;
        }

        /**
         * @return Accessed.
         */
        public Boolean accessed() {
            return accessed;
        }

        /**
         * @param accessed New accessed.
         */
        public void accessed(Boolean accessed) {
            this.accessed = accessed;
        }
    }

    @SuppressWarnings("PublicInnerClass")
    public static class VisorFieldsQueryJob
        extends VisorOneNodeJob<VisorFieldsQueryArg, T2<? extends Exception, VisorFieldsQueryResultEx>> {
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
        protected T2<? extends Exception, VisorFieldsQueryResultEx> run(VisorFieldsQueryArg arg) throws GridException {
            try {
                Boolean scan = arg.queryTxt().toUpperCase().startsWith("SCAN");

                String qryId = (scan ? SCAN_QRY_NAME : SQL_QRY_NAME) + "-" + UUID.randomUUID();

                GridCache<Object, Object> c = g.cachex(arg.cacheName());

                if (scan) {
                    GridCacheQueryFuture<Map.Entry<Object, Object>> fut = c.queries().createScanQuery(null)
                            .pageSize(arg.pageSize())
                            .projection(g.forNodeIds(arg.proj()))
                            .execute();

                    T2<List<Object[]>, Map.Entry<Object, Object>> rows =
                        fetchScanQueryRows(fut, null, arg.pageSize());

                    Map.Entry<Object, Object> next = rows.get2();

                    g.<String, VisorFutureResultSetHolder>nodeLocalMap().put(qryId,
                        new VisorFutureResultSetHolder<>(fut, next, false));

                    scheduleQueryRemoval(qryId);

                    return new T2<>(null, new VisorFieldsQueryResultEx(g.localNode().id(), qryId,
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
                        return new T2<Exception, VisorFieldsQueryResultEx>(new SQLException("Fail to execute query. No metadata available."), null);
                    else {
                        VisorFieldsQueryColumn[] names = new VisorFieldsQueryColumn[meta.size()];

                        for (int i = 0; i < meta.size(); i ++) {
                            GridIndexingFieldMetadata col = meta.get(i);

                            names[i] = new VisorFieldsQueryColumn(col.typeName(), col.fieldName());
                        }

                        T2<List<Object[]>, List<?>> nextRows = fetchSqlQueryRows(fut, firstRow, arg.pageSize());

                        g.<String, VisorFutureResultSetHolder>nodeLocalMap().put(qryId,
                            new VisorFutureResultSetHolder<>(fut, nextRows.get2(), false));

                        scheduleQueryRemoval(qryId);

                        return new T2<>(null, new VisorFieldsQueryResultEx(g.localNode().id(), qryId,
                            names, nextRows.get1(), nextRows.get2() != null));
                    }
                }
            }
            catch (Exception e) { return new T2<>(e, null); }
        }

        private void scheduleQueryRemoval(final String id) {
            ((GridKernal)g).context().timeout().addTimeoutObject(new GridTimeoutObjectAdapter(RMV_DELAY) {
                @Override public void onTimeout() {
                    GridNodeLocalMap<String, VisorFutureResultSetHolder> storage = g.nodeLocalMap();

                    VisorFutureResultSetHolder<?> t = storage.get(id);

                    if (t != null) {
                        // If future was accessed since last scheduling,  set access flag to false and reschedule.
                        if (t.accessed()) {
                            t.accessed(false);

                            scheduleQueryRemoval(id);
                        }
                        else
                            storage.remove(id); // Remove stored future otherwise.
                    }
                }
            });
        }
    }

    @Override
    protected VisorJob<VisorFieldsQueryArg, T2<? extends Exception, VisorFieldsQueryResultEx>> job(VisorFieldsQueryArg arg) {
        return new VisorFieldsQueryJob(arg);
    }
}
