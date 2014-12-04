/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.query;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.GridCache;
import org.gridgain.grid.cache.query.GridCacheQueryFuture;
import org.gridgain.grid.kernal.GridKernal;
import org.gridgain.grid.kernal.processors.cache.query.*;
import org.gridgain.grid.kernal.processors.task.GridInternal;
import org.gridgain.grid.kernal.processors.timeout.GridTimeoutObjectAdapter;
import org.gridgain.grid.kernal.visor.*;
import org.apache.ignite.lang.IgniteBiTuple;
import org.gridgain.grid.spi.indexing.GridIndexingFieldMetadata;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.sql.SQLException;
import java.util.*;

import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

/**
 * Task for execute SCAN or SQL query and get first page of results.
 */
@GridInternal
public class VisorQueryTask extends VisorOneNodeTask<VisorQueryTask.VisorQueryArg,
    IgniteBiTuple<? extends Exception, VisorQueryResultEx>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorQueryJob job(VisorQueryArg arg) {
        return new VisorQueryJob(arg);
    }

    /**
     * Arguments for {@link VisorQueryTask}.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorQueryArg implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Node ids for query. */
        private final Collection<UUID> proj;

        /** Cache name for query. */
        private final String cacheName;

        /** Query text. */
        private final String qryTxt;

        /** Result batch size. */
        private final Integer pageSize;

        /**
         * @param proj Node ids for query.
         * @param cacheName Cache name for query.
         * @param qryTxt Query text.
         * @param pageSize Result batch size.
         */
        public VisorQueryArg(Collection<UUID> proj, String cacheName, String qryTxt, Integer pageSize) {
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

        /** Future with query results. */
        private final GridCacheQueryFuture<R> fut;

        /** Next record from future. */
        private final R next;

        /** Flag indicating that this furure was read from last check. */
        private Boolean accessed;

        public VisorFutureResultSetHolder(GridCacheQueryFuture<R> fut, R next, Boolean accessed) {
            this.fut = fut;
            this.next = next;
            this.accessed = accessed;
        }

        /**
         * @return Future with query results.
         */
        public GridCacheQueryFuture<R> future() {
            return fut;
        }

        /**
         * @return Next record from future.
         */
        public R next() {
            return next;
        }

        /**
         * @return Flag indicating that this furure was read from last check..
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

    /**
     * Job for execute SCAN or SQL query and get first page of results.
     */
    private static class VisorQueryJob extends
        VisorJob<VisorQueryArg, IgniteBiTuple<? extends Exception, VisorQueryResultEx>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         */
        protected VisorQueryJob(VisorQueryArg arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected IgniteBiTuple<? extends Exception, VisorQueryResultEx> run(VisorQueryArg arg)
            throws GridException {
            try {
                Boolean scan = arg.queryTxt().toUpperCase().startsWith("SCAN");

                String qryId = (scan ? VisorQueryUtils.SCAN_QRY_NAME : VisorQueryUtils.SQL_QRY_NAME) + "-" + UUID.randomUUID();

                GridCache<Object, Object> c = g.cachex(arg.cacheName());

                if (c == null)
                    return new IgniteBiTuple<>(new GridException("Cache not found: " + escapeName(arg.cacheName())), null);

                if (scan) {
                    GridCacheQueryFuture<Map.Entry<Object, Object>> fut = c.queries().createScanQuery(null)
                        .pageSize(arg.pageSize())
                        .projection(g.forNodeIds(arg.proj()))
                        .execute();

                    long start = U.currentTimeMillis();

                    IgniteBiTuple<List<Object[]>, Map.Entry<Object, Object>> rows =
                        VisorQueryUtils.fetchScanQueryRows(fut, null, arg.pageSize());

                    long fetchDuration = U.currentTimeMillis() - start;

                    long duration = fut.duration() + fetchDuration; // Scan duration + fetch duration.

                    Map.Entry<Object, Object> next = rows.get2();

                    g.<String, VisorFutureResultSetHolder>nodeLocalMap().put(qryId,
                        new VisorFutureResultSetHolder<>(fut, next, false));

                    scheduleResultSetHolderRemoval(qryId);

                    return new IgniteBiTuple<>(null, new VisorQueryResultEx(g.localNode().id(), qryId,
                        VisorQueryUtils.SCAN_COL_NAMES, rows.get1(), next != null, duration));
                }
                else {
                    GridCacheQueryFuture<List<?>> fut = ((GridCacheQueriesEx<?, ?>)c.queries())
                        .createSqlFieldsQuery(arg.queryTxt(), true)
                        .pageSize(arg.pageSize())
                        .projection(g.forNodeIds(arg.proj()))
                        .execute();

                    List<Object> firstRow = (List<Object>)fut.next();

                    List<GridIndexingFieldMetadata> meta = ((GridCacheQueryMetadataAware)fut).metadata().get();

                    if (meta == null)
                        return new IgniteBiTuple<Exception, VisorQueryResultEx>(
                            new SQLException("Fail to execute query. No metadata available."), null);
                    else {
                        VisorQueryField[] names = new VisorQueryField[meta.size()];

                        for (int i = 0; i < meta.size(); i++) {
                            GridIndexingFieldMetadata col = meta.get(i);

                            names[i] = new VisorQueryField(col.typeName(), col.fieldName());
                        }

                        long start = U.currentTimeMillis();

                        IgniteBiTuple<List<Object[]>, List<?>> rows = VisorQueryUtils.fetchSqlQueryRows(fut, firstRow, arg.pageSize());

                        long fetchDuration = U.currentTimeMillis() - start;

                        long duration = fut.duration() + fetchDuration; // Query duration + fetch duration.

                        g.<String, VisorFutureResultSetHolder>nodeLocalMap().put(qryId,
                            new VisorFutureResultSetHolder<>(fut, rows.get2(), false));

                        scheduleResultSetHolderRemoval(qryId);

                        return new IgniteBiTuple<>(null, new VisorQueryResultEx(g.localNode().id(), qryId,
                            names, rows.get1(), rows.get2() != null, duration));
                    }
                }
            }
            catch (Exception e) {
                return new IgniteBiTuple<>(e, null);
            }
        }

        /**
         *
         * @param id Uniq query result id.
         */
        private void scheduleResultSetHolderRemoval(final String id) {
            ((GridKernal)g).context().timeout().addTimeoutObject(new GridTimeoutObjectAdapter(VisorQueryUtils.RMV_DELAY) {
                @Override public void onTimeout() {
                    ClusterNodeLocalMap<String, VisorFutureResultSetHolder> storage = g.nodeLocalMap();

                    VisorFutureResultSetHolder<?> t = storage.get(id);

                    if (t != null) {
                        // If future was accessed since last scheduling,  set access flag to false and reschedule.
                        if (t.accessed()) {
                            t.accessed(false);

                            scheduleResultSetHolderRemoval(id);
                        }
                        else
                            storage.remove(id); // Remove stored future otherwise.
                    }
                }
            });
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorQueryJob.class, this);
        }
    }
}
