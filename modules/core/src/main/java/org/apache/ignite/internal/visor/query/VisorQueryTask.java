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

package org.apache.ignite.internal.visor.query;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.processors.timeout.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.visor.*;
import org.apache.ignite.lang.*;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.internal.visor.query.VisorQueryUtils.*;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.*;

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
        return new VisorQueryJob(arg, debug);
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
        private final CacheQueryFuture<R> fut;

        /** Next record from future. */
        private final R next;

        /** Flag indicating that this future was read from last check. */
        private Boolean accessed;

        /**
         * @param fut Future.
         * @param next Next value.
         * @param accessed {@code true} if query was accessed before remove timeout expired.
         */
        public VisorFutureResultSetHolder(CacheQueryFuture<R> fut, R next, Boolean accessed) {
            this.fut = fut;
            this.next = next;
            this.accessed = accessed;
        }

        /**
         * @return Future with query results.
         */
        public CacheQueryFuture<R> future() {
            return fut;
        }

        /**
         * @return Next record from future.
         */
        public R next() {
            return next;
        }

        /**
         * @return Flag indicating that this future was read from last check..
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
         * @param debug Debug flag.
         */
        protected VisorQueryJob(VisorQueryArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected IgniteBiTuple<? extends Exception, VisorQueryResultEx> run(VisorQueryArg arg) {
            try {
                Boolean scan = arg.queryTxt().toUpperCase().startsWith("SCAN");

                String qryId = (scan ? SCAN_QRY_NAME : SQL_QRY_NAME) + "-" +
                    UUID.randomUUID();

                GridCache<Object, Object> c = ignite.cachex(arg.cacheName());

                if (c == null)
                    return new IgniteBiTuple<>(new IgniteCheckedException("Cache not found: " +
                        escapeName(arg.cacheName())), null);

                CacheProjection<Object, Object> cp = c.keepPortable();

                if (scan) {
                    CacheQueryFuture<Map.Entry<Object, Object>> fut = cp.queries().createScanQuery(null)
                        .pageSize(arg.pageSize())
                        .projection(ignite.cluster().forNodeIds(arg.proj()))
                        .execute();

                    long start = U.currentTimeMillis();

                    IgniteBiTuple<List<Object[]>, Map.Entry<Object, Object>> rows =
                        fetchScanQueryRows(fut, null, arg.pageSize());

                    long fetchDuration = U.currentTimeMillis() - start;

                    long duration = fut.duration() + fetchDuration; // Scan duration + fetch duration.

                    Map.Entry<Object, Object> next = rows.get2();

                    ignite.cluster().<String, VisorFutureResultSetHolder>nodeLocalMap().put(qryId,
                        new VisorFutureResultSetHolder<>(fut, next, false));

                    scheduleResultSetHolderRemoval(qryId);

                    return new IgniteBiTuple<>(null, new VisorQueryResultEx(ignite.localNode().id(), qryId,
                        SCAN_COL_NAMES, rows.get1(), next != null, duration));
                }
                else {
                    CacheQueryFuture<List<?>> fut = ((GridCacheQueriesEx<?, ?>)cp.queries())
                        .createSqlFieldsQuery(arg.queryTxt(), true)
                        .pageSize(arg.pageSize())
                        .projection(ignite.cluster().forNodeIds(arg.proj()))
                        .execute();

                    List<Object> firstRow = (List<Object>)fut.next();

                    List<GridQueryFieldMetadata> meta = ((GridCacheQueryMetadataAware)fut).metadata().get();

                    if (meta == null)
                        return new IgniteBiTuple<Exception, VisorQueryResultEx>(
                            new SQLException("Fail to execute query. No metadata available."), null);
                    else {
                        VisorQueryField[] names = new VisorQueryField[meta.size()];

                        for (int i = 0; i < meta.size(); i++) {
                            GridQueryFieldMetadata col = meta.get(i);

                            names[i] = new VisorQueryField(col.typeName(), col.fieldName());
                        }

                        long start = U.currentTimeMillis();

                        IgniteBiTuple<List<Object[]>, List<?>> rows =
                            fetchSqlQueryRows(fut, firstRow, arg.pageSize());

                        long fetchDuration = U.currentTimeMillis() - start;

                        long duration = fut.duration() + fetchDuration; // Query duration + fetch duration.

                        ignite.cluster().<String, VisorFutureResultSetHolder>nodeLocalMap().put(qryId,
                            new VisorFutureResultSetHolder<>(fut, rows.get2(), false));

                        scheduleResultSetHolderRemoval(qryId);

                        return new IgniteBiTuple<>(null, new VisorQueryResultEx(ignite.localNode().id(), qryId,
                            names, rows.get1(), rows.get2() != null, duration));
                    }
                }
            }
            catch (Exception e) {
                return new IgniteBiTuple<>(e, null);
            }
        }

        /**
         * @param id Unique query result id.
         */
        private void scheduleResultSetHolderRemoval(final String id) {
            ((IgniteKernal)ignite).context().timeout().addTimeoutObject(new GridTimeoutObjectAdapter(RMV_DELAY) {
                @Override public void onTimeout() {
                    ConcurrentMap<String, VisorFutureResultSetHolder> storage = ignite.cluster().nodeLocalMap();

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
