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
import org.apache.ignite.cache.query.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.processors.timeout.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.visor.*;
import org.apache.ignite.lang.*;

import javax.cache.*;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.internal.visor.query.VisorQueryUtils.*;

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
    public static class VisorQueryCursorHolder implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Query cursor. */
        private final VisorQueryCursor cur;

        /** Flag indicating that this future was read from last check. */
        private boolean accessed;

        /**
         * @param cur Future.
         * @param accessed {@code true} if query was accessed before remove timeout expired.
         */
        public VisorQueryCursorHolder(VisorQueryCursor cur, boolean accessed) {
            this.cur = cur;
            this.accessed = accessed;
        }

        /**
         * @return Query cursor.
         */
        public VisorQueryCursor cursor() {
            return cur;
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

                GridCacheProcessor cacheProcessor = ignite.context().cache();

                IgniteCache<Object, Object> c = cacheProcessor.privateJCache(arg.cacheName(), false);

                if (scan) {
                    ScanQuery<Object, Object> qry = new ScanQuery<>(null);
                    qry.setPageSize(arg.pageSize());

                    long start = U.currentTimeMillis();

                    VisorQueryCursor<Cache.Entry<Object, Object>> cur = new VisorQueryCursor<>(c.query(qry));

                    List<Object[]> rows = fetchScanQueryRows(cur, arg.pageSize());

                    long duration = U.currentTimeMillis() - start; // Scan duration + fetch duration.

                    ignite.cluster().<String, VisorQueryCursorHolder>nodeLocalMap().put(qryId,
                        new VisorQueryCursorHolder(cur, false));

                    scheduleResultSetHolderRemoval(qryId);

                    return new IgniteBiTuple<>(null, new VisorQueryResultEx(ignite.localNode().id(), qryId,
                        SCAN_COL_NAMES, rows, cur.hasNext(), duration));
                }
                else {
                    SqlFieldsQuery qry = new SqlFieldsQuery(arg.queryTxt());
                    qry.setPageSize(arg.pageSize());

                    long start = U.currentTimeMillis();

                    VisorQueryCursor<List<?>> cur = new VisorQueryCursor<>(c.query(qry));

                    Collection<GridQueryFieldMetadata> meta = cur.fieldsMeta();

                    if (meta == null)
                        return new IgniteBiTuple<Exception, VisorQueryResultEx>(
                            new SQLException("Fail to execute query. No metadata available."), null);
                    else {
                        List<VisorQueryField> names = new ArrayList<>(meta.size());

                        for (GridQueryFieldMetadata col : meta)
                            names.add(new VisorQueryField(col.typeName(), col.fieldName()));

                        List<Object[]> rows = fetchSqlQueryRows(cur, arg.pageSize());

                        long duration = U.currentTimeMillis() - start; // Query duration + fetch duration.

                        ignite.cluster().<String, VisorQueryCursorHolder>nodeLocalMap().put(qryId, new VisorQueryCursorHolder(cur, false));

                        scheduleResultSetHolderRemoval(qryId);

                        return new IgniteBiTuple<>(null, new VisorQueryResultEx(ignite.localNode().id(), qryId,
                            names, rows, cur.hasNext(), duration));
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
            ignite.context().timeout().addTimeoutObject(new GridTimeoutObjectAdapter(RMV_DELAY) {
                @Override public void onTimeout() {
                    ConcurrentMap<String, VisorQueryCursorHolder> storage = ignite.cluster().nodeLocalMap();

                    VisorQueryCursorHolder holder = storage.get(id);

                    if (holder != null) {
                        // If future was accessed since last scheduling,  set access flag to false and reschedule.
                        if (holder.accessed()) {
                            holder.accessed(false);

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
