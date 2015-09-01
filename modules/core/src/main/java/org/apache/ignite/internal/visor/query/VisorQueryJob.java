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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.util.VisorExceptionWrapper;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.visor.query.VisorQueryUtils.RMV_DELAY;
import static org.apache.ignite.internal.visor.query.VisorQueryUtils.SCAN_COL_NAMES;
import static org.apache.ignite.internal.visor.query.VisorQueryUtils.SCAN_QRY_NAME;
import static org.apache.ignite.internal.visor.query.VisorQueryUtils.SQL_QRY_NAME;
import static org.apache.ignite.internal.visor.query.VisorQueryUtils.fetchScanQueryRows;
import static org.apache.ignite.internal.visor.query.VisorQueryUtils.fetchSqlQueryRows;

/**
 * Job for execute SCAN or SQL query and get first page of results.
 */
public class VisorQueryJob extends VisorJob<VisorQueryArg, IgniteBiTuple<? extends VisorExceptionWrapper, VisorQueryResultEx>> {
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

    /**
     * @param cacheName Cache name.
     * @return Cache to execute query.
     */
    protected IgniteCache<Object, Object> cache(String cacheName) {
        GridCacheProcessor cacheProcessor = ignite.context().cache();

        return cacheProcessor.jcache(cacheName);
    }

    /** {@inheritDoc} */
    @Override protected IgniteBiTuple<? extends VisorExceptionWrapper, VisorQueryResultEx> run(VisorQueryArg arg) {
        try {
            UUID nid = ignite.localNode().id();

            boolean scan = arg.queryTxt() == null;

            String qryId = (scan ? SCAN_QRY_NAME : SQL_QRY_NAME) + "-" +
                UUID.randomUUID();

            IgniteCache<Object, Object> c = cache(arg.cacheName());

            if (scan) {
                ScanQuery<Object, Object> qry = new ScanQuery<>(null);
                qry.setPageSize(arg.pageSize());
                qry.setLocal(arg.local());

                long start = U.currentTimeMillis();

                VisorQueryCursor<Cache.Entry<Object, Object>> cur = new VisorQueryCursor<>(c.query(qry));

                List<Object[]> rows = fetchScanQueryRows(cur, arg.pageSize());

                long duration = U.currentTimeMillis() - start; // Scan duration + fetch duration.

                boolean hasNext = cur.hasNext();

                if (hasNext) {
                    ignite.cluster().<String, VisorQueryCursor>nodeLocalMap().put(qryId, cur);

                    scheduleResultSetHolderRemoval(qryId);
                }
                else
                    cur.close();

                return new IgniteBiTuple<>(null, new VisorQueryResultEx(nid, qryId, SCAN_COL_NAMES, rows, hasNext,
                    duration));
            }
            else {
                SqlFieldsQuery qry = new SqlFieldsQuery(arg.queryTxt());
                qry.setPageSize(arg.pageSize());
                qry.setLocal(arg.local());

                long start = U.currentTimeMillis();

                VisorQueryCursor<List<?>> cur = new VisorQueryCursor<>(c.query(qry));

                Collection<GridQueryFieldMetadata> meta = cur.fieldsMeta();

                if (meta == null)
                    return new IgniteBiTuple<>(
                        new VisorExceptionWrapper(new SQLException("Fail to execute query. No metadata available.")), null);
                else {
                    List<VisorQueryField> names = new ArrayList<>(meta.size());

                    for (GridQueryFieldMetadata col : meta)
                        names.add(new VisorQueryField(col.schemaName(), col.typeName(),
                            col.fieldName(), col.fieldTypeName()));

                    List<Object[]> rows = fetchSqlQueryRows(cur, arg.pageSize());

                    long duration = U.currentTimeMillis() - start; // Query duration + fetch duration.

                    boolean hasNext = cur.hasNext();

                    if (hasNext) {
                        ignite.cluster().<String, VisorQueryCursor<List<?>>>nodeLocalMap().put(qryId, cur);

                        scheduleResultSetHolderRemoval(qryId);
                    }
                    else
                        cur.close();

                    return new IgniteBiTuple<>(null, new VisorQueryResultEx(nid, qryId, names, rows, hasNext, duration));
                }
            }
        }
        catch (Exception e) {
            return new IgniteBiTuple<>(new VisorExceptionWrapper(e), null);
        }
    }

    /**
     * @param qryId Unique query result id.
     */
    private void scheduleResultSetHolderRemoval(final String qryId) {
        ignite.context().timeout().addTimeoutObject(new GridTimeoutObjectAdapter(RMV_DELAY) {
            @Override public void onTimeout() {
                ConcurrentMap<String, VisorQueryCursor> storage = ignite.cluster().nodeLocalMap();

                VisorQueryCursor cur = storage.get(qryId);

                if (cur != null) {
                    // If cursor was accessed since last scheduling, set access flag to false and reschedule.
                    if (cur.accessed()) {
                        cur.accessed(false);

                        scheduleResultSetHolderRemoval(qryId);
                    }
                    else {
                        // Remove stored cursor otherwise.
                        storage.remove(qryId);

                        cur.close();
                    }
                }
            }
        });
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorQueryJob.class, this);
    }
}