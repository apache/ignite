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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorEither;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.internal.visor.util.VisorExceptionWrapper;
import org.apache.ignite.lang.IgniteBiPredicate;

import static org.apache.ignite.internal.visor.query.VisorQueryUtils.SCAN_COL_NAMES;
import static org.apache.ignite.internal.visor.query.VisorQueryUtils.SCAN_QRY_NAME;
import static org.apache.ignite.internal.visor.query.VisorQueryUtils.fetchScanQueryRows;
import static org.apache.ignite.internal.visor.query.VisorQueryUtils.scheduleResultSetHolderRemoval;

/**
 * Task for execute SCAN query and get first page of results.
 */
@GridInternal
public class VisorScanQueryTask extends VisorOneNodeTask<VisorScanQueryTaskArg, VisorEither<VisorQueryResult>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorScanQueryJob job(VisorScanQueryTaskArg arg) {
        return new VisorScanQueryJob(arg, debug);
    }

    /**
     * Job for execute SCAN query and get first page of results.
     */
    private static class VisorScanQueryJob extends VisorJob<VisorScanQueryTaskArg, VisorEither<VisorQueryResult>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Debug flag.
         */
        private VisorScanQueryJob(VisorScanQueryTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /**
         * Execute scan query.
         *
         * @param c Cache to scan.
         * @param arg Job argument with query parameters.
         * @return Query cursor.
         */
        private QueryCursor<Cache.Entry<Object, Object>> scan(IgniteCache<Object, Object> c, VisorScanQueryTaskArg arg,
            IgniteBiPredicate<Object, Object> filter) {
            ScanQuery<Object, Object> qry = new ScanQuery<>(filter);
            qry.setPageSize(arg.getPageSize());
            qry.setLocal(arg.isLocal());

            return c.withKeepBinary().query(qry);
        }

        /**
         * Scan near cache.
         *
         * @param c Cache to scan near entries.
         * @return Cache entries iterator wrapped with query cursor.
         */
        private QueryCursor<Cache.Entry<Object, Object>> near(IgniteCache<Object, Object> c) {
            return new VisorNearCacheCursor<>(c.localEntries(CachePeekMode.NEAR).iterator());
        }

        /** {@inheritDoc} */
        @Override protected VisorEither<VisorQueryResult> run(final VisorScanQueryTaskArg arg) {
            try {
                IgniteCache<Object, Object> c = ignite.context().cache().jcache(arg.getCacheName());
                UUID nid = ignite.localNode().id();

                String filterText = arg.getFilter();

                long start = U.currentTimeMillis();

                IgniteBiPredicate<Object, Object> filter = null;

                if (!F.isEmpty(filterText))
                    filter = new VisorQueryScanRegexFilter(arg.isCaseSensitive(), arg.isRegEx(), filterText);

                VisorQueryCursor<Cache.Entry<Object, Object>> cur =
                    new VisorQueryCursor<>(arg.isNear() ? near(c) : scan(c, arg, filter));

                List<Object[]> rows = fetchScanQueryRows(cur, arg.getPageSize());

                long duration = U.currentTimeMillis() - start; // Scan duration + fetch duration.

                boolean hasNext = cur.hasNext();

                // Generate query ID to store query cursor in node local storage.
                String qryId = SCAN_QRY_NAME + "-" + UUID.randomUUID();

                if (hasNext) {
                    ignite.cluster().<String, VisorQueryCursor>nodeLocalMap().put(qryId, cur);

                    scheduleResultSetHolderRemoval(qryId, ignite);
                }
                else
                    cur.close();

                return new VisorEither<>(new VisorQueryResult(nid, qryId, SCAN_COL_NAMES, rows, hasNext,
                    duration));
            }
            catch (Throwable e) {
                return new VisorEither<>(new VisorExceptionWrapper(e));
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorScanQueryJob.class, this);
        }

        /**
         * Wrapper for cache iterator to behave like {@link QueryCursor}.
         */
        private static class VisorNearCacheCursor<T> implements QueryCursor<T> {
            /** Wrapped iterator.  */
            private final Iterator<T> it;

            /**
             * Wrapping constructor.
             *
             * @param it Near cache iterator to wrap.
             */
            private VisorNearCacheCursor(Iterator<T> it) {
                this.it = it;
            }

            /** {@inheritDoc} */
            @Override public List<T> getAll() {
                List<T> all = new ArrayList<>();

                while(it.hasNext())
                    all.add(it.next());

                return all;
            }

            /** {@inheritDoc} */
            @Override public void close() {
                // Nothing to close.
            }

            /** {@inheritDoc} */
            @Override public Iterator<T> iterator() {
                return it;
            }
        }
    }
}
