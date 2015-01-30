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
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.visor.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.util.*;

/**
 *  Task for collecting next page previously executed SQL or SCAN query.
 */
@GridInternal
public class VisorQueryNextPageTask extends VisorOneNodeTask<IgniteBiTuple<String, Integer>, VisorQueryResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorQueryNextPageJob job(IgniteBiTuple<String, Integer> arg) {
        return new VisorQueryNextPageJob(arg, debug);
    }

    /**
     * Job for collecting next page previously executed SQL or SCAN query.
     */
    private static class VisorQueryNextPageJob extends VisorJob<IgniteBiTuple<String, Integer>, VisorQueryResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Debug flag.
         */
        private VisorQueryNextPageJob(IgniteBiTuple<String, Integer> arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorQueryResult run(IgniteBiTuple<String, Integer> arg) {
            try {
                return arg.get1().startsWith(VisorQueryUtils.SCAN_QRY_NAME) ? nextScanPage(arg) : nextSqlPage(arg);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }

        /** Collect data from SQL query */
        private VisorQueryResult nextSqlPage(IgniteBiTuple<String, Integer> arg) throws IgniteCheckedException {
            long start = U.currentTimeMillis();

            ClusterNodeLocalMap<String, VisorQueryTask.VisorFutureResultSetHolder<List<?>>> storage = g.nodeLocalMap();

            VisorQueryTask.VisorFutureResultSetHolder<List<?>> t = storage.get(arg.get1());

            if (t == null)
                throw new GridInternalException("SQL query results are expired.");

            IgniteBiTuple<List<Object[]>, List<?>> nextRows = VisorQueryUtils.fetchSqlQueryRows(t.future(), t.next(), arg.get2());

            boolean hasMore = nextRows.get2() != null;

            if (hasMore)
                storage.put(arg.get1(), new VisorQueryTask.VisorFutureResultSetHolder<>(t.future(), nextRows.get2(), true));
            else
                storage.remove(arg.get1());

            return new VisorQueryResult(nextRows.get1(), hasMore, U.currentTimeMillis() - start);
        }

        /** Collect data from SCAN query */
        private VisorQueryResult nextScanPage(IgniteBiTuple<String, Integer> arg) throws IgniteCheckedException {
            long start = U.currentTimeMillis();

            ClusterNodeLocalMap<String, VisorQueryTask.VisorFutureResultSetHolder<Map.Entry<Object, Object>>> storage = g.nodeLocalMap();

            VisorQueryTask.VisorFutureResultSetHolder<Map.Entry<Object, Object>> t = storage.get(arg.get1());

            if (t == null)
                throw new GridInternalException("Scan query results are expired.");

            IgniteBiTuple<List<Object[]>, Map.Entry<Object, Object>> rows =
                VisorQueryUtils.fetchScanQueryRows(t.future(), t.next(), arg.get2());

            Boolean hasMore = rows.get2() != null;

            if (hasMore)
                storage.put(arg.get1(), new VisorQueryTask.VisorFutureResultSetHolder<>(t.future(), rows.get2(), true));
            else
                storage.remove(arg.get1());

            return new VisorQueryResult(rows.get1(), hasMore, U.currentTimeMillis() - start);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorQueryNextPageJob.class, this);
        }
    }
}
