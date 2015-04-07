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
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.visor.*;
import org.apache.ignite.lang.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Task for collecting next page previously executed SQL or SCAN query.
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

        /**
         * Collect data from SQL query.
         *
         * @param arg
         * @return
         * @throws IgniteCheckedException
         */
        private VisorQueryResult nextSqlPage(IgniteBiTuple<String, Integer> arg) throws IgniteCheckedException {
            long start = U.currentTimeMillis();

            ConcurrentMap<String, VisorQueryTask.VisorQueryCursorHolder> storage =
                ignite.cluster().nodeLocalMap();

            VisorQueryTask.VisorQueryCursorHolder t = storage.get(arg.get1());

            if (t == null)
                throw new GridInternalException("SQL query results are expired.");

            VisorQueryCursor cur = t.cursor();

            List<Object[]> nextRows = VisorQueryUtils.fetchSqlQueryRows(cur, arg.get2());

            boolean hasMore = cur.hasNext();

            if (hasMore)
                storage.put(arg.get1(), new VisorQueryTask.VisorQueryCursorHolder(t.cursor(), true));
            else
                storage.remove(arg.get1());

            return new VisorQueryResult(nextRows, hasMore, U.currentTimeMillis() - start);
        }

        /**
         * Collect data from SCAN query
         *
         * @param arg
         * @return
         * @throws IgniteCheckedException
         */
        private VisorQueryResult nextScanPage(IgniteBiTuple<String, Integer> arg) throws IgniteCheckedException {
            long start = U.currentTimeMillis();

            ConcurrentMap<String, VisorQueryTask.VisorQueryCursorHolder> storage =
                ignite.cluster().nodeLocalMap();

            VisorQueryTask.VisorQueryCursorHolder t = storage.get(arg.get1());

            if (t == null)
                throw new GridInternalException("Scan query results are expired.");

            VisorQueryCursor cur = t.cursor();

            List<Object[]> rows = VisorQueryUtils.fetchScanQueryRows(cur, arg.get2());

            Boolean hasMore = cur.hasNext();

            if (hasMore)
                storage.put(arg.get1(), new VisorQueryTask.VisorQueryCursorHolder(cur, true));
            else
                storage.remove(arg.get1());

            return new VisorQueryResult(rows, hasMore, U.currentTimeMillis() - start);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorQueryNextPageJob.class, this);
        }
    }
}
