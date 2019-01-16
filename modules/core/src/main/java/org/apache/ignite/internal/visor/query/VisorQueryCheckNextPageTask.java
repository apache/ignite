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

import java.util.List;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorEither;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.internal.visor.util.VisorExceptionWrapper;

import static org.apache.ignite.internal.visor.query.VisorQueryUtils.getQueryHolder;
import static org.apache.ignite.internal.visor.query.VisorQueryUtils.removeQueryHolder;

/**
 * Task for check a query execution and receiving first page of query result.
 */
@GridInternal
public class VisorQueryCheckNextPageTask extends VisorOneNodeTask<VisorQueryNextPageTaskArg, VisorEither<VisorQueryResult>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorQueryCheckNextPageJob job(VisorQueryNextPageTaskArg arg) {
        return new VisorQueryCheckNextPageJob(arg, debug);
    }

    /**
     * Job for collecting next page previously executed SQL or SCAN query.
     */
    private static class VisorQueryCheckNextPageJob extends VisorJob<VisorQueryNextPageTaskArg, VisorEither<VisorQueryResult>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Debug flag.
         */
        private VisorQueryCheckNextPageJob(VisorQueryNextPageTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorEither<VisorQueryResult> run(VisorQueryNextPageTaskArg arg) {
            String qryId = arg.getQueryId();

            VisorQueryHolder holder = getQueryHolder(ignite, qryId);

            if (holder.getErr() != null)
                return new VisorEither<>(new VisorExceptionWrapper(holder.getErr()));

            VisorQueryCursor<?> cur = holder.getCursor();
            List<Object[]> rows = holder.getRows();
            List<VisorQueryField> cols = null;

            boolean hasMore = cur == null || rows == null;

            if (rows != null) {
                cols = holder.getColumns();
                hasMore = cur.hasNext();
            }

            if (hasMore)
                holder.accessed(true);
            else
                removeQueryHolder(ignite, qryId);

            return new VisorEither<>(
                new VisorQueryResult(ignite.localNode().id(), qryId, cols, rows, hasMore, holder.duration()));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorQueryCheckNextPageJob.class, this);
        }
    }
}
