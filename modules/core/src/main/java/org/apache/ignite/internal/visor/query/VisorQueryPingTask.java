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

import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorEither;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.internal.visor.util.VisorExceptionWrapper;

import static org.apache.ignite.internal.visor.query.VisorQueryUtils.getQueryHolder;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.log;

/**
 * Task for inform a node about awaiting of query result from Web console.
 */
@GridInternal
public class VisorQueryPingTask extends VisorOneNodeTask<VisorQueryNextPageTaskArg, VisorEither<VisorQueryPingTaskResult>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorQueryFetchFirstPageJob job(VisorQueryNextPageTaskArg arg) {
        return new VisorQueryFetchFirstPageJob(arg, debug);
    }

    /**
     * Job for inform a node about awaiting of query result from Web console.
     */
    private static class VisorQueryFetchFirstPageJob extends VisorJob<VisorQueryNextPageTaskArg, VisorEither<VisorQueryPingTaskResult>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Debug flag.
         */
        private VisorQueryFetchFirstPageJob(VisorQueryNextPageTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorEither<VisorQueryPingTaskResult> run(VisorQueryNextPageTaskArg arg) {
            String qryId = arg.getQueryId();

            long start = U.currentTimeMillis();

            if (debug)
                start = log(ignite.log(), "Ping of query started: " + qryId, getClass(), start);

            VisorQueryHolder holder = getQueryHolder(ignite, qryId);

            if (holder.getErr() != null)
                return new VisorEither<>(new VisorExceptionWrapper(holder.getErr()));

            holder.setAccessed(true);

            if (debug)
                log(ignite.log(), "Ping of query finished: " + qryId, getClass(), start);

            return new VisorEither<>(new VisorQueryPingTaskResult());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorQueryFetchFirstPageJob.class, this);
        }
    }
}
