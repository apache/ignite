/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.visor.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Task to collect currently running queries.
 */
@GridInternal
public class VisorRunningQueriesCollectorTask extends VisorMultiNodeTask<VisorRunningQueriesCollectorTaskArg, Map<UUID, Collection<VisorRunningQuery>>, Collection<VisorRunningQuery>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCollectRunningQueriesJob job(VisorRunningQueriesCollectorTaskArg arg) {
        return new VisorCollectRunningQueriesJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Map<UUID, Collection<VisorRunningQuery>> reduce0(List<ComputeJobResult> results) throws IgniteException {
        Map<UUID, Collection<VisorRunningQuery>> map = new HashMap<>();

        for (ComputeJobResult res : results)
            if (res.getException() == null) {
                Collection<VisorRunningQuery> queries = res.getData();

                map.put(res.getNode().id(), queries);
            }

        return map;
    }

    /**
     * Job to collect currently running queries from node.
     */
    private static class VisorCollectRunningQueriesJob
        extends VisorJob<VisorRunningQueriesCollectorTaskArg, Collection<VisorRunningQuery>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorCollectRunningQueriesJob(@Nullable VisorRunningQueriesCollectorTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Collection<VisorRunningQuery> run(@Nullable VisorRunningQueriesCollectorTaskArg arg)
            throws IgniteException {
            assert arg != null;

            Collection<GridRunningQueryInfo> queries = ignite.context().query()
                .runningQueries(arg.getDuration());

            Collection<VisorRunningQuery> res = new ArrayList<>(queries.size());

            long curTime = U.currentTimeMillis();

            for (GridRunningQueryInfo qry : queries)
                res.add(new VisorRunningQuery(qry.id(), qry.query(), qry.queryType(), qry.schemaName(),
                    qry.startTime(), curTime - qry.startTime(),
                    qry.cancelable(), qry.local()));

            return res;
        }
    }
}
