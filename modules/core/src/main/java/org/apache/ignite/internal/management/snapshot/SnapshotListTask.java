/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.management.snapshot;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;

import static org.apache.ignite.internal.util.lang.ClusterNodeFunc.nodeIds;

/**
 * Task to collect the names of all the snapshots existing in the cluster.
 */
@GridInternal
public class SnapshotListTask extends VisorMultiNodeTask<SnapshotListCommandArg, Collection<String>, List<String>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<SnapshotListCommandArg, List<String>> job(SnapshotListCommandArg arg) {
        return new SnapshotListJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<SnapshotListCommandArg> arg) {
        return nodeIds(ignite.cluster().forServers().nodes());
    }

    /** {@inheritDoc} */
    @Override protected Collection<String> reduce0(List<ComputeJobResult> results) {
        Set<String> res = new TreeSet<>();

        for (ComputeJobResult jobRes : results) {
            if (jobRes.getException() != null)
                throw jobRes.getException();

            if (jobRes.getData() != null)
                res.addAll(jobRes.getData());
        }

        return res;
    }

    /** */
    private static class SnapshotListJob extends SnapshotJob<SnapshotListCommandArg, List<String>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Snapshot list task argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected SnapshotListJob(SnapshotListCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected List<String> run(SnapshotListCommandArg arg) {
            if (!CU.isPersistenceEnabled(ignite.context().config()) || ignite.context().clientNode())
                return java.util.Collections.emptyList();

            IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();

            return snpMgr.localSnapshotNames(arg.dest());
        }
    }
}
