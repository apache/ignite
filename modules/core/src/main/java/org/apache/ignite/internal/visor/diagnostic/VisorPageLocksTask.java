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

package org.apache.ignite.internal.visor.diagnostic;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.cluster.NodeOrderComparator;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.dumpprocessors.ToStringDumpProcessor;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.jetbrains.annotations.Nullable;

@GridInternal
public class VisorPageLocksTask
    extends VisorMultiNodeTask<VisorPageLocksTrackerArgs, Map<ClusterNode, VisorPageLocksResult>, VisorPageLocksResult> {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorPageLocksTrackerArgs, VisorPageLocksResult> job(VisorPageLocksTrackerArgs arg) {
        return new VisorPageLocksTrackerJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<VisorPageLocksTrackerArgs> arg) {
        Set<UUID> nodeIds = new HashSet<>();

        Set<String> nodeIds0 = arg.getArgument().nodeIds();

        for (ClusterNode node : ignite.cluster().nodes()) {
            if (nodeIds0.contains(String.valueOf(node.consistentId())) || nodeIds0.contains(node.id().toString()))
                nodeIds.add(node.id());
        }

        if (F.isEmpty(nodeIds))
            nodeIds.add(ignite.localNode().id());

        return nodeIds;
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Map<ClusterNode, VisorPageLocksResult> reduce0(
        List<ComputeJobResult> results
    ) throws IgniteException {
        Map<ClusterNode, VisorPageLocksResult> mapRes = new TreeMap<>(NodeOrderComparator.getInstance());

        results.forEach(j -> {
            if (j.getException() == null)
                mapRes.put(j.getNode(), j.getData());
            else if (j.getException() != null) {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);

                j.getException().printStackTrace(pw);

                mapRes.put(j.getNode(), new VisorPageLocksResult(sw.toString()));
            }
        });

        return mapRes;
    }

    /**
     *
     */
    private static class VisorPageLocksTrackerJob extends VisorJob<VisorPageLocksTrackerArgs, VisorPageLocksResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Formal job argument.
         * @param debug Debug flag.
         */
        private VisorPageLocksTrackerJob(VisorPageLocksTrackerArgs arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorPageLocksResult run(VisorPageLocksTrackerArgs arg) {
            PageLockTrackerManager lockTrackerMgr = ignite.context().cache().context().diagnostic().pageLockTracker();

            String result;

            switch (arg.operation()) {
                case DUMP_LOG:
                    lockTrackerMgr.dumpLocksToLog();

                    result = "Page locks dump was printed to console " +
                        ToStringDumpProcessor.DATE_FMT.format(new Date(System.currentTimeMillis()));

                    break;
                case DUMP_FILE:
                    String filePath = arg.filePath() != null ?
                        lockTrackerMgr.dumpLocksToFile(arg.filePath()) :
                        lockTrackerMgr.dumpLocksToFile();

                    result = "Page locks dump was writtern to file " + filePath;

                    break;
                default:
                    result = "Unsupported operation: " + arg.operation();
            }

            return new VisorPageLocksResult(result);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorPageLocksTrackerJob.class, this);
        }
    }
}
