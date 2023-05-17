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
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.cluster.NodeOrderComparator;
import org.apache.ignite.internal.management.diagnostic.DiagnosticPagelocksCommandArg;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.dumpprocessors.ToStringDumpHelper;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/** */
@GridInternal
public class VisorPageLocksTask
    extends VisorMultiNodeTask<DiagnosticPagelocksCommandArg, Map<ClusterNode, VisorPageLocksResult>, VisorPageLocksResult> {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<DiagnosticPagelocksCommandArg, VisorPageLocksResult> job(DiagnosticPagelocksCommandArg arg) {
        return new VisorPageLocksTrackerJob(arg, debug);
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
    private static class VisorPageLocksTrackerJob extends VisorJob<DiagnosticPagelocksCommandArg, VisorPageLocksResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Formal job argument.
         * @param debug Debug flag.
         */
        private VisorPageLocksTrackerJob(DiagnosticPagelocksCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorPageLocksResult run(DiagnosticPagelocksCommandArg arg) {
            PageLockTrackerManager lockTrackerMgr = ignite.context().cache().context().diagnostic().pageLockTracker();

            String result;

            switch (arg.operation()) {
                case DUMP_LOG:
                    lockTrackerMgr.dumpLocksToLog();

                    result = "Page locks dump was printed to console " +
                        ToStringDumpHelper.DATE_FMT.format(Instant.now());

                    break;
                case DUMP:
                    String filePath = arg.path() != null ?
                        lockTrackerMgr.dumpLocksToFile(arg.path()) :
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
