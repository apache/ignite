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

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

@GridInternal
public class VisorPageLocksTrackerTask extends VisorOneNodeTask<VisorPageLocksTrackerArgs, VisorPageLocksTrackerResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorPageLocksTrackerArgs, VisorPageLocksTrackerResult> job(
        VisorPageLocksTrackerArgs arg) {
        return new VisorPageLocksTrackerJob(arg, debug);
    }

    /**
     *
     */
    private static class VisorPageLocksTrackerJob extends VisorJob<VisorPageLocksTrackerArgs, VisorPageLocksTrackerResult> {
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
        @Override protected VisorPageLocksTrackerResult run(VisorPageLocksTrackerArgs arg) {
            PageLockTrackerManager lockTrackerMgr = ignite.context().cache().context().diagnostic().pageLockTracker();

            String op = arg.operation();

            String result;

            switch (op) {
                case "status":
                case "enable":
                case "disable":
                    result = "Unsupported operation: " + op + ", " + nodeInfo(ignite.localNode());
                    break;
                case "dump":
                    String dumpLocks = lockTrackerMgr.dumpLocks();

                    result = "Dump:\n" + dumpLocks;

                    if (arg.type() != null) {
                        if ("log".equals(arg.type())) {
                            ignite.log().info(dumpLocks);

                            result = "Page locks dump was printed to console on nodeId=" +
                                ignite.localNode().id() + ", nodeConsistentId=" + ignite.localNode().consistentId();
                        }
                        else if ("file".equals(arg.type())) {
                            String filePath = arg.filePath() != null ?
                                lockTrackerMgr.dumpLocksToFile(arg.filePath()) :
                                lockTrackerMgr.dumpLocksToFile();

                            result = "Page locks dump was writtern to file " +
                                filePath + " on nodeId=" +
                                ignite.localNode().id() + ", nodeConsistentId=" + ignite.localNode().consistentId();
                        }
                    }

                    break;
                default:
                    result = "Unsupported operation: " + op + ", " + nodeInfo(ignite.localNode());
            }

            return new VisorPageLocksTrackerResult(result);
        }

        /**
         * @param node Cluster node.
         * @return Node info in string format.
         */
        private String nodeInfo(ClusterNode node) {
            return "nodeId=" + node.id() + ", nodeConsistentId=" + node.consistentId();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorPageLocksTrackerJob.class, this);
        }
    }
}
