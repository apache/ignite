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

package org.apache.ignite.internal.visor.tx;

import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.management.kill.KillTransactionCommandArg;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;
import static org.apache.ignite.internal.visor.tx.VisorTxOperation.KILL;

/** */
@GridInternal
public class KillTransactionTask
    extends VisorMultiNodeTask<KillTransactionCommandArg, Map<ClusterNode, VisorTxTaskResult>, VisorTxTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<KillTransactionCommandArg, VisorTxTaskResult> job(KillTransactionCommandArg arg) {
        return new KillTransactionJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected @Nullable Map<ClusterNode, VisorTxTaskResult> reduce0(
        List<ComputeJobResult> results
    ) throws IgniteException {
        return VisorTxTask.reduce0(
            results,
            new VisorTxTaskArg(KILL, null, null, null, null, null, null, taskArg.getXid(), null, null, null)
        );
    }

    /** */
    private static class KillTransactionJob extends VisorJob<KillTransactionCommandArg, VisorTxTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        public KillTransactionJob(KillTransactionCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorTxTaskResult run(KillTransactionCommandArg arg) throws IgniteException {
            return VisorTxTask.VisorTxJob.run(
                ignite,
                new VisorTxTaskArg(KILL, null, null, null, null, null, null, arg.getXid(), null, null, null)
            );
        }
    }
}
