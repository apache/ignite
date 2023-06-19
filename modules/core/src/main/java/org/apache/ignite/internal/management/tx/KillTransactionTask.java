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

package org.apache.ignite.internal.management.tx;

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

/** */
@GridInternal
public class KillTransactionTask
    extends VisorMultiNodeTask<KillTransactionCommandArg, Map<ClusterNode, TxTaskResult>, TxTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<KillTransactionCommandArg, TxTaskResult> job(KillTransactionCommandArg arg) {
        return new KillTransactionJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected @Nullable Map<ClusterNode, TxTaskResult> reduce0(
        List<ComputeJobResult> results
    ) throws IgniteException {
        return TxTask.reduce0(results, false);
    }

    /** */
    private static class KillTransactionJob extends VisorJob<KillTransactionCommandArg, TxTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        public KillTransactionJob(KillTransactionCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected TxTaskResult run(KillTransactionCommandArg arg) throws IgniteException {
            TxCommandArg arg0 = new TxCommandArg();

            arg0.kill(true);
            arg0.xid(arg.xid());

            return TxTask.VisorTxJob.run(ignite, arg0, null);
        }
    }
}
