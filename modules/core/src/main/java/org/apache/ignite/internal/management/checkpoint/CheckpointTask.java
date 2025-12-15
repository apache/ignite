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

package org.apache.ignite.internal.management.checkpoint;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/** Checkpoint task. */
public class CheckpointTask extends VisorMultiNodeTask<CheckpointCommandArg, String, Void> {
    /** */
    private static final long serialVersionUID = 0;

    /** {@inheritDoc} */
    @Override protected VisorJob<CheckpointCommandArg, Void> job(CheckpointCommandArg arg) {
        return new CheckpointJob(arg, false);
    }

    /** {@inheritDoc} */
    @Override protected @Nullable String reduce0(List<ComputeJobResult> results) throws IgniteException {
        for (ComputeJobResult res : results) {
            if (res.getException() != null)
                throw res.getException();
        }

        return "Checkpoint triggered on all nodes";
    }

    /** Checkpoint job. */
    private static class CheckpointJob extends VisorJob<CheckpointCommandArg, Void> {
        /** */
        private static final long serialVersionUID = 0;

        /** */
        protected CheckpointJob(CheckpointCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(CheckpointCommandArg arg) throws IgniteException {
            if (!CU.isPersistenceEnabled(ignite.configuration()))
                throw new IgniteException("Can't checkpoint on in-memory node");

            try {
                GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)ignite.context().cache().context().database();

                CheckpointProgress checkpointfut = dbMgr.forceCheckpoint(arg.reason());

                if (arg.waitForFinish()) {
                    long timeout = arg.timeout();

                    if (timeout > 0)
                        checkpointfut.futureFor(CheckpointState.FINISHED).get(timeout, TimeUnit.MILLISECONDS);
                    else
                        checkpointfut.futureFor(CheckpointState.FINISHED).get();
                }
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to force checkpoint on node: " + ignite.localNode().id(), e);
            }

            return null;
        }
    }
}
