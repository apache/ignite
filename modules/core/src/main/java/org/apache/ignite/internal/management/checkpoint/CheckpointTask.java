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
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/** Checkpoint task. */
public class CheckpointTask extends VisorMultiNodeTask<CheckpointCommandArg, String, String> {
    /** */
    private static final long serialVersionUID = 0;

    /** {@inheritDoc} */
    @Override protected VisorJob<CheckpointCommandArg, String> job(CheckpointCommandArg arg) {
        return new CheckpointJob(arg, false);
    }

    /** {@inheritDoc} */
    @Override protected @Nullable String reduce0(List<ComputeJobResult> results) throws IgniteException {
        StringBuilder result = new StringBuilder();

        for (ComputeJobResult res : results) {
            if (res.getException() != null)
                throw res.getException();

            result.append(res.getData().toString()).append('\n');
        }

        return result.toString();
    }

    /** Checkpoint job. */
    private static class CheckpointJob extends VisorJob<CheckpointCommandArg, String> {
        /** */
        private static final long serialVersionUID = 0;

        /** */
        protected CheckpointJob(CheckpointCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(CheckpointCommandArg arg) throws IgniteException {
            if (!CU.isPersistenceEnabled(ignite.configuration()))
                  return result("persistence disabled, checkpoint skipped");

            try {
                GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)ignite.context().cache().context().database();

                CheckpointProgress checkpointfut = dbMgr.forceCheckpoint(arg.reason());

                if (arg.waitForFinish()) {
                    long timeout = arg.timeout();

                    if (timeout > 0) {
                        try {
                            checkpointfut.futureFor(CheckpointState.FINISHED).get(timeout, TimeUnit.MILLISECONDS);
                        }
                        catch (IgniteFutureTimeoutCheckedException e) {
                            return result("Checkpoint started but not finished within timeout " + timeout + " ms");
                        }
                    }
                    else
                        checkpointfut.futureFor(CheckpointState.FINISHED).get();

                    return result("Checkpoint finished");
                }

                return result("Checkpoint started");
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(result("Failed to force checkpoint on node"), e);
            }
        }

        /**
         * Create result string with node id and given description
         *
         * @param desc info about node to be put in result.
         */
        private String result(String desc) {
            return ignite.localNode().id() + ": " + desc;
        }
    }
}
