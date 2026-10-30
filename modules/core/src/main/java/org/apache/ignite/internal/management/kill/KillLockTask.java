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

package org.apache.ignite.internal.management.kill;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.processors.cache.GridCacheExplicitLockSpan;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheMvccManager;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Task to kill explicit cache locks.
 */
@GridInternal
public class KillLockTask extends VisorMultiNodeTask<KillLockCommandArg, UUID, UUID> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<KillLockCommandArg, UUID> job(KillLockCommandArg arg) {
        return new KillLockJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected UUID reduce0(List<ComputeJobResult> results) throws IgniteException {
        IgniteException err = null;

        for (ComputeJobResult res : results) {
            // The lock is held on one node only, failures on other nodes don't matter if it is killed.
            if (res.getException() == null) {
                if (res.getData() != null)
                    return res.getData();
            }
            else if (err == null)
                err = new IgniteException("Failed to kill lock on node: " + res.getNode().id(), res.getException());
        }

        if (err != null)
            throw err;

        return null;
    }

    /**
     * Job to kill explicit lock on node. Returns ID of the node if the lock is found and killed.
     */
    private static class KillLockJob extends VisorJob<KillLockCommandArg, UUID> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected KillLockJob(KillLockCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Nullable @Override protected UUID run(KillLockCommandArg arg) throws IgniteException {
            GridCacheMvccManager mvcc = ignite.context().cache().context().mvcc();

            for (GridCacheExplicitLockSpan span : mvcc.activeExplicitLocks()) {
                for (GridCacheMvccCandidate cand : span.candidates()) {
                    if (arg.xid().equals(BinaryUtils.asIgniteUuid(cand.version()))) {
                        // Xid identifies the lock of one thread, all keys of the lock are released at once.
                        return mvcc.releaseExplicitLock(cand.threadId(), cand.version()) ?
                            ignite.localNode().id() : null;
                    }
                }
            }

            return null;
        }
    }
}
