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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Task to periodically clean partition deffered delete queue.
 *
 * @see GridDhtLocalPartition#cleanupRemoveQueue().
 */
public class PartitionDefferedDeleteQueueCleanupTask implements GridTimeoutObject {
    /** */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** */
    private final long endTime;

    /** */
    private final long timeout;

    /** */
    private final GridCacheSharedContext cctx;

    /** */
    private final IgniteLogger log;

    /**
     * @param timeout Timeout.
     */
    public PartitionDefferedDeleteQueueCleanupTask(GridCacheSharedContext cctx, long timeout) {
        this.timeout = timeout;
        endTime = U.currentTimeMillis() + timeout;
        this.cctx = cctx;
        log = cctx.logger(getClass());
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid timeoutId() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public long endTime() {
        return endTime;
    }

    /** {@inheritDoc} */
    @Override public void onTimeout() {
        cctx.kernalContext().closure().runLocalSafe(new GridPlainRunnable() {
            @Override
            public void run() {
                try {
                    for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                        if (!grp.isLocal() && grp.affinityNode()) {
                            GridDhtPartitionTopology top = null;

                            try {
                                top = grp.topology();
                            } catch (IllegalStateException ignore) {
                                // Cache stopped.
                            }

                            if (top != null) {
                                for (GridDhtLocalPartition part : top.currentLocalPartitions())
                                    part.cleanupRemoveQueue();
                            }

                            if (cctx.kernalContext().isStopping())
                                return;
                        }
                    }
                }
                catch (Exception e) {
                    U.error(log, "Failed to cleanup removed cache items: " + e, e);
                }

                if (cctx.kernalContext().isStopping())
                    return;

                // Re-schedule task after finish.
                cctx.time().addTimeoutObject(new PartitionDefferedDeleteQueueCleanupTask(cctx, timeout));
            }
        }, true);
    }
}
