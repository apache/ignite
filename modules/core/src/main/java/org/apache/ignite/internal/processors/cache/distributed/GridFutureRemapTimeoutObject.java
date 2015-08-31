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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 * Future remap timeout object.
 */
public class GridFutureRemapTimeoutObject extends GridTimeoutObjectAdapter {
    /** */
    private final GridFutureAdapter<?> fut;

    /** Finished flag. */
    private final AtomicBoolean finished = new AtomicBoolean();

    /** Topology version to wait. */
    private final AffinityTopologyVersion topVer;

    /** Exception cause. */
    private final IgniteCheckedException e;

    /**
     * @param fut Future.
     * @param timeout Timeout.
     * @param topVer Topology version timeout was created on.
     * @param e Exception cause.
     */
    public GridFutureRemapTimeoutObject(
        GridFutureAdapter<?> fut,
        long timeout,
        AffinityTopologyVersion topVer,
        IgniteCheckedException e) {
        super(timeout);

        this.fut = fut;
        this.topVer = topVer;
        this.e = e;
    }

    /** {@inheritDoc} */
    @Override public void onTimeout() {
        if (finish()) // Fail the whole get future, else remap happened concurrently.
            fut.onDone(new IgniteCheckedException("Failed to wait for topology version to change: " + topVer, e));
    }

    /**
     * @return Guard against concurrent completion.
     */
    public boolean finish() {
        return finished.compareAndSet(false, true);
    }
}