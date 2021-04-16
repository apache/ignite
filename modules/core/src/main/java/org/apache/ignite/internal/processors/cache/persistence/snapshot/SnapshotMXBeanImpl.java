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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.mxbean.SnapshotMXBean;

/**
 * Snapshot MBean features.
 */
public class SnapshotMXBeanImpl implements SnapshotMXBean {
    /** Instance of snapshot cache shared manager. */
    private final IgniteSnapshotManager mgr;

    /**
     * @param ctx Kernal context.
     */
    public SnapshotMXBeanImpl(GridKernalContext ctx) {
        mgr = ctx.cache().context().snapshotMgr();
    }

    /** {@inheritDoc} */
    @Override public void createSnapshot(String snpName) {
        IgniteFuture<Void> fut = mgr.createSnapshot(snpName);

        if (fut.isDone())
            fut.get();
    }

    /** {@inheritDoc} */
    @Override public void cancelSnapshot(String snpName) {
        mgr.cancelSnapshot(snpName).get();
    }
}
