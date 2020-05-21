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

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.mxbean.SnapshotMXBean;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Snapshot MBean features.
 */
public class SnapshotMXBeanImpl implements SnapshotMXBean {
    /** Instance of snapshot cache shared manager. */
    private final IgniteSnapshotManager mgr;

    /** Ignite instance. */
    private final IgniteEx ig;

    /**
     * @param ctx Kernal context.
     */
    public SnapshotMXBeanImpl(GridKernalContext ctx) {
        ig = ctx.grid();
        mgr = ctx.cache().context().snapshotMgr();
    }

    /** {@inheritDoc} */
    @Override public void createSnapshot(String snpName) {
        IgniteFuture<Void> fut = mgr.createSnapshot(snpName);

        if (fut.isDone())
            fut.get();
    }

    /** {@inheritDoc} */
    @Override public void cancel(String snpName) {
        A.notNullOrEmpty(snpName, "Snapshot name must be not empty or null");

        try {
            ig.compute(ig.cluster()).broadcast(new SnapshotCancelClosure(), snpName);
        }
        catch (IgniteException e) {
            throw new RuntimeException(e);
        }
    }

    /** Cancel snapshot operation closure. */
    @GridInternal
    private static class SnapshotCancelClosure implements IgniteClosure<String, Void> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /** {@inheritDoc} */
        @Override public Void apply(String snpName) {
            ignite.context().cache().context().snapshotMgr().cancelSnapshot(snpName);

            return null;
        }
    }
}
