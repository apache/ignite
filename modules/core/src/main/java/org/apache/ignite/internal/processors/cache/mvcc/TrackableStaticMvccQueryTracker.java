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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Static Mvcc tracker which is tracked during coordinator change.
 */
@SuppressWarnings({"unchecked", "NonPrivateFieldAccessedInSynchronizedContext"})
public class TrackableStaticMvccQueryTracker extends StaticMvccQueryTracker{
    /** */
    public static final long MVCC_TRACKER_ID_NA = -1;

    /** */
    private static final AtomicLong idCntr = new AtomicLong();

    /** */
    protected long mvccCrdVer;

    /** */
    private final long id;

    /**
     * Constructor for a pessimistic tx query trackers.
     *
     * @param cctx Cache context.
     * @param mvccSnapshot Mvcc snapshot.
     */
    public TrackableStaticMvccQueryTracker(GridCacheContext cctx, MvccSnapshot mvccSnapshot) {
        super(mvccSnapshot, cctx);
        assert cctx.mvccEnabled() : cctx.name();
        assert cctx != null;

        this.id = idCntr.getAndIncrement();

        if (mvccSnapshot != null) {
            this.mvccCrdVer = mvccSnapshot.coordinatorVersion();

            cctx.shared().coordinators().addQueryTracker(this);
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized long onMvccCoordinatorChange(MvccCoordinator newCrd) {
        if (mvccSnapshot != null) {
            assert mvccCrdVer != 0 : this;

            if (mvccCrdVer != newCrd.coordinatorVersion()) {
                mvccCrdVer = newCrd.coordinatorVersion(); // Need notify new coordinator.

                return id;
            }
            else
                return MVCC_TRACKER_ID_NA;
        }
        else if (mvccCrdVer != 0)
            mvccCrdVer = 0; // Mark for remap.

        return MVCC_TRACKER_ID_NA;
    }

    /** {@inheritDoc} */
    @Override public long id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override @Nullable public IgniteInternalFuture<Void> onDone() {
        cctx.shared().coordinators().removeQueryTracker(id);

        MvccProcessor mvccProcessor = cctx.shared().coordinators();

        if (mvccSnapshot != null &&
            mvccProcessor.currentCoordinator().coordinatorVersion() != mvccSnapshot.coordinatorVersion())
            mvccProcessor.ackQueryDoneNewCoordinator(id); // Notify new coordinator.

        return null;
    }

    /** {@inheritDoc} */
    @Override public void onDone(IgniteCheckedException e) {
        onDone();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TrackableStaticMvccQueryTracker.class, this);
    }
}
