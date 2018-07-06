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

import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.jetbrains.annotations.Nullable;

/**
 * Simple MVCC tracker used only as an Mvcc snapshot holder.
 */
public class MvccQueryTracker {
    /** */
    protected volatile MvccSnapshot mvccSnapshot;

    /**
     *
     */
    MvccQueryTracker() {
    }

    /**
     * @param mvccSnapshot Mvcc snapshot.
     */
    public MvccQueryTracker(MvccSnapshot mvccSnapshot) {
        assert mvccSnapshot != null;

        this.mvccSnapshot = mvccSnapshot;
    }

    /**
     * @return Requested MVCC snapshot.
     */
    public MvccSnapshot snapshot() {
        assert mvccSnapshot != null : this;

        return mvccSnapshot;
    }

    /**
     * Marks tracker done.
     *
     * @return Acknowledge future.
     */
    @Nullable public IgniteInternalFuture<Void> onDone() {
        // No-op.
        return null;
    }

    /**
     * Marks tracker done.
     *
     * @return Acknowledge future.
     */
    @Nullable public IgniteInternalFuture<Void> onDone(GridNearTxLocal tx, boolean commit) {
        throw new UnsupportedOperationException("Operation not supported.");
    }
}
