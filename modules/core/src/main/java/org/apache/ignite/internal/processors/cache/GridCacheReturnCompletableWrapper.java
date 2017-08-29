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

package org.apache.ignite.internal.processors.cache;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.Nullable;

/**
 * Provides initialized GridCacheReturn.
 */
public class GridCacheReturnCompletableWrapper {
    /** Completable wrapper upd. */
    private static final AtomicReferenceFieldUpdater<GridCacheReturnCompletableWrapper, Object> COMPLETABLE_WRAPPER_UPD =
        AtomicReferenceFieldUpdater.newUpdater(GridCacheReturnCompletableWrapper.class, Object.class, "o");

    /** */
    private volatile Object o;

    /** Node id. */
    private final UUID nodeId;

    /**
     * @param nodeId Node id.
     */
    public GridCacheReturnCompletableWrapper(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return ID of node initiated tx or {@code null} if this node is local.
     */
    @Nullable public UUID nodeId() {
        return nodeId;
    }

    /**
     * Marks as initialized.
     *
     * @param ret Return.
     */
    public void initialize(GridCacheReturn ret) {
        final Object obj = this.o;

        if (obj == null) {
            boolean res = COMPLETABLE_WRAPPER_UPD.compareAndSet(this, null, ret);

            if (!res)
                initialize(ret);
        }
        else if (obj instanceof GridFutureAdapter) {
            ((GridFutureAdapter)obj).onDone(ret);

            boolean res = COMPLETABLE_WRAPPER_UPD.compareAndSet(this, obj, ret);

            assert res;
        }
        else
            throw new IllegalStateException("GridCacheReturnCompletableWrapper can't be reinitialized");
    }

    /**
     * Allows wait for properly initialized value.
     */
    public IgniteInternalFuture<GridCacheReturn> fut() {
        final Object obj = this.o;

        if (obj instanceof GridCacheReturn)
            return new GridFinishedFuture<>((GridCacheReturn)obj);
        else if (obj instanceof IgniteInternalFuture)
            return (IgniteInternalFuture)obj;
        else if (obj == null) {
            boolean res = COMPLETABLE_WRAPPER_UPD.compareAndSet(this, null, new GridFutureAdapter<>());

            if (res)
                return (IgniteInternalFuture)this.o;
            else
                return fut();
        }
        else
            throw new IllegalStateException();
    }
}
