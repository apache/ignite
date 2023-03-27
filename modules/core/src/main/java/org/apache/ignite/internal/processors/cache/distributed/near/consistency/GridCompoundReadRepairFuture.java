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

package org.apache.ignite.internal.processors.cache.distributed.near.consistency;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Compound future that represents the result of the external fixes for some keys.
 */
public class GridCompoundReadRepairFuture extends GridFutureAdapter<Void> implements IgniteInClosure<IgniteInternalFuture<Void>> {
    /** Listener calls updater. */
    private static final AtomicIntegerFieldUpdater<GridCompoundReadRepairFuture> LSNR_CALLS_UPD =
        AtomicIntegerFieldUpdater.newUpdater(GridCompoundReadRepairFuture.class, "lsnrCalls");

    /** Initialized. */
    private volatile boolean inited;

    /** Listener calls. */
    private volatile int lsnrCalls;

    /** Count of compounds in the future. */
    private volatile int size;

    /** Irreparable Keys. */
    private volatile Collection<Object> irreparableKeys;

    /**
     * @param fut Future.
     */
    public void add(IgniteInternalFuture<Void> fut) {
        size++; // All additions are from the same thread.

        fut.listen(this);
    }

    /** {@inheritDoc} */
    @Override public void apply(IgniteInternalFuture<Void> fut) {
        Throwable e = fut.error();

        if (e != null) {
            if (e instanceof IgniteIrreparableConsistencyViolationException) {
                Collection<Object> repairableKey = ((IgniteIrreparableConsistencyViolationException)e).repairableKeys();
                Collection<Object> irreparableKeys = ((IgniteIrreparableConsistencyViolationException)e).irreparableKeys();

                assert repairableKey == null || repairableKey.isEmpty() : repairableKey.size();
                assert irreparableKeys.size() == 1 : irreparableKeys.size(); // Single key fix.

                synchronized (this) {
                    if (this.irreparableKeys == null)
                        this.irreparableKeys = ConcurrentHashMap.newKeySet();
                }

                this.irreparableKeys.addAll(irreparableKeys);
            }
            else
                onDone(e);
        }

        LSNR_CALLS_UPD.incrementAndGet(this);

        checkComplete();
    }

    /**
     * Mark this future as initialized.
     */
    public final void markInitialized() {
        inited = true;

        checkComplete();
    }

    /**
     * Check completeness of the future.
     */
    private void checkComplete() {
        assert lsnrCalls <= size;

        if (inited && !isDone() && lsnrCalls == size) {
            if (irreparableKeys == null)
                onDone();
            else
                onDone(new IgniteIrreparableConsistencyViolationException(null, irreparableKeys));
        }
    }
}
