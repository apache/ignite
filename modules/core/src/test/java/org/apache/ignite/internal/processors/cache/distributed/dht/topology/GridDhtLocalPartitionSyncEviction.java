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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Extends a DHT partition adding a support for blocking capabilities on clearing.
 */
public class GridDhtLocalPartitionSyncEviction extends GridDhtLocalPartition {
    /** */
    static final int TIMEOUT = 30_000;

    /** */
    private boolean delayed;

    /** */
    private int mode;

    /** */
    private CountDownLatch lock;

    /** */
    private CountDownLatch unlock;

    /**
     * @param ctx Context.
     * @param grp Group.
     * @param id Id.
     * @param recovery Recovery.
     * @param mode Delay mode: 0 - delay before rent, 1 - delay in the middle of clearing, 2 - delay after tryFinishEviction
     *             3 - delay before clearing.
     * @param lock Clearing lock latch.
     * @param unlock Clearing unlock latch.
     */
    public GridDhtLocalPartitionSyncEviction(
        GridCacheSharedContext ctx,
        CacheGroupContext grp,
        int id,
        boolean recovery,
        int mode,
        CountDownLatch lock,
        CountDownLatch unlock
    ) {
        super(ctx, grp, id, recovery);
        this.mode = mode;
        this.lock = lock;
        this.unlock = unlock;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> rent() {
        if (mode == 0)
            sync();

        return super.rent();
    }

    /** {@inheritDoc} */
    @Override protected long clearAll(EvictionContext evictionCtx) throws NodeStoppingException {
        EvictionContext spied = mode == 1 ? Mockito.spy(evictionCtx) : evictionCtx;

        if (mode == 3)
            sync();

        if (mode == 1) {
            Mockito.doAnswer(new Answer() {
                @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                    if (!delayed) {
                        sync();

                        delayed = true;
                    }

                    return invocation.callRealMethod();
                }
            }).when(spied).shouldStop();
        }

        long cnt = super.clearAll(spied);

        if (mode == 2)
            sync();

        return cnt;
    }

    /** */
    protected void sync() {
        lock.countDown();

        try {
            if (!U.await(unlock, TIMEOUT, TimeUnit.MILLISECONDS))
                throw new AssertionError("Failed to wait for lock release");
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new AssertionError(X.getFullStackTrace(e));
        }
    }
}
