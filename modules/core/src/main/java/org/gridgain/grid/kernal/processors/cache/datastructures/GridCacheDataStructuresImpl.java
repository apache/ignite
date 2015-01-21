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

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.apache.ignite.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.jetbrains.annotations.*;

/**
 * Data structures implementation object.
 */
public class GridCacheDataStructuresImpl<K, V> implements GridCacheDataStructures {
    /** Data structures manager. */
    private GridCacheDataStructuresManager<K, V> dsMgr;

    /**
     * @param cctx Cache context.
     */
    public GridCacheDataStructuresImpl(GridCacheContext<K, V> cctx) {
        dsMgr = cctx.dataStructures();
    }

    /** {@inheritDoc} */
    @Override public GridCacheAtomicSequence atomicSequence(String name, long initVal, boolean create)
        throws IgniteCheckedException {
        return dsMgr.sequence(name, initVal, create);
    }

    /** {@inheritDoc} */
    @Override public boolean removeAtomicSequence(String name) throws IgniteCheckedException {
        return dsMgr.removeSequence(name);
    }

    /** {@inheritDoc} */
    @Override public GridCacheAtomicLong atomicLong(String name, long initVal, boolean create) throws IgniteCheckedException {
        return dsMgr.atomicLong(name, initVal, create);
    }

    /** {@inheritDoc} */
    @Override public boolean removeAtomicLong(String name) throws IgniteCheckedException {
        return dsMgr.removeAtomicLong(name);
    }

    /** {@inheritDoc} */
    @Override public <T> GridCacheAtomicReference<T> atomicReference(String name, T initVal, boolean create)
        throws IgniteCheckedException {
        return dsMgr.atomicReference(name, initVal, create);
    }

    /** {@inheritDoc} */
    @Override public boolean removeAtomicReference(String name) throws IgniteCheckedException {
        return dsMgr.removeAtomicReference(name);
    }

    /** {@inheritDoc} */
    @Override public <T, S> GridCacheAtomicStamped<T, S> atomicStamped(String name, T initVal, S initStamp,
        boolean create) throws IgniteCheckedException {
        return dsMgr.atomicStamped(name, initVal, initStamp, create);
    }

    /** {@inheritDoc} */
    @Override public boolean removeAtomicStamped(String name) throws IgniteCheckedException {
        return dsMgr.removeAtomicStamped(name);
    }

    /** {@inheritDoc} */
    @Override public <T> GridCacheQueue<T> queue(String name, int cap, boolean collocated, boolean create)
        throws IgniteCheckedException {
        return dsMgr.queue(name, cap <= 0 ? Integer.MAX_VALUE : cap, collocated, create);
    }

    /** {@inheritDoc} */
    @Override public boolean removeQueue(String name) throws IgniteCheckedException {
        return dsMgr.removeQueue(name, 0);
    }

    /** {@inheritDoc} */
    @Override public boolean removeQueue(String name, int batchSize) throws IgniteCheckedException {
        return dsMgr.removeQueue(name, batchSize);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> GridCacheSet<T> set(String name, boolean collocated, boolean create)
        throws IgniteCheckedException {
        return dsMgr.set(name, collocated, create);
    }

    /** {@inheritDoc} */
    @Override public boolean removeSet(String name) throws IgniteCheckedException {
        return dsMgr.removeSet(name);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheCountDownLatch countDownLatch(String name, int cnt, boolean autoDel,
        boolean create) throws IgniteCheckedException {
        return dsMgr.countDownLatch(name, cnt, autoDel, create);
    }

    /** {@inheritDoc} */
    @Override public boolean removeCountDownLatch(String name) throws IgniteCheckedException {
        return dsMgr.removeCountDownLatch(name);
    }
}
