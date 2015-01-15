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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Test store.
 */
@SuppressWarnings({"TypeParameterExtendsFinalClass"})
public class GridCacheGenericTestStore<K, V> implements GridCacheStore<K, V> {
    /** Store. */
    private final Map<K, V> map = new ConcurrentHashMap<>();

    /** Last method called. */
    private String lastMtd;

    /** */
    private long ts = System.currentTimeMillis();

    /** {@link #put(IgniteTx, Object, Object)} method call counter .*/
    private AtomicInteger putCnt = new AtomicInteger();

    /** {@link #putAll(IgniteTx, Map)} method call counter .*/
    private AtomicInteger putAllCnt = new AtomicInteger();

    /** {@link #remove(IgniteTx, Object)} method call counter. */
    private AtomicInteger rmvCnt = new AtomicInteger();

    /** {@link #removeAll(IgniteTx, Collection)} method call counter. */
    private AtomicInteger rmvAllCnt = new AtomicInteger();

    /** Flag indicating if methods of this store should fail. */
    private volatile boolean shouldFail;

    /** Configurable delay to simulate slow storage. */
    private int operationDelay;

    /**
     * @return Underlying map.
     */
    public Map<K, V> getMap() {
        return Collections.unmodifiableMap(map);
    }

    /**
     * Sets a flag indicating if methods of this class should fail with {@link IgniteCheckedException}.
     *
     * @param shouldFail {@code true} if should fail.
     */
    public void setShouldFail(boolean shouldFail) {
        this.shouldFail = shouldFail;
    }

    /**
     * Sets delay that this store should wait on each operation.
     *
     * @param operationDelay If zero, no delay applied, positive value means
     *        delay in milliseconds.
     */
    public void setOperationDelay(int operationDelay) {
        assert operationDelay >= 0;

        this.operationDelay = operationDelay;
    }

    /**
     *
     * @return Last method called.
     */
    public String getLastMethod() {
        return lastMtd;
    }

    /**
     * @return Last timestamp.
     */
    public long getTimestamp() {
        return ts;
    }

    /**
     * @return Integer timestamp.
     */
    public int getStart() {
        return Math.abs((int)ts);
    }

    /**
     * Sets last method to <tt>null</tt>.
     */
    public void resetLastMethod() {
        lastMtd = null;
    }

    /**
     * Resets timestamp.
     */
    public void resetTimestamp() {
        ts = System.currentTimeMillis();
    }

    /**
     * Resets the store to initial state.
     */
    public void reset() {
        lastMtd = null;

        map.clear();

        putCnt.set(0);
        putAllCnt.set(0);
        rmvCnt.set(0);
        rmvAllCnt.set(0);

        ts = System.currentTimeMillis();
    }

    /**
     * @return Count of {@link #put(IgniteTx, Object, Object)} method calls since last reset.
     */
    public int getPutCount() {
        return putCnt.get();
    }

    /**
     * @return Count of {@link #putAll(IgniteTx, Map)} method calls since last reset.
     */
    public int getPutAllCount() {
        return putAllCnt.get();
    }

    /**
     * @return Number of {@link #remove(IgniteTx, Object)} method calls since last reset.
     */
    public int getRemoveCount() {
        return rmvCnt.get();
    }

    /**
     * @return Number of {@link #removeAll(IgniteTx, Collection)} method calls since last reset.
     */
    public int getRemoveAllCount() {
        return rmvAllCnt.get();
    }

    /** {@inheritDoc} */
    @Override public V load(IgniteTx tx, K key) throws IgniteCheckedException {
        lastMtd = "load";

        checkOperation();

        return map.get(key);
    }

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<K, V> clo, Object[] args)
        throws IgniteCheckedException {
        lastMtd = "loadAllFull";

        checkOperation();
    }

    /** {@inheritDoc} */
    @Override public void loadAll(IgniteTx tx, Collection<? extends K> keys,
        IgniteBiInClosure<K, V> c) throws IgniteCheckedException {
        lastMtd = "loadAll";

        for (K key : keys) {
            V val = map.get(key);

            if (val != null)
                c.apply(key, val);
        }

        checkOperation();
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable IgniteTx tx, K key, V val)
        throws IgniteCheckedException {
        lastMtd = "put";

        checkOperation();

        map.put(key, val);

        putCnt.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public void putAll(IgniteTx tx, Map<? extends K, ? extends V> map)
        throws IgniteCheckedException {
        lastMtd = "putAll";

        checkOperation();

        this.map.putAll(map);

        putAllCnt.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public void remove(IgniteTx tx, K key) throws IgniteCheckedException {
        lastMtd = "remove";

        checkOperation();

        map.remove(key);

        rmvCnt.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public void removeAll(IgniteTx tx, Collection<? extends K> keys)
        throws IgniteCheckedException {
        lastMtd = "removeAll";

        checkOperation();

        for (K key : keys)
            map.remove(key);

        rmvAllCnt.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public void txEnd(IgniteTx tx, boolean commit) {
        // No-op.
    }

    /**
     * Checks the flag and throws exception if it is set. Checks operation delay and sleeps
     * for specified amount of time, if needed.
     *
     * @throws IgniteCheckedException Always if flag is set.
     */
    private void checkOperation() throws IgniteCheckedException {
        if (shouldFail)
            throw new IgniteCheckedException("Store exception");

        if (operationDelay > 0)
            U.sleep(operationDelay);
    }
}
