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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;

import static javax.cache.Cache.Entry;

/**
 * Test store.
 */
@SuppressWarnings({"TypeParameterExtendsFinalClass"})
public class GridCacheGenericTestStore<K, V> implements CacheStore<K, V> {
    /** Store. */
    private final Map<K, V> map = new ConcurrentHashMap<>();

    /** Last method called. */
    private String lastMtd;

    /** */
    private long ts = System.currentTimeMillis();

    /** {@link #write(Entry)} method call counter .*/
    private AtomicInteger putCnt = new AtomicInteger();

    /** {@link #writeAll(Collection)} method call counter .*/
    private AtomicInteger putAllCnt = new AtomicInteger();

    /** {@link #delete(Object)} method call counter. */
    private AtomicInteger rmvCnt = new AtomicInteger();

    /** {@link #deleteAll(Collection)} method call counter. */
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
     * @return Count of {@link #write(Entry)} method calls since last reset.
     */
    public int getPutCount() {
        return putCnt.get();
    }

    /**
     * @return Count of {@link #writeAll(Collection)} method calls since last reset.
     */
    public int getPutAllCount() {
        return putAllCnt.get();
    }

    /**
     * @return Number of {@link #delete(Object)} method calls since last reset.
     */
    public int getRemoveCount() {
        return rmvCnt.get();
    }

    /**
     * @return Number of {@link #deleteAll(Collection)} method calls since last reset.
     */
    public int getRemoveAllCount() {
        return rmvAllCnt.get();
    }

    /** {@inheritDoc} */
    @Override public V load(K key) {
        lastMtd = "load";

        checkOperation();

        return map.get(key);
    }

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<K, V> clo, Object[] args) {
        lastMtd = "loadAllFull";

        checkOperation();
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> loadAll(Iterable<? extends K> keys) {
        lastMtd = "loadAll";

        Map<K, V> loaded = new HashMap<>();

        for (K key : keys) {
            V val = map.get(key);

            if (val != null)
                loaded.put(key, val);
        }

        checkOperation();

        return loaded;
    }

    /** {@inheritDoc} */
    @Override public void write(Cache.Entry<? extends K, ? extends V> e) {
        lastMtd = "put";

        checkOperation();

        map.put(e.getKey(), e.getValue());

        putCnt.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> entries) {
        lastMtd = "putAll";

        checkOperation();

        for (Cache.Entry<? extends K, ? extends V> e : entries)
            this.map.put(e.getKey(), e.getValue());

        putAllCnt.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public void delete(Object key) {
        lastMtd = "remove";

        checkOperation();

        map.remove(key);

        rmvCnt.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public void deleteAll(Collection<?> keys) {
        lastMtd = "removeAll";

        checkOperation();

        for (Object key : keys)
            map.remove(key);

        rmvAllCnt.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public void sessionEnd(boolean commit) {
        // No-op.
    }

    /**
     * Checks the flag and throws exception if it is set. Checks operation delay and sleeps
     * for specified amount of time, if needed.
     */
    private void checkOperation() {
        if (shouldFail)
            throw new IgniteException("Store exception");

        if (operationDelay > 0) {
            try {
                U.sleep(operationDelay);
            }
            catch(IgniteInterruptedCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }
}