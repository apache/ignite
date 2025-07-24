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

package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongFunction;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.datastructures.AtomicDataStructureValue;
import org.apache.ignite.internal.processors.datastructures.GridCacheAtomicSequenceValue;
import org.apache.ignite.internal.processors.datastructures.GridCacheInternalKey;
import org.apache.ignite.internal.processors.datastructures.GridCacheInternalKeyImpl;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.DEFAULT_DS_GROUP_NAME;

/**
 *
 */
public class AtomicDataStructuresWithCacheStoreTest extends GridCommonAbstractTest {
    /** Servers count. */
    public static final int SRV_CNT = 3;

    /** Atomic sequence reserve size. */
    public static final int RESERVE_SIZE = 7;

    /** Sequence initial value. */
    public static final int INIT_VAL = 10;

    /** Local store. */
    private static final Map<GridCacheInternalKey, AtomicDataStructureValue> localStore = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(false)
                    )
            )
            .setAtomicConfiguration(new AtomicConfiguration()
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(1)
                .setAtomicSequenceReserveSize(RESERVE_SIZE)
                .setCacheStoreFactory(new TestFactory()));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
        cleanPersistenceDir();

        localStore.clear();
    }

    /** */
    @Test
    public void testAtomicSequence() throws Exception {
        int restartCnt = 3;

        LongFunction<Long> expStoredFun = cnt -> INIT_VAL + RESERVE_SIZE * cnt;

        int reservedCnt = 0;

        for (int i = 0; i < restartCnt; i++) {
            startGrids(SRV_CNT).cluster().state(ClusterState.ACTIVE);

            for (int j = 0; j < SRV_CNT; j++) {
                // Here sequence is persisted with an upper bound.
                IgniteAtomicSequence seq = grid(j).atomicSequence("test", INIT_VAL, true);

                reservedCnt++;

                // Initial local value is always less, than persisted one
                long expCurVal = expStoredFun.apply(reservedCnt) - RESERVE_SIZE;

                assertEquals("Unexpected current value after init: i=" + i + ", j=" + j, expCurVal, seq.get());
                assertEquals("Unexpected stored value after init: i=" + i + ", j=" + j, expStoredFun.apply(reservedCnt), seqVal());

                seq.addAndGet(RESERVE_SIZE - 1);

                expCurVal += RESERVE_SIZE - 1;

                assertEquals("Unexpected current value before new reservation: i=" + i + ", j=" + j, expCurVal, seq.get());
                assertEquals("Unexpected stored value before new reservation" + i + ", j=" + j, expStoredFun.apply(reservedCnt), seqVal());

                // Causes new reservation and causes persisting of sequence with a new upper bound.
                seq.incrementAndGet();

                expCurVal++;
                reservedCnt++;

                assertEquals("Unexpected current value after new reservation: i=" + i + ", j=" + j, expCurVal, seq.get());
                assertEquals("Unexpected stored value after new reservation" + i + ", j=" + j, expStoredFun.apply(reservedCnt), seqVal());
            }

            stopAllGrids();
        }
    }

    /**
     * AtomicSequence is persisted with the upper bound value.
     */
    private static Long seqVal() {
        AtomicDataStructureValue val = localStore.get(new GridCacheInternalKeyImpl("test", DEFAULT_DS_GROUP_NAME));

        return ((GridCacheAtomicSequenceValue)val).get();
    }

    /** */
    private static class TestFactory implements Factory<CacheStore<GridCacheInternalKey, AtomicDataStructureValue>> {
        /** {@inheritDoc} */
        @Override public CacheStore<GridCacheInternalKey, AtomicDataStructureValue> create() {
            return new CacheStoreAdapter<>() {
                @Override public AtomicDataStructureValue load(GridCacheInternalKey key) throws CacheLoaderException {
                    AtomicDataStructureValue val = localStore.get(key);

                    log.warning(">>>>>> Loading seq: " + val);

                    return val;
                }

                @Override public void write(Cache.Entry<? extends GridCacheInternalKey, ? extends AtomicDataStructureValue> entry)
                    throws CacheWriterException {
                    log.warning(">>>>>> Writing seq: " + entry);

                    localStore.put(entry.getKey(), entry.getValue());
                }

                @Override public void delete(Object key) throws CacheWriterException {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }
}
