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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

@RunWith(Parameterized.class)
public class ForwardCursorFailureReproducer extends GridCommonAbstractTest {
    @Parameterized.Parameters
    public static List<Object[]> p() {
        return Arrays.asList(new Object[100][0]);
    }

    private static int dummy;

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setPageSize(1024)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));
    }

    @Before
    public void cleanWorkDir() throws Exception {
        cleanPersistenceDir();
    }

    @After
    public void stopCluster() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    @Test
    public void massiveReadWriteRemove() throws Exception {
        IgniteEx ign = startGrid(0);
        ign.cluster().active(true);

        CacheAtomicityMode atomicityMode = ATOMIC;

        IgniteCache<Object, Object> cache = ign.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(1, null))
            .setAtomicityMode(atomicityMode)
            .setIndexedTypes(Integer.class, Integer.class));

        AtomicBoolean stop = new AtomicBoolean();

        AtomicInteger idGen = new AtomicInteger();

        IgniteInternalFuture<?> wFut = multithreadedAsync(() -> {
            try {
                int tid = idGen.getAndIncrement();
                int min = tid * 100;
                int max = min + 100;

                for (int i = 0; i < 3000 && !stop.get();) {
                    HashMap<Integer, Integer> putMap = new HashMap<>();
                    HashSet<Integer> rmvSet = new HashSet<>();
                    for (int j = 0; j < 20; j++) {
                        int key = (i % 100) + min;
                        putMap.put(key, ThreadLocalRandom.current().nextInt());
                        rmvSet.add(key - 5);
                        i++;
                    }

                    cache.putAll(putMap);

                    cache.removeAll(rmvSet);
                }
            }
            finally {
                stop.set(true);
            }
        }, 4, "writer");

        IgniteInternalFuture<?> rFut = multithreadedAsync(() -> {
            try {
                while (!stop.get()) {
                    for (Cache.Entry<Object, Object> e : cache) {
                        dummy += e.hashCode();

                        Thread.yield();
                    }
                }
            }
            finally {
                stop.set(true);
            }
        }, 4, "reader");

        wFut.get();
        stop.set(true);

        rFut.get();

        System.out.println(dummy);
    }
}
