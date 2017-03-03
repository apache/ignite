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
package org.apache.ignite.internal.processors.cache.eviction.paged;

import java.util.Arrays;
import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class PageEvictionSmokeTest extends GridCommonAbstractTest {
    /** Size. */
    private static final int SIZE = 5000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        MemoryConfiguration dbCfg = new MemoryConfiguration();

        dbCfg.setPageCacheSize(8 * 1024 * 1024);

        cfg.setMemoryConfiguration(dbCfg);

        CacheConfiguration ccfg1 = new CacheConfiguration();

        ccfg1.setName("name1");

        ccfg1.setAffinity(new RendezvousAffinityFunction(false, 4));

        cfg.setCacheConfiguration(ccfg1);

        return cfg;
    }

    /**
     *
     */
    public void testStuff() throws Exception {
        Ignite ignite = startGrid();
        IgniteCache<Object, Object> cache1 = ignite.cache("name1");

        for (int i = 0; i < SIZE; i++) {
//            if (i % 10 == 0) {
//                cache1.put(i, new Bigger(i));
//
//                continue;
//            }
//          TODO IGNITE-4534: Bigger objects cause BufferUnderflowException sometimes.

            cache1.put(i, new Small(i));
        }

        int cnt = 0;
        for (int i = 0; i < SIZE; i++) {
            if (cache1.get((SIZE / 3 + i) % SIZE) != null)
                cnt++;
        }

        System.out.println(">>> Before eviction: " + cnt);

        for (int i = 0; i < 20; i++)
            internalCache(cache1).context().shared().database().evictionTracker().evictDataPage();

        int afterCnt = 0;
        for (int i = 0; i < SIZE; i++) {
            if (cache1.get(i) != null)
                afterCnt++;
        }

        System.out.println(">>> After eviction: " + afterCnt);

        assertTrue(afterCnt < cnt);

    }

    class Small {
        static final int SIZE = 28;
        int b;
        String c;
        int[] arr = new int[SIZE];

        public Small(int seed) {
            this.b = seed;
            this.c = String.valueOf(2 * seed);

            for (int i =0; i < SIZE; i++) {
                Random random = new Random(seed);
                arr[i] = random.nextInt();
            }
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Small small = (Small)o;

            if (b != small.b)
                return false;
            if (c != null ? !c.equals(small.c) : small.c != null)
                return false;
            return Arrays.equals(arr, small.arr);
        }

        @Override public int hashCode() {
            int result = b;
            result = 31 * result + (c != null ? c.hashCode() : 0);
            result = 31 * result + Arrays.hashCode(arr);
            return result;
        }
    }

    class Bigger {
        public static final int SIZE = 1170;
        int d;
        String e;
        int[] arrr = new int[SIZE];

        public Bigger(int seed) {
            this.d = 3 * seed;
            this.e = String.valueOf(4 * seed);

            for (int i =0; i < SIZE; i++) {
                Random random = new Random(seed);
                arrr[i] = random.nextInt();
            }
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Bigger bigger = (Bigger)o;

            if (d != bigger.d)
                return false;
            if (e != null ? !e.equals(bigger.e) : bigger.e != null)
                return false;
            return Arrays.equals(arrr, bigger.arrr);
        }

        @Override public int hashCode() {
            int result = d;
            result = 31 * result + (e != null ? e.hashCode() : 0);
            result = 31 * result + Arrays.hashCode(arrr);
            return result;
        }
    }
}