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

package org.apache.ignite.internal.pagemem.wal.record;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheStateSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    public void testUnsorted() {
        for (int r = 0; r < 500; r++) {
            checkMap(new HashMap<Integer, IgniteBiTuple<Long, Long>>(), true);
        }
    }

    /**
     *
     */
    public void testSorted() {
        for (int r = 0; r < 500; r++) {
            checkMap(new TreeMap<Integer, IgniteBiTuple<Long, Long>>(), false);
        }
    }

    /**
     * @param map Map to check.
     * @param needSort {@code True} if needs sorting.
     */
    private void checkMap(Map<Integer, IgniteBiTuple<Long, Long>> map, boolean needSort) {
        Random rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 10_000; i++) {
            map.put(rnd.nextInt(CacheConfiguration.MAX_PARTITIONS_COUNT),
                F.t(rnd.nextLong(), rnd.nextLong()));
        }

        CacheState state = new CacheState(map.size());

        for (Map.Entry<Integer, IgniteBiTuple<Long, Long>> e : map.entrySet())
            state.addPartitionState(e.getKey(), e.getValue().get1(), e.getValue().get2());

        if (needSort)
            state.checkSorted();

        for (Map.Entry<Integer, IgniteBiTuple<Long, Long>> e : map.entrySet()) {
            assertEquals(e.getValue().get1(), (Long)state.sizeByPartition(e.getKey()));
            assertEquals(e.getValue().get2(), (Long)state.counterByPartition(e.getKey()));
        }
    }
}
