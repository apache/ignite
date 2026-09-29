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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.topology.MdcTopologyValidator;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Cuts one DC off the cluster: the side the topology validator lets write keeps writing, the cut-off DC only reads,
 * and after the cut-off DC restarts every DC holds the same data. Also splits three DCs three ways, where no side
 * writes.
 */
public class MdcDcIsolationTest extends MdcTopologySplitAbstractTest {
    /** */
    private static final int KEYS = 10;

    /** DCs of the current test. */
    private List<String> dcs;

    /** {@inheritDoc} */
    @Override protected List<String> dataCenters() {
        return dcs;
    }

    /** Three DCs with majority validation: DC3 alone is a minority, DC1 and DC2 keep writing. */
    @Test
    public void testIsolatedDcOfThree() throws Exception {
        checkIsolation(Arrays.asList(DC1, DC2, DC3), majorityValidator(DC1, DC2, DC3), DC3, DC1);
    }

    /** Three DCs with majority validation: DC1 alone is a minority too, no DC is privileged. */
    @Test
    public void testIsolatedFirstDcOfThree() throws Exception {
        checkIsolation(Arrays.asList(DC1, DC2, DC3), majorityValidator(DC1, DC2, DC3), DC1, DC2);
    }

    /** Three DCs with majority validation split three ways: no side writes, every side reads. */
    @Test
    public void testThreeWaySplit() throws Exception {
        dcs = Arrays.asList(DC1, DC2, DC3);

        MdcTopologyValidator validator = majorityValidator(DC1, DC2, DC3);

        startCluster();

        Map<String, Map<Integer, Integer>> expected = fill(DC1, validator);

        splitInto(Arrays.asList(Arrays.asList(DC1), Arrays.asList(DC2), Arrays.asList(DC3)));

        for (String dc : dcs) {
            for (String cacheName : expected.keySet()) {
                IgniteCache<Integer, Integer> cache = client(dc).cache(cacheName);

                assertWriteRejected(() -> cache.put(KEYS, KEYS));

                assertEquals(Integer.valueOf(1), cache.get(1));
            }
        }

        heal(DC2, DC3);

        for (Map.Entry<String, Map<Integer, Integer>> e : expected.entrySet())
            assertDataInEveryDc(e.getKey(), e.getValue());

        assertPartitionsSame(idleVerify(client(DC1), expected.keySet().toArray(new String[0])));
    }

    /** Two DCs with DC1 as the main one: DC2 cut off from it only reads. */
    @Test
    public void testIsolatedDcOfTwo() throws Exception {
        checkIsolation(Arrays.asList(DC1, DC2), mainDcValidator(DC1), DC2, DC1);
    }

    /**
     * @param dcs DCs of the cluster.
     * @param validator Topology validator of the caches.
     * @param isolatedDc DC to cut off; must end up read-only.
     * @param writerDc DC that keeps writing.
     */
    private void checkIsolation(
        List<String> dcs,
        MdcTopologyValidator validator,
        String isolatedDc,
        String writerDc
    ) throws Exception {
        this.dcs = dcs;

        startCluster();

        Map<String, Map<Integer, Integer>> expected = fill(writerDc, validator);

        split(isolatedDc);

        for (Map.Entry<String, Map<Integer, Integer>> e : expected.entrySet()) {
            IgniteCache<Integer, Integer> writer = client(writerDc).cache(e.getKey());
            IgniteCache<Integer, Integer> isolated = client(isolatedDc).cache(e.getKey());

            writer.put(KEYS, KEYS);
            e.getValue().put(KEYS, KEYS);

            assertEquals(Integer.valueOf(0), writer.get(0));

            assertWriteRejected(() -> isolated.put(KEYS + 1, KEYS + 1));

            assertEquals(Integer.valueOf(1), isolated.get(1));
        }

        heal(isolatedDc);

        for (Map.Entry<String, Map<Integer, Integer>> e : expected.entrySet())
            assertDataInEveryDc(e.getKey(), e.getValue());

        assertPartitionsSame(idleVerify(client(writerDc), expected.keySet().toArray(new String[0])));
    }

    /**
     * Creates an atomic and a transactional cache and writes {@link #KEYS} keys into each.
     *
     * @param dc DC whose client creates and fills the caches.
     * @param validator Topology validator of the caches.
     * @return Content of each cache by its name.
     */
    private Map<String, Map<Integer, Integer>> fill(String dc, MdcTopologyValidator validator) {
        Map<String, Map<Integer, Integer>> expected = new HashMap<>();

        for (CacheAtomicityMode mode : new CacheAtomicityMode[] {ATOMIC, TRANSACTIONAL}) {
            IgniteCache<Integer, Integer> cache = client(dc).createCache(cacheConfiguration(mode.name(), mode, validator));

            Map<Integer, Integer> data = new HashMap<>();

            for (int key = 0; key < KEYS; key++)
                data.put(key, key);

            cache.putAll(data);

            expected.put(mode.name(), data);
        }

        return expected;
    }
}
