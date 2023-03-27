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
package org.apache.ignite.snippets;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.cache.Cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

public class CollocatedComputations {

    public static void main(String[] args) {
        Ignite ignite = Ignition.start();
        HashSet<Long> keys = new HashSet<>();
        keys.add(1L);
        keys.add(2L);
        keys.add(3L);
        keys.add(4L);

        calculateAverage(ignite, keys);
        
    }

    void collocatingByKey(Ignite ignite) {
        // tag::collocating-by-key[]
        IgniteCache<Integer, String> cache = ignite.cache("myCache");

        IgniteCompute compute = ignite.compute();

        int key = 1;

        // This closure will execute on the remote node where
        // data for the given 'key' is located.
        compute.affinityRun("myCache", key, () -> {
            // Peek is a local memory lookup.
            System.out.println("Co-located [key= " + key + ", value= " + cache.localPeek(key) + ']');
        });

        // end::collocating-by-key[]
    }

    // tag::calculate-average[]
    // this task sums up the values of the salary field for the given set of keys
    private static class SumTask implements IgniteCallable<BigDecimal> {
        private Set<Long> keys;

        public SumTask(Set<Long> keys) {
            this.keys = keys;
        }

        @IgniteInstanceResource
        private Ignite ignite;

        @Override
        public BigDecimal call() throws Exception {

            IgniteCache<Long, BinaryObject> cache = ignite.cache("person").withKeepBinary();

            BigDecimal sum = new BigDecimal(0);

            for (long k : keys) {
                BinaryObject person = cache.localPeek(k, CachePeekMode.PRIMARY);
                if (person != null)
                    sum = sum.add(new BigDecimal((float) person.field("salary")));
            }

            return sum;
        }
    }

    public static void calculateAverage(Ignite ignite, Set<Long> keys) {

        // get the affinity function configured for the cache
        Affinity<Long> affinityFunc = ignite.affinity("person");

        // this map stores collections of keys for each partition
        HashMap<Integer, Set<Long>> partMap = new HashMap<>();
        keys.forEach(k -> {
            int partId = affinityFunc.partition(k);

            Set<Long> keysByPartition = partMap.computeIfAbsent(partId, key -> new HashSet<Long>());
            keysByPartition.add(k);
        });

        BigDecimal total = new BigDecimal(0);

        IgniteCompute compute = ignite.compute();

        List<String> caches = Arrays.asList("person");

        // iterate over all partitions
        for (Map.Entry<Integer, Set<Long>> pair : partMap.entrySet()) {
            // send a task that gets specific keys for the partition
            BigDecimal sum = compute.affinityCall(caches, pair.getKey().intValue(), new SumTask(pair.getValue()));
            total = total.add(sum);
        }

        System.out.println("the average salary is " + total.floatValue() / keys.size());
    }

    // end::calculate-average[]

    // tag::sum-by-partition[]
    // this task sums up the value of the 'salary' field for all objects stored in
    // the given partition
    public static class SumByPartitionTask implements IgniteCallable<BigDecimal> {
        private int partId;

        public SumByPartitionTask(int partId) {
            this.partId = partId;
        }

        @IgniteInstanceResource
        private Ignite ignite;

        @Override
        public BigDecimal call() throws Exception {
            // use binary objects to avoid deserialization
            IgniteCache<Long, BinaryObject> cache = ignite.cache("person").withKeepBinary();

            BigDecimal total = new BigDecimal(0);
            try (QueryCursor<Cache.Entry<Long, BinaryObject>> cursor = cache
                    .query(new ScanQuery<Long, BinaryObject>(partId).setLocal(true))) {
                for (Cache.Entry<Long, BinaryObject> entry : cursor) {
                    total = total.add(new BigDecimal((float) entry.getValue().field("salary")));
                }
            }

            return total;
        }
    }

    // end::sum-by-partition[]

    public static void entryProcessor(Ignite ignite) {
        // tag::entry-processor[]
        IgniteCache<String, Integer> cache = ignite.cache("mycache");

        // Increment the value for a specific key by 1.
        // The operation will be performed on the node where the key is stored.
        // Note that if the cache does not contain an entry for the given key, it will
        // be created.
        cache.invoke("mykey", (entry, args) -> {
            Integer val = entry.getValue();

            entry.setValue(val == null ? 1 : val + 1);

            return null;
        });

        // end::entry-processor[]
    }
}
