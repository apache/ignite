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

package org.apache.ignite.yardstick.cache.load;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import javax.cache.Cache;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Ignite capitalization benchmark.
 * Topology should NOT be changed during benchmark processing.
 */
public class IgniteCapitalizationBenchmark extends IgniteAbstractBenchmark {
    /** Person cache name. */
    static final String PERSON_CACHE = "person";

    /** Deposit cache name. */
    static final String DEPOSIT_CACHE = "deposit";

    /** Find deposit SQL query. */
    static final String FIND_DEPOSIT_SQL = "SELECT _key FROM \"" + DEPOSIT_CACHE + "\".Deposit WHERE affKey=?";

    /** Distribution of partitions by nodes. */
    private Map<UUID, List<Integer>> partitionsMap;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        preLoading();

        partitionsMap = personCachePartitions();
    }

    /**
     * @throws Exception If fail.
     */
    private void preLoading() throws Exception {

        Thread preloadPerson = new Thread() {
            @Override public void run() {
                setName("preloadPerson");

                try (IgniteDataStreamer dataLdr = ignite().dataStreamer(PERSON_CACHE)) {
                    for (int i = 0; i < args.preloadAmount() && !isInterrupted(); i++)
                        dataLdr.addData(i, createPerson(i));
                }
            }
        };

        preloadPerson.start();

        Thread preloadDeposit = new Thread() {
            @Override public void run() {
                setName("preloadDeposit");

                try (IgniteDataStreamer dataLdr = ignite().dataStreamer(DEPOSIT_CACHE)) {
                    for (int i = 0; i < args.preloadAmount() && !isInterrupted(); i++) {
                        int personId = nextRandom(args.preloadAmount());

                        dataLdr.addData(new AffinityKey(i, personId), createDeposit(i, personId));
                    }
                }
            }
        };

        preloadDeposit.start();

        preloadDeposit.join();

        preloadPerson.join();
    }

    /**
     * @param id Identifier.
     * @return Person entity as binary object.
     */
    private BinaryObject createPerson(int id) {
        BinaryObjectBuilder clientBuilder = ignite().binary().builder("Person");

        clientBuilder.setField("firstName", "First name " + id);

        clientBuilder.setField("lastName", "Last name " + id);

        clientBuilder.setField("age", id);

        clientBuilder.setField("personId", id);

        return clientBuilder.build();
    }

    /**
     * @param id Identifier.
     * @return Deposit entity as binary object.
     */
    private BinaryObject createDeposit(int id, int affKey) {
        BinaryObjectBuilder clientBuilder = ignite().binary().builder("Deposit");

        clientBuilder.setField("accountNumber", String.format("%s%s-%s%s",id,id,id,id));

        clientBuilder.setField("amount", (long)(id % 1000));

        clientBuilder.setField("depositId", id);

        clientBuilder.setField("affKey", affKey);

        return clientBuilder.build();
    }



    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {

        ScanQueryBroadcastClosure c = new ScanQueryBroadcastClosure(partitionsMap);

        ClusterGroup clusterGrp = ignite().cluster().forNodeIds(partitionsMap.keySet());

        IgniteCompute compute = ignite().compute(clusterGrp);

        compute.broadcast(c);


        return true;
    }

    /**
     * Building a map that contains mapping of node ID to a list of partitions stored on the node.
     *
     * @param cacheName Name of Ignite cache.
     * @return Node to partitions map.
     */
    private Map<UUID, List<Integer>> personCachePartitions() {
        // Getting affinity for person cache.
        Affinity affinity = ignite().affinity(PERSON_CACHE);

        // Building a list of all partitions numbers.
        List<Integer> partitionNumbers = new ArrayList<>(affinity.partitions());

        for (int i = 0; i < affinity.partitions(); i++)
            partitionNumbers.add(i);

        // Getting partition to node mapping.
        Map<Integer, ClusterNode> partPerNodes = affinity.mapPartitionsToNodes(partitionNumbers);

        // Building node to partitions mapping.
        Map<UUID, List<Integer>> nodesToPart = new HashMap<>();

        for (Map.Entry<Integer, ClusterNode> entry : partPerNodes.entrySet()) {
            List<Integer> nodeParts = nodesToPart.get(entry.getValue().id());

            if (nodeParts == null) {
                nodeParts = new ArrayList<>();

                nodesToPart.put(entry.getValue().id(), nodeParts);
            }
            nodeParts.add(entry.getKey());
        }

        return nodesToPart;
    }

    /**
     * Closure for scan query executing.
     */
    private static class ScanQueryBroadcastClosure implements IgniteRunnable {
        /**
         * Ignite node.
         */
        @IgniteInstanceResource
        private Ignite node;

        /**
         * Information about partition.
         */
        private Map<UUID, List<Integer>> cachePart;

        /**
         * @param cacheName Name of Ignite cache.
         * @param cachePart Partition by node for Ignite cache.
         */
        private ScanQueryBroadcastClosure(Map<UUID, List<Integer>> cachePart) {
            this.cachePart = cachePart;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            IgniteCache cache = node.cache(PERSON_CACHE).withKeepBinary();

            // Getting a list of the partitions owned by this node.
            List<Integer> myPartitions = cachePart.get(node.cluster().localNode().id());

            for (Integer part : myPartitions) {

                ScanQuery scanQry = new ScanQuery();

                scanQry.setPartition(part);

                try (QueryCursor<Cache.Entry<Integer, BinaryObject>> cursor = cache.query(scanQry)) {
                    for (Cache.Entry<Integer, BinaryObject> entry : cursor) {
                        Integer personId = entry.getKey();

                        IgniteCache<AffinityKey, BinaryObject> depositCache = node.cache(DEPOSIT_CACHE);

                        SqlFieldsQuery findDepositQuery = new SqlFieldsQuery(FIND_DEPOSIT_SQL).setLocal(true);

                        try (QueryCursor cursor1 = depositCache.query(findDepositQuery.setArgs(personId))) {
                            for (Object obj : cursor1) {
                                List<AffinityKey> depositKeys = (List<AffinityKey>)obj;
                                for (AffinityKey affinityKey: depositKeys)
                                    updateDeposit(affinityKey);
                            }
                        }
                    }
                }

            }
        }

        /**
         * @param affinityKey Deposit affinity key.
         */
        private void updateDeposit(AffinityKey affinityKey) {
            try (Transaction transaction = node.transactions().txStart(
                TransactionConcurrency.PESSIMISTIC,
                TransactionIsolation.REPEATABLE_READ)) {

                IgniteCache<AffinityKey, BinaryObject> depositCache = node.cache(DEPOSIT_CACHE)
                    .withKeepBinary();
                BinaryObject deposit = depositCache.get(affinityKey);

                Long amount = deposit.field("amount");

                deposit = deposit.toBuilder()
                    .setField("amount", (long)((amount * 1.01 > amount) ? (amount * 1.01) : (amount + 1)))
                    .build();

                depositCache.put(affinityKey, deposit);

                transaction.commit();
            }
        }
    }
}
