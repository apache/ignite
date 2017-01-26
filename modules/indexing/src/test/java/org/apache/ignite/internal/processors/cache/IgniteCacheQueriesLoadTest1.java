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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
@SuppressWarnings("unchecked")
public class IgniteCacheQueriesLoadTest1 extends GridCommonAbstractTest {
    /** Operation. */
    private static final String OPERATION = "Operation";

    /** Deposit. */
    private static final String DEPOSIT = "Deposit";

    /** Trader. */
    private static final String TRADER = "Trader";

    /** Id. */
    private static final String ID = "ID";

    /** Deposit id. */
    private static final String DEPOSIT_ID = "DEPOSIT_ID";

    /** Trader id. */
    private static final String TRADER_ID = "TRADER_ID";

    /** Firstname. */
    private static final String FIRSTNAME = "FIRSTNAME";

    /** Secondname. */
    private static final String SECONDNAME = "SECONDNAME";

    /** Email. */
    private static final String EMAIL = "EMAIL";

    /** Business day. */
    private static final String BUSINESS_DAY = "BUSINESS_DAY";

    /** Trader link. */
    private static final String TRADER_LINK = "TRADER";

    /** Balance. */
    private static final String BALANCE = "BALANCE";

    /** Margin rate. */
    private static final String MARGIN_RATE = "MARGIN_RATE";

    /** Balance on day open. */
    private static final String BALANCE_ON_DAY_OPEN = "BALANCEDO";

    /** Trader cache name. */
    private static final String TRADER_CACHE = "TRADER_CACHE";

    /** Deposit cache name. */
    private static final String DEPOSIT_CACHE = "DEPOSIT_CACHE";

    /** History of operation over deposit. */
    private static final String DEPOSIT_HISTORY_CACHE = "DEPOSIT_HISTORY_CACHE";

    /** Count of operations by deposit. */
    private static final String DEPOSIT_OPERATION_COUNT_SQL = "SELECT COUNT(*) FROM \"" + DEPOSIT_HISTORY_CACHE
        + "\"." + OPERATION + " WHERE " + "DEPOSIT_ID" + "=?";

    /** Get last history row. */
    private static final String LAST_HISTORY_ROW_SQL = "SELECT MAX("+BUSINESS_DAY+") FROM \""+DEPOSIT_HISTORY_CACHE
        + "\"." + OPERATION + " WHERE " + "DEPOSIT_ID" + "=?";

    /** Find deposit SQL query. */
    private static final String FIND_DEPOSIT_SQL = "SELECT _key FROM \"" + DEPOSIT_CACHE + "\"." + DEPOSIT
        + " WHERE " + TRADER_ID + "=?";

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 5;

    /** Distribution of partitions by nodes. */
    private Map<UUID, List<Integer>> partitionsMap;

    /** Preload amount. */
    private final int preloadAmount = 10_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setIncludeEventTypes();

        cfg.setMarshaller(null);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        RendezvousAffinityFunction aff = new RendezvousAffinityFunction();
        aff.setPartitions(3000);

        CacheConfiguration<Object, Object> parentCfg = new CacheConfiguration<>();
        parentCfg.setAffinity(aff);
        parentCfg.setAtomicityMode(TRANSACTIONAL);
        parentCfg.setCacheMode(PARTITIONED);
        parentCfg.setMemoryMode(OFFHEAP_TIERED);
        parentCfg.setBackups(2);
        parentCfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(
            getTraderCfg(parentCfg),
            getDepositCfg(parentCfg),
            getDepositHistoryCfg(parentCfg)
        );

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueries() throws Exception {
        runQueries(1, true, 10_000);

        runQueries(10, false, 30_000);
    }

    /**
     * @param threads Threads number.
     * @param checkBalance Check balance flag.
     * @param time Execution time.
     * @throws Exception If failed.
     */
    private void runQueries(int threads, final boolean checkBalance, final long time) throws Exception {
        final Ignite ignite = grid(0);

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() {
                long endTime = System.currentTimeMillis() + time;

                while (System.currentTimeMillis() < endTime) {
                    ScanQueryBroadcastClosure c = new ScanQueryBroadcastClosure(partitionsMap, checkBalance);

                    ignite.compute().broadcast(c);
                }

                return null;
            }
        }, threads, "test-thread");
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(NODES);

        partitionsMap = traderCachePartitions(ignite(0));

        assertEquals(NODES, partitionsMap.size());

        preLoading();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        assert G.allGrids().isEmpty();
    }

    /**
     * @throws Exception If fail.
     */
    private void preLoading() throws Exception {
        final Thread preloadAccount = new Thread() {
            @Override public void run() {
                setName("preloadTraders");

                Ignite ignite = ignite(0);

                try (IgniteDataStreamer dataLdr = ignite.dataStreamer(TRADER_CACHE)) {
                    for (int i = 0; i < preloadAmount && !isInterrupted(); i++) {
                        String traderKey = "traderId=" + i;

                        dataLdr.addData(traderKey, createTrader(ignite, traderKey));
                    }
                }
            }
        };

        preloadAccount.start();

        Thread preloadTrade = new Thread() {
            @Override public void run() {
                setName("preloadDeposits");

                Ignite ignite = ignite(0);

                try (IgniteDataStreamer dataLdr = ignite.dataStreamer(DEPOSIT_CACHE)) {
                    for (int i = 0; i < preloadAmount && !isInterrupted(); i++) {
                        int traderId = nextRandom(preloadAmount);

                        String traderKey = "traderId=" + traderId;
                        String key = traderKey + "&depositId=" + i;

                        dataLdr.addData(key, createDeposit(ignite, key, traderKey, i));
                    }
                }
            }
        };

        preloadTrade.start();

        preloadTrade.join();
        preloadAccount.join();
    }

    /**
     * @param ignite Node.
     * @param id Identifier.
     * @return Trader entity as binary object.
     */
    private BinaryObject createTrader(Ignite ignite, String id) {
        return ignite.binary()
            .builder(TRADER)
            .setField(ID, id)
            .setField(FIRSTNAME, "First name " + id)
            .setField(SECONDNAME, "Second name " + id)
            .setField(EMAIL, "trader" + id + "@mail.org")
            .build();
    }

    /**
     * @param ignite Node.
     * @param id Identifier.
     * @param traderId Key.
     * @param num Num.
     * @return Deposit entity as binary object.
     */
    private BinaryObject createDeposit(Ignite ignite, String id, String traderId, int num) {
        double startBalance = 100 + nextRandom(100) / 1.123;

        return ignite.binary()
            .builder(DEPOSIT)
            .setField(ID, id)
            .setField(TRADER_ID, traderId)
            .setField(TRADER_LINK, num)
            .setField(BALANCE, new BigDecimal(startBalance))
            .setField(MARGIN_RATE, new BigDecimal(0.1))
            .setField(BALANCE_ON_DAY_OPEN, new BigDecimal(startBalance))
            .build();
    }

    /**
     * Building a map that contains mapping of node ID to a list of partitions stored on the node.
     *
     * @param ignite Node.
     * @return Node to partitions map.
     */
    private Map<UUID, List<Integer>> traderCachePartitions(Ignite ignite) {
        // Getting affinity for account cache.
        Affinity<?> affinity = ignite.affinity(TRADER_CACHE);

        // Building a list of all partitions numbers.
        List<Integer> partNumbers = new ArrayList<>(affinity.partitions());

        for (int i = 0; i < affinity.partitions(); i++)
            partNumbers.add(i);

        // Getting partition to node mapping.
        Map<Integer, ClusterNode> partPerNodes = affinity.mapPartitionsToNodes(partNumbers);

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
        private final Map<UUID, List<Integer>> cachePart;

        /** */
        private final boolean checkBalance;

        /**
         * @param cachePart Partition by node for Ignite cache.
         * @param checkBalance Check balance flag.
         */
        private ScanQueryBroadcastClosure(Map<UUID, List<Integer>> cachePart, boolean checkBalance) {
            this.cachePart = cachePart;
            this.checkBalance = checkBalance;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                IgniteCache traders = node.cache(TRADER_CACHE).withKeepBinary();

                IgniteCache<String, BinaryObject> depositCache = node.cache(DEPOSIT_CACHE).withKeepBinary();

                // Getting a list of the partitions owned by this node.
                List<Integer> myPartitions = cachePart.get(node.cluster().localNode().id());

                for (Integer part : myPartitions) {
                    ScanQuery scanQry = new ScanQuery();

                    scanQry.setPartition(part);

                    try (QueryCursor<Cache.Entry<String, BinaryObject>> cursor = traders.query(scanQry)) {
                        for (Cache.Entry<String, BinaryObject> entry : cursor) {
                            String traderId = entry.getKey();

                            SqlFieldsQuery findDepositQry = new SqlFieldsQuery(FIND_DEPOSIT_SQL).setLocal(true);

                            try (QueryCursor cursor1 = depositCache.query(findDepositQry.setArgs(traderId))) {
                                for (Object obj : cursor1) {
                                    List<String> depositIds = (List<String>)obj;

                                    for (String depositId : depositIds) {
                                        updateDeposit(depositCache, depositId);

                                        checkDeposit(depositCache, depositId);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }

        /**
         * @param depositCache Ignite cache of deposit.
         * @param depositKey Key of deposit.
         * @throws Exception If failed.
         */
        private void updateDeposit(final IgniteCache<String, BinaryObject> depositCache, final String depositKey)
            throws Exception {
            final IgniteCache histCache = node.cache(DEPOSIT_HISTORY_CACHE).withKeepBinary();

            doInTransaction(node, PESSIMISTIC,
                REPEATABLE_READ, new IgniteCallable<Object>() {
                    @Override public Object call() throws Exception {
                        BinaryObject deposit = depositCache.get(depositKey);

                        BigDecimal amount = deposit.field(BALANCE);
                        BigDecimal rate = deposit.field(MARGIN_RATE);

                        BigDecimal newBalance = amount.multiply(rate.add(BigDecimal.ONE));

                        deposit = deposit.toBuilder()
                            .setField(BALANCE, newBalance)
                            .build();

                        SqlFieldsQuery findDepositHist = new SqlFieldsQuery(LAST_HISTORY_ROW_SQL).setLocal(true);

                        try (QueryCursor cursor1 = histCache.query(findDepositHist.setArgs(depositKey))) {
                            for (Object o: cursor1){
                                // No-op.
                            }
                        }

                        String depositHistKey = depositKey + "&histId=" + System.nanoTime();

                        BinaryObject depositHistRow = node.binary().builder(OPERATION)
                            .setField(ID, depositHistKey)
                            .setField(DEPOSIT_ID, depositKey)
                            .setField(BUSINESS_DAY, new Date())
                            .setField(BALANCE, newBalance)
                            .build();

                        histCache.put(depositHistKey, depositHistRow);

                        depositCache.put(depositKey, deposit);

                        return null;
                    }
                });
        }

        /**
         * @param depositCache Deposit cache.
         * @param depositKey Deposit key.
         */
        private void checkDeposit(IgniteCache<String, BinaryObject> depositCache, String depositKey) {
            IgniteCache histCache = node.cache(DEPOSIT_HISTORY_CACHE).withKeepBinary();

            BinaryObject deposit = depositCache.get(depositKey);

            BigDecimal startBalance = deposit.field(BALANCE_ON_DAY_OPEN);

            BigDecimal balance = deposit.field(BALANCE);

            BigDecimal rate = deposit.field(MARGIN_RATE);

            BigDecimal expBalance;

            SqlFieldsQuery findDepositHist = new SqlFieldsQuery(DEPOSIT_OPERATION_COUNT_SQL);

            try (QueryCursor cursor1 = histCache.query(findDepositHist.setArgs(depositKey))) {
                Long cnt = (Long)((ArrayList)cursor1.iterator().next()).get(0);

                expBalance = startBalance.multiply(rate.add(BigDecimal.ONE).pow(cnt.intValue()));
            }

            expBalance = expBalance.setScale(2, BigDecimal.ROUND_DOWN);
            balance = balance.setScale(2, BigDecimal.ROUND_DOWN);

            if (checkBalance && !expBalance.equals(balance)) {
                node.log().error("Deposit " + depositKey + " has incorrect balance "
                    + balance + " when expected " + expBalance, null);

                throw new IgniteException("Deposit " + depositKey + " has incorrect balance "
                    + balance + " when expected " + expBalance);

            }
        }
    }

    /**
     * @param max Max.
     * @return Random value.
     */
    private static int nextRandom(int max) {
        return ThreadLocalRandom.current().nextInt(max);
    }

    /**
     * @param parentCfg Parent config.
     * @return Configuration.
     */
    private static CacheConfiguration<Object, Object> getDepositHistoryCfg(
        CacheConfiguration<Object, Object> parentCfg) {
        CacheConfiguration<Object, Object> depositHistCfg = new CacheConfiguration<>(parentCfg);
        depositHistCfg.setName(DEPOSIT_HISTORY_CACHE);

        String strCls = String.class.getCanonicalName();
        String dblCls = Double.class.getCanonicalName();
        String dtCls = Date.class.getCanonicalName();

        LinkedHashMap<String, String> qryFields = new LinkedHashMap<>();
        qryFields.put(ID, strCls);
        qryFields.put(DEPOSIT_ID, strCls);
        qryFields.put(BUSINESS_DAY, dtCls);
        qryFields.put(BALANCE, dblCls);

        QueryEntity qryEntity = new QueryEntity();
        qryEntity.setValueType(OPERATION);
        qryEntity.setKeyType(strCls);
        qryEntity.setFields(qryFields);
        qryEntity.setIndexes(Arrays.asList(new QueryIndex(ID, true), new QueryIndex(DEPOSIT_ID, true)));

        depositHistCfg.setQueryEntities(Collections.singleton(qryEntity));

        return depositHistCfg;
    }

    /**
     * @param parentCfg Parent config.
     * @return Configuration.
     */
    private static CacheConfiguration<Object, Object> getDepositCfg(CacheConfiguration<Object, Object> parentCfg) {
        CacheConfiguration<Object, Object> depositCfg = new CacheConfiguration<>(parentCfg);
        depositCfg.setName(DEPOSIT_CACHE);

        String strCls = String.class.getCanonicalName();
        String dblCls = Double.class.getCanonicalName();
        String intCls = Integer.class.getCanonicalName();

        LinkedHashMap<String, String> qryFields = new LinkedHashMap<>();
        qryFields.put(ID, strCls);
        qryFields.put(TRADER_ID, strCls);
        qryFields.put(TRADER_LINK, intCls);
        qryFields.put(BALANCE, dblCls);
        qryFields.put(MARGIN_RATE, dblCls);
        qryFields.put(BALANCE_ON_DAY_OPEN, dblCls);

        QueryEntity qryEntity = new QueryEntity();
        qryEntity.setValueType(DEPOSIT);
        qryEntity.setKeyType(strCls);
        qryEntity.setFields(qryFields);
        qryEntity.setIndexes(Collections.singleton(new QueryIndex(ID, false)));

        depositCfg.setQueryEntities(Collections.singleton(qryEntity));
        return depositCfg;
    }

    /**
     * @param parentCfg Parent config.
     * @return Configuration.
     */
    private static CacheConfiguration<Object, Object> getTraderCfg(CacheConfiguration<Object, Object> parentCfg) {
        CacheConfiguration<Object, Object> traderCfg = new CacheConfiguration<>(parentCfg);
        traderCfg.setName(TRADER_CACHE);

        String strCls = String.class.getCanonicalName();

        LinkedHashMap<String, String> qryFields = new LinkedHashMap<>();
        qryFields.put(ID, strCls);
        qryFields.put(FIRSTNAME, strCls);
        qryFields.put(SECONDNAME, strCls);
        qryFields.put(EMAIL, strCls);

        QueryEntity qryEntity = new QueryEntity();
        qryEntity.setValueType(TRADER);
        qryEntity.setKeyType(strCls);
        qryEntity.setFields(qryFields);

        LinkedHashMap<String, Boolean> grpIdx = new LinkedHashMap<>();
        grpIdx.put(FIRSTNAME, false);
        grpIdx.put(SECONDNAME, false);

        qryEntity.setIndexes(Arrays.asList(
            new QueryIndex(ID, true),
            new QueryIndex(grpIdx, QueryIndexType.FULLTEXT)
        ));

        traderCfg.setQueryEntities(Collections.singleton(qryEntity));
        return traderCfg;
    }
}
