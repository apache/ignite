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

package org.apache.ignite.internal.processors.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/**
 * Tests for {@link SqlFieldsQueryEx#skipReducerOnUpdate} flag.
 */
public class IgniteSqlSkipReducerOnUpdateDmlFlagSelfTest extends AbstractIndexingCommonTest {
    /** */
    private static final int NODE_COUNT = 4;

    /** */
    private static final String NODE_CLIENT = "client";

    /** */
    private static final String CACHE_ACCOUNT = "acc";

    /** */
    private static final String CACHE_REPORT = "rep";

    /** */
    private static final String CACHE_STOCK = "stock";

    /** */
    private static final String CACHE_TRADE = "trade";

    /** */
    private static final String CACHE_LIST = "list";

    /** */
    private static IgniteEx client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        List<CacheConfiguration> ccfgs = new ArrayList<>();

        ccfgs.add(buildCacheConfiguration(CACHE_ACCOUNT));
        ccfgs.add(buildCacheConfiguration(CACHE_STOCK));
        ccfgs.add(buildCacheConfiguration(CACHE_TRADE));
        ccfgs.add(buildCacheConfiguration(CACHE_REPORT));
        ccfgs.add(buildCacheConfiguration(CACHE_LIST));

        c.setCacheConfiguration(ccfgs.toArray(new CacheConfiguration[ccfgs.size()]));

        return c;
    }

    /**
     * Creates a cache configuration.
     *
     * @param name Name of the cache.
     * @return Cache configuration.
     */
    private CacheConfiguration buildCacheConfiguration(String name) {
        if (name.equals(CACHE_ACCOUNT)) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_ACCOUNT);

            ccfg.setCacheMode(CacheMode.PARTITIONED);

            QueryEntity entity = new QueryEntity(Integer.class, Account.class);

            ccfg.setQueryEntities(Collections.singletonList(entity));

            return ccfg;
        }
        if (name.equals(CACHE_STOCK)) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_STOCK);

            ccfg.setCacheMode(CacheMode.REPLICATED);

            QueryEntity entity = new QueryEntity(Integer.class, Stock.class);

            ccfg.setQueryEntities(Collections.singletonList(entity));

            return ccfg;
        }
        if (name.equals(CACHE_TRADE)) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_TRADE);

            ccfg.setCacheMode(CacheMode.PARTITIONED);

            QueryEntity entity = new QueryEntity(Integer.class, Trade.class);

            ccfg.setQueryEntities(Collections.singletonList(entity));

            return ccfg;
        }
        if (name.equals(CACHE_REPORT)) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_REPORT);

            ccfg.setCacheMode(CacheMode.PARTITIONED);

            QueryEntity entity = new QueryEntity(Integer.class, Report.class);

            ccfg.setQueryEntities(Collections.singletonList(entity));

            return ccfg;
        }
        if (name.equals(CACHE_LIST)) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_LIST);

            ccfg.setCacheMode(CacheMode.PARTITIONED);

            QueryEntity entity = new QueryEntity(Integer.class, String.class);

            ccfg.setQueryEntities(Collections.singletonList(entity));

            return ccfg;
        }

        assert false;

        return null;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODE_COUNT);

        client = startClientGrid(NODE_CLIENT);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        awaitPartitionMapExchange();

        client.cache(CACHE_ACCOUNT).clear();
        client.cache(CACHE_STOCK).clear();
        client.cache(CACHE_TRADE).clear();
        client.cache(CACHE_REPORT).clear();
        client.cache(CACHE_LIST).clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        client = null;
    }

    /**
     *
     */
    @Test
    public void testUpdate() {
        Map<Integer, Account> accounts = getAccounts(100, 1, 100);

        String text = "UPDATE \"acc\".Account SET depo = depo - ? WHERE depo > 0";

        checkUpdate(client.cache(CACHE_ACCOUNT), accounts, new SqlFieldsQueryEx(text, false).setArgs(10));
    }

    /**
     *
     */
    @Test
    public void testUpdateFastKey() {
        Map<Integer, Account> accounts = getAccounts(100, 1, 100);

        String text = "UPDATE \"acc\".Account SET depo = depo - ? WHERE _key = ?";

        checkUpdate(client.cache(CACHE_ACCOUNT), accounts,
            new SqlFieldsQueryEx(text, false).setArgs(10, 1));
    }

    /**
     *
     */
    @Test
    public void testUpdateLimit() {
        Map<Integer, Account> accounts = getAccounts(100, 1, 100);

        String text = "UPDATE \"acc\".Account SET depo = depo - ? WHERE sn >= ? AND sn < ? LIMIT ?";

        checkUpdate(client.cache(CACHE_ACCOUNT), accounts,
            new SqlFieldsQueryEx(text, false).setArgs(10, 0, 10, 10));
    }

    /**
     *
     */
    @Test
    public void testUpdateWhereSubquery() {
        Map<Integer, Account> accounts = getAccounts(100, 1, -100);

        Map<Integer, Trade> trades = getTrades(100, 2);

        client.cache(CACHE_ACCOUNT).putAll(accounts);

        String text = "UPDATE \"trade\".Trade t SET qty = ? " +
            "WHERE accountId IN (SELECT p._key FROM \"acc\".Account p WHERE depo < ?)";

        checkUpdate(client.cache(CACHE_TRADE), trades,
            new SqlFieldsQueryEx(text, false).setArgs(0, 0));
    }

    /**
     *
     */
    @Test
    public void testUpdateSetSubquery() {
        Map<Integer, Account> accounts = getAccounts(100, 1, 1000);
        Map<Integer, Trade> trades = getTrades(100, 2);

        client.cache(CACHE_ACCOUNT).putAll(accounts);

        String text = "UPDATE \"trade\".Trade t SET qty = " +
            "(SELECT a.depo/t.price FROM \"acc\".Account a WHERE t.accountId = a._key)";

        checkUpdate(client.cache(CACHE_TRADE), trades,
            new SqlFieldsQueryEx(text, false));
    }

    /**
     *
     */
    @Test
    public void testUpdateSetTableSubquery() {
        Map<Integer, Account> accounts = getAccounts(100, 1, 1000);
        Map<Integer, Trade> trades = getTrades(100, 2);

        client.cache(CACHE_ACCOUNT).putAll(accounts);

        String text = "UPDATE \"trade\".Trade t SET (qty) = " +
            "(SELECT a.depo/t.price FROM \"acc\".Account a WHERE t.accountId = a._key)";

        checkUpdate(client.cache(CACHE_TRADE), trades,
            new SqlFieldsQueryEx(text, false));
    }

    /**
     *
     */
    @Test
    public void testInsertValues() {
        String text = "INSERT INTO \"acc\".Account (_key, name, sn, depo)" +
            " VALUES (?, ?, ?, ?), (?, ?, ?, ?)";

        checkUpdate(client.<Integer, Account>cache(CACHE_ACCOUNT), null,
            new SqlFieldsQueryEx(text, false).setArgs(1, "John Marry", 11111, 100, 2, "Marry John", 11112, 200));
    }

    /**
     *
     */
    @Test
    public void testInsertFromSelect() {
        Map<Integer, Account> accounts = getAccounts(100, 1, 1000);

        client.cache(CACHE_ACCOUNT).putAll(accounts);

        String text = "INSERT INTO \"trade\".Trade (_key, accountId, stockId, qty, price) " +
            "SELECT a._key, a._key, ?, a.depo/?, ? FROM \"acc\".Account a";

        checkUpdate(client.<Integer, Trade>cache(CACHE_TRADE), null,
            new SqlFieldsQueryEx(text, false).setArgs(1, 10, 10));
    }

    /**
     *
     */
    @Test
    public void testInsertFromSelectOrderBy() {
        Map<Integer, Account> accounts = getAccounts(100, 1, 1000);

        client.cache(CACHE_ACCOUNT).putAll(accounts);

        String text = "INSERT INTO \"trade\".Trade (_key, accountId, stockId, qty, price) " +
            "SELECT a._key, a._key, ?, a.depo/?, ? FROM \"acc\".Account a " +
            "ORDER BY a.sn DESC";

        checkUpdate(client.<Integer, Trade>cache(CACHE_TRADE), null,
            new SqlFieldsQueryEx(text, false).setArgs(1, 10, 10));
    }

    /**
     *
     */
    @Test
    public void testInsertFromSelectUnion() {
        Map<Integer, Account> accounts = getAccounts(20, 1, 1000);

        client.cache(CACHE_ACCOUNT).putAll(accounts);

        String text = "INSERT INTO \"trade\".Trade (_key, accountId, stockId, qty, price) " +
            "SELECT a._key, a._key, 0, a.depo, 1 FROM \"acc\".Account a " +
            "UNION " +
            "SELECT 101 + a2._key, a2._key, 1, a2.depo, 1 FROM \"acc\".Account a2";

        checkUpdate(client.<Integer, Trade>cache(CACHE_TRADE), null,
            new SqlFieldsQueryEx(text, false));
    }

    /**
     *
     */
    @Test
    public void testInsertFromSelectGroupBy() {
        Map<Integer, Account> accounts = getAccounts(100, 1, 1000);
        Map<Integer, Trade> trades = getTrades(100, 2);

        client.cache(CACHE_ACCOUNT).putAll(accounts);
        client.cache(CACHE_TRADE).putAll(trades);

        String text = "INSERT INTO \"rep\".Report (_key, accountId, spends, count) " +
            "SELECT accountId, accountId, SUM(qty * price), COUNT(*) " +
            "FROM \"trade\".Trade " +
            "GROUP BY accountId";

        checkUpdate(client.<Integer, Report>cache(CACHE_REPORT), null,
            new SqlFieldsQueryEx(text, false));
    }

    /**
     *
     */
    @Test
    public void testInsertFromSelectDistinct() {
        Map<Integer, Account> accounts = getAccounts(100, 2, 100);

        client.cache(CACHE_ACCOUNT).putAll(accounts);

        String text = "INSERT INTO \"list\".String (_key, _val) " +
            "SELECT DISTINCT sn, name FROM \"acc\".Account ";

        checkUpdate(client.<Integer, String>cache(CACHE_LIST), null,
            new SqlFieldsQueryEx(text, false));
    }

    /**
     *
     */
    @Test
    public void testInsertFromSelectJoin() {
        Map<Integer, Account> accounts = getAccounts(100, 1, 100);
        Map<Integer, Stock> stocks = getStocks(5);

        client.cache(CACHE_ACCOUNT).putAll(accounts);
        client.cache(CACHE_STOCK).putAll(stocks);

        String text = "INSERT INTO \"trade\".Trade(_key, accountId, stockId, qty, price) " +
            "SELECT 5*a._key + s._key, a._key, s._key, ?, a.depo/? " +
            "FROM \"acc\".Account a JOIN \"stock\".Stock s ON 1=1";

        checkUpdate(client.<Integer, Trade>cache(CACHE_TRADE), null,
            new SqlFieldsQueryEx(text, false).setArgs(10, 10));
    }

    /**
     *
     */
    @Test
    public void testDelete() {
        Map<Integer, Account> accounts = getAccounts(100, 1, 100);

        client.cache(CACHE_ACCOUNT).putAll(accounts);

        String text = "DELETE FROM \"acc\".Account WHERE sn > ?";

        checkUpdate(client.cache(CACHE_ACCOUNT), accounts,
            new SqlFieldsQueryEx(text, false).setArgs(10));
    }

    /**
     *
     */
    @Test
    public void testDeleteTop() {
        Map<Integer, Account> accounts = getAccounts(100, 1, 100);

        client.cache(CACHE_ACCOUNT).putAll(accounts);

        String text = "DELETE TOP ? FROM \"acc\".Account WHERE sn < ?";

        checkUpdate(client.cache(CACHE_ACCOUNT), accounts,
            new SqlFieldsQueryEx(text, false).setArgs(10, 10));
    }

    /**
     *
     */
    @Test
    public void testDeleteWhereSubquery() {
        Map<Integer, Account> accounts = getAccounts(20, 1, 100);
        Map<Integer, Trade> trades = getTrades(10, 2);

        client.cache(CACHE_ACCOUNT).putAll(accounts);
        client.cache(CACHE_TRADE).putAll(trades);

        String text = "DELETE FROM \"acc\".Account " +
            "WHERE _key IN (SELECT t.accountId FROM \"trade\".Trade t)";

        checkUpdate(client.cache(CACHE_ACCOUNT), accounts,
            new SqlFieldsQueryEx(text, false));
    }

    /**
     *
     */
    @Test
    public void testMergeValues() {
        Map<Integer, Account> accounts = getAccounts(1, 1, 100);

        String text = "MERGE INTO \"acc\".Account (_key, name, sn, depo)" +
            " VALUES (?, ?, ?, ?), (?, ?, ?, ?)";

        checkUpdate(client.cache(CACHE_ACCOUNT), accounts,
            new SqlFieldsQueryEx(text, false).setArgs(0, "John Marry", 11111, 100, 1, "Marry John", 11112, 200));
    }

    /**
     *
     */
    @Test
    public void testMergeFromSelectJoin() {
        Map<Integer, Account> accounts = getAccounts(100, 1, 100);
        Map<Integer, Stock> stocks = getStocks(5);

        client.cache(CACHE_ACCOUNT).putAll(accounts);
        client.cache(CACHE_STOCK).putAll(stocks);

        Map<Integer, Trade> trades = new HashMap<>();

        trades.put(5, new Trade(1, 1, 1, 1));

        String text = "MERGE INTO \"trade\".Trade(_key, accountId, stockId, qty, price) " +
            "SELECT 5*a._key + s._key, a._key, s._key, ?, a.depo/? " +
            "FROM \"acc\".Account a JOIN \"stock\".Stock s ON 1=1";

        checkUpdate(client.cache(CACHE_TRADE), trades,
            new SqlFieldsQueryEx(text, false).setArgs(10, 10));
    }

    /**
     *
     */
    @Test
    public void testMergeFromSelectOrderBy() {
        Map<Integer, Account> accounts = getAccounts(100, 1, 1000);

        client.cache(CACHE_ACCOUNT).putAll(accounts);

        Map<Integer, Trade> trades = new HashMap<>();

        trades.put(5, new Trade(1, 1, 1, 1));

        String text = "MERGE INTO \"trade\".Trade (_key, accountId, stockId, qty, price) " +
            "SELECT a._key, a._key, ?, a.depo/?, ? FROM \"acc\".Account a " +
            "ORDER BY a.sn DESC";

        checkUpdate(client.cache(CACHE_TRADE), trades,
            new SqlFieldsQueryEx(text, false).setArgs(1, 10, 10));
    }

    /**
     *
     */
    @Test
    public void testMergeFromSelectGroupBy() {
        Map<Integer, Account> accounts = getAccounts(100, 1, 1000);
        Map<Integer, Trade> trades = getTrades(100, 2);

        client.cache(CACHE_ACCOUNT).putAll(accounts);
        client.cache(CACHE_TRADE).putAll(trades);

        Map<Integer, Report> reports = new HashMap<>();

        reports.put(5, new Report(5, 1, 1));

        String text = "MERGE INTO \"rep\".Report (_key, accountId, spends, count) " +
            "SELECT accountId, accountId, SUM(qty * price), COUNT(*) " +
            "FROM \"trade\".Trade " +
            "GROUP BY accountId";

        checkUpdate(client.<Integer, Report>cache(CACHE_REPORT), reports,
            new SqlFieldsQueryEx(text, false));
    }

    /**
     * Constructs multiple Account objects.
     *
     * @param num Number of accounts.
     * @param numCopy Number of copies.
     * @param depo Deposit amount.
     * @return Map of accounts.
     */
    private Map<Integer, Account> getAccounts(int num, int numCopy, int depo) {
        Map<Integer, Account> res = new HashMap<>();

        int count = 0;

        for (int i = 0; i < num; ++i) {
            String name = "John doe #" + i;

            for (int j = 0; j < numCopy; ++j)
                res.put(count++, new Account(name, i, depo));
        }

        return res;
    }

    /**
     * Constructs multiple Stock objects.
     *
     * @param num Number of stocks.
     * @return Map of Stock objects.
     */
    private Map<Integer, Stock> getStocks(int num) {
        Map<Integer, Stock> res = new HashMap<>();

        for (int i = 0; i < num; ++i)
            res.put(i, new Stock("T" + i, "Stock #" + i));

        return res;
    }

    /**
     * Constructs multiple Trade objects.
     *
     * @param numAccounts Number of accounts.
     * @param numStocks Number of stocks.
     * @return Map of Trade objects.
     */
    private Map<Integer, Trade> getTrades(int numAccounts, int numStocks) {
        Map<Integer, Trade> res = new HashMap<>();

        int count = 0;

        for (int i = 0; i < numAccounts; ++i) {
            for (int j = 0; j < numStocks; ++j)
                res.put(count++, new Trade(i, j, 100, 100));
        }

        return res;
    }

    /**
     * Executes provided sql update with skipReducerOnUpdate flag on and off and checks results are the same.
     *
     * @param cache Cache.
     * @param initial Initial content of the cache.
     * @param qry Query to execute.
     * @param <K> Key type.
     * @param <V> Value type.
     */
    private <K, V> void checkUpdate(IgniteCache<K, V> cache, Map<K, V> initial, SqlFieldsQueryEx qry) {
        cache.clear();

        if (!F.isEmpty(initial))
            cache.putAll(initial);

        List<List<?>> updRes = cache.query(qry.setSkipReducerOnUpdate(true)).getAll();

        Map<K, V> result = new HashMap<>(cache.size());

        for (Cache.Entry<K, V> e : cache)
            result.put(e.getKey(), e.getValue());

        cache.clear();

        if (!F.isEmpty(initial))
            cache.putAll(initial);

        List<List<?>> updRes2 = cache.query(qry.setSkipReducerOnUpdate(false)).getAll();

        assertTrue(((Number)updRes.get(0).get(0)).intValue() > 0);

        assertEquals(((Number)updRes.get(0).get(0)).intValue(), ((Number)updRes2.get(0).get(0)).intValue());

        assertEquals(result.size(), cache.size());

        for (Cache.Entry<K, V> e : cache)
            assertEquals(e.getValue(), result.get(e.getKey()));
    }

    /** */
    public static class Account {
        /** */
        @QuerySqlField
        String name;

        /** */
        @QuerySqlField
        int sn;

        /** */
        @QuerySqlField
        int depo;

        /**
         * Constructor.
         *
         * @param name Name.
         * @param sn ID.
         * @param depo Deposit amount.
         */
        Account(String name, int sn, int depo) {
            this.name = name;
            this.sn = sn;
            this.depo = depo;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return (name == null ? 0 : name.hashCode()) ^ sn ^ depo;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (obj == null)
                return false;

            if (!obj.getClass().equals(Account.class))
                return false;

            Account other = (Account)obj;

            return F.eq(name, other.name) && sn == other.sn && depo == other.depo;
        }
    }

    /** */
    public static class Stock {
        /** */
        @QuerySqlField
        String ticker;

        /** */
        @QuerySqlField
        String name;

        /**
         * Constructor.
         *
         * @param ticker Short name.
         * @param name Name.
         */
        Stock(String ticker, String name) {
            this.ticker = ticker;
            this.name = name;
        }
    }

    /** */
    public static class Trade {
        /** */
        @QuerySqlField
        int accountId;

        /** */
        @QuerySqlField
        int stockId;

        /** */
        @QuerySqlField
        int qty;

        /** */
        @QuerySqlField
        int price;

        /**
         * Constructor.
         *
         * @param accountId Account id.
         * @param stockId Stock id.
         * @param qty Quantity.
         * @param price Price.
         */
        Trade(int accountId, int stockId, int qty, int price) {
            this.accountId = accountId;
            this.stockId = stockId;
            this.qty = qty;
            this.price = price;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return accountId ^ stockId ^ qty ^ price;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (obj == null)
                return false;

            if (!obj.getClass().equals(Trade.class))
                return false;

            Trade other = (Trade)obj;

            return accountId == other.accountId && stockId == other.stockId &&
                qty == other.qty && price == other.price;
        }

    }

    /** */
    public static class Report {
        /** */
        @QuerySqlField
        int accountId;

        /** */
        @QuerySqlField
        int spends;

        /** */
        @QuerySqlField
        int count;

        /**
         * Constructor.
         *
         * @param accountId Account id.
         * @param spends Spends.
         * @param count Count.
         */
        Report(int accountId, int spends, int count) {
            this.accountId = accountId;
            this.spends = spends;
            this.count = count;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return accountId ^ spends ^ count;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (obj == null)
                return false;

            if (!obj.getClass().equals(Report.class))
                return false;

            Report other = (Report)obj;

            return accountId == other.accountId && spends == other.spends &&
                count == other.count;
        }
    }
}
