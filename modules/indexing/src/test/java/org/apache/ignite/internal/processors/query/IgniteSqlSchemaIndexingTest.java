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

import java.util.List;
import java.util.concurrent.Callable;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests {@link IgniteH2Indexing} support {@link CacheConfiguration#setSqlSchema(String)} configuration.
 */
@SuppressWarnings("unchecked")
@RunWith(JUnit4.class)
public class IgniteSqlSchemaIndexingTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * @param name Cache name.
     * @param partitioned Partition or replicated cache.
     * @param idxTypes Indexed types.
     * @return Cache configuration.
     */
    private static CacheConfiguration cacheConfig(String name, boolean partitioned, Class<?>... idxTypes) {
        return new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setName(name)
            .setCacheMode(partitioned ? CacheMode.PARTITIONED : CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(1)
            .setIndexedTypes(idxTypes);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Tests collision for case insensitive sqlScheme.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCaseSensitive() throws Exception {
        //TODO rewrite with dynamic cache creation, and GRID start in #beforeTest - IGNITE-1094 resolved

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                final CacheConfiguration cfg = cacheConfig("InSensitiveCache", true, Integer.class, Integer.class)
                    .setSqlSchema("InsensitiveCache");

                final CacheConfiguration collisionCfg = cacheConfig("InsensitiveCache", true, Integer.class, Integer.class)
                    .setSqlSchema("Insensitivecache");

                IgniteConfiguration icfg = new IgniteConfiguration()
                    .setLocalHost("127.0.0.1")
                    .setCacheConfiguration(cfg, collisionCfg);

                Ignition.start(icfg);

                return null;
            }
        }, IgniteException.class, "Duplicate index name");
    }

    /**
     * Test collision of table names in different caches, sharing a single SQL schema.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCustomSchemaMultipleCachesTablesCollision() throws Exception {
        //TODO: Rewrite with dynamic cache creation, and GRID start in #beforeTest - IGNITE-1094 resolved

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                final CacheConfiguration cfg = cacheConfig("cache1", true, Integer.class, Fact.class)
                    .setSqlSchema("TEST_SCHEMA");

                final CacheConfiguration collisionCfg = cacheConfig("cache2", true, Integer.class, Fact.class)
                    .setSqlSchema("TEST_SCHEMA");

                IgniteConfiguration icfg = new IgniteConfiguration()
                    .setLocalHost("127.0.0.1")
                    .setCacheConfiguration(cfg, collisionCfg);

                Ignition.start(icfg);

                return null;
            }
        }, IgniteException.class, "Failed to register query type");
    }

    /**
     * Tests unregistration of previous scheme.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheUnregistration() throws Exception {
        startGridsMultiThreaded(3, true);

        final CacheConfiguration<Integer, Fact> cfg = cacheConfig("Insensitive_Cache", true, Integer.class, Fact.class)
            .setSqlSchema("Insensitive_Cache");
        final CacheConfiguration<Integer, Fact> collisionCfg = cacheConfig("InsensitiveCache", true, Integer.class, Fact.class)
            .setSqlSchema("Insensitive_Cache");

        IgniteCache<Integer, Fact> cache = ignite(0).createCache(cfg);

        SqlFieldsQuery qry = new SqlFieldsQuery("select f.id, f.name from InSENSitive_Cache.Fact f");

        cache.put(1, new Fact(1, "cacheInsensitive"));

        for (List<?> row : cache.query(qry)) {
            assertEquals(2, row.size());
            assertEquals(1, row.get(0));
            assertEquals("cacheInsensitive", row.get(1));
        }

        ignite(0).destroyCache(cache.getName());

        cache = ignite(0).createCache(collisionCfg); // Previous collision should be removed by now.

        cache.put(1, new Fact(1, "cacheInsensitive"));
        cache.put(2, new Fact(2, "ThisIsANewCache"));
        cache.put(3, new Fact(3, "With3RecordsAndAnotherName"));

        assertEquals(3, cache.query(qry).getAll().size());

        ignite(0).destroyCache(cache.getName());
    }

    /**
     * Tests escapeAll and sqlSchema apposition.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSchemaEscapeAll() throws Exception {
        startGridsMultiThreaded(3, true);

        final CacheConfiguration<Integer, Fact> cfg = cacheConfig("simpleSchema", true, Integer.class, Fact.class)
            .setSqlSchema("SchemaName1")
            .setSqlEscapeAll(true);

        final CacheConfiguration<Integer, Fact> cfgEsc = cacheConfig("escapedSchema", true, Integer.class, Fact.class)
            .setSqlSchema("\"SchemaName2\"")
            .setSqlEscapeAll(true);

        escapeCheckSchemaName(ignite(0).createCache(cfg), log, cfg.getSqlSchema(), false,
            "Table \"FACT\" not found");

        escapeCheckSchemaName(ignite(0).createCache(cfgEsc), log, "SchemaName2", true,
            "Schema \"SCHEMANAME2\" not found");

        ignite(0).destroyCache(cfg.getName());
        ignite(0).destroyCache(cfgEsc.getName());
    }

    /**
     * Executes query with and without escaped schema name.
     * @param cache cache for querying
     * @param log logger for assertThrows
     * @param schemaName Schema name without quotes for testing
     * @param caseSensitive Whether schema name is case sensitive.
     * @param msg Expected error message.
     */
    private static void escapeCheckSchemaName(final IgniteCache<Integer, Fact> cache, IgniteLogger log,
        String schemaName, boolean caseSensitive, String msg) {
        final SqlFieldsQuery qryWrong = new SqlFieldsQuery("select f.id, f.name " +
            "from " + schemaName.toUpperCase() + ".Fact f");

        cache.put(1, new Fact(1, "cacheInsensitive"));

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                cache.query(qryWrong);

                return null;
            }
        }, CacheException.class, msg);

        if (caseSensitive)
            schemaName = "\"" + schemaName + "\"";

        SqlFieldsQuery qryCorrect = new SqlFieldsQuery("select f.\"id\", f.\"name\" " +
            "from "+  schemaName + ".\"Fact\" f");

        for ( List<?> row : cache.query(qryCorrect)) {
            assertEquals(2, row.size());
            assertEquals(1, row.get(0));
            assertEquals("cacheInsensitive", row.get(1));
        }
    }

    // TODO add tests with dynamic cache unregistration - IGNITE-1094 resolved

    /** Test class as query entity */
    private static class Fact {
        /** Primary key. */
        @QuerySqlField
        private int id;

        @QuerySqlField
        private String name;

        /**
         * Constructs a fact.
         *
         * @param id fact ID.
         * @param name fact name.
         */
        Fact(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /**
         * Gets fact ID.
         *
         * @return fact ID.
         */
        public int getId() {
            return id;
        }

        /**
         * Gets fact name.
         *
         * @return Fact name.
         */
        public String getName() {
            return name;
        }
    }
}
