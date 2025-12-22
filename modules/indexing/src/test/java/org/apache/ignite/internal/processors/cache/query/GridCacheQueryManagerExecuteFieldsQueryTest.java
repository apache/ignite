package org.apache.ignite.internal.processors.cache.query;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import javax.cache.Cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Reproducer: verify that GridCacheQueryManager.executeFieldsQuery(...) is never called
 * for local SQL fields queries (and parallel local SQL fields queries).
 */
public class GridCacheQueryManagerExecuteFieldsQueryTest extends GridCommonAbstractTest {
    private static final String CACHE_NAME = "testCache";

    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>(CACHE_NAME);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        ccfg.setBackups(0);

        ccfg.setIndexedTypes(Integer.class, Person.class);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    @Override
    protected void afterTest() throws Exception {
        stopAllGrids();
        super.afterTest();
    }

    @Test
    public void testLocalSqlFieldsQueryDoesNotHitExecuteFieldsQuery() throws Exception {
        Ignite ignite = startGrid(0);

        IgniteCache<Integer, Person> cache = ignite.cache(CACHE_NAME);
        assertNotNull(cache);

        cache.put(1, new Person("Alice", 10));
        cache.put(2, new Person("Bob", 20));

        GridCacheQueryManager.resetExecuteFieldsQueryHitCount();

        SqlFieldsQuery qry = new SqlFieldsQuery(
                "select _key, name, age from Person order by _key"
        );
        qry.setLocal(true);

        List<List<?>> rows;
        try (QueryCursor<List<?>> cur = cache.query(qry)) {
            rows = cur.getAll();
        }

        assertEquals(2, rows.size());
        assertEquals(0, GridCacheQueryManager.executeFieldsQueryHitCount());
    }

    @Test
    public void testTwoParallelLocalSqlFieldsQueriesDoNotHitExecuteFieldsQuery() throws Exception {
        Ignite ignite = startGrid(0);

        IgniteCache<Integer, Person> cache = ignite.cache(CACHE_NAME);
        assertNotNull(cache);

        cache.put(1, new Person("Kirill", 10));
        cache.put(2, new Person("Michailo", 20));

        GridCacheQueryManager.resetExecuteFieldsQueryHitCount();

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Callable<List<List<?>>> task = () -> {
                SqlFieldsQuery qry = new SqlFieldsQuery("select _key, name from Person order by _key");
                qry.setLocal(true);

                try (QueryCursor<List<?>> cur = cache.query(qry)) {
                    return (List<List<?>>) (List<?>) cur.getAll();
                }
            };

            Future<List<List<?>>> f1 = pool.submit(task);
            Future<List<List<?>>> f2 = pool.submit(task);

            List<List<?>> r1 = f1.get();
            List<List<?>> r2 = f2.get();

            assertEquals(2, r1.size());
            assertEquals(2, r2.size());

            assertEquals(0, GridCacheQueryManager.executeFieldsQueryHitCount());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void testNonLocalSqlFieldsQueryHitsExecuteFieldsQuery() throws Exception {
        startGrids(2);

        Ignite ignite0 = grid(0);
        Ignite ignite1 = grid(1);

        IgniteCache<Integer, Person> cache0 = ignite0.cache(CACHE_NAME);

        awaitPartitionMapExchange();

        int kOn1 = 0;
        while (!ignite1.affinity(CACHE_NAME).isPrimary(ignite1.cluster().localNode(), kOn1))
            kOn1++;

        int kOn0 = 0;
        while (!ignite0.affinity(CACHE_NAME).isPrimary(ignite0.cluster().localNode(), kOn0))
            kOn0++;

        cache0.put(kOn0, new Person("On0", 10));
        cache0.put(kOn1, new Person("On1", 20));

        GridCacheQueryManager.resetExecuteFieldsQueryHitCount();

        SqlFieldsQuery qry = new SqlFieldsQuery("select _key, name from Person order by _key");
        qry.setLocal(false);

        try (QueryCursor<List<?>> cur = cache0.query(qry)) {
            List<List<?>> rows = cur.getAll();
            assertEquals(2, rows.size());
        }
        assertTrue(ignite1.affinity(CACHE_NAME).isPrimary(ignite1.cluster().localNode(), kOn1));
        assertTrue(ignite0.affinity(CACHE_NAME).isPrimary(ignite0.cluster().localNode(), kOn0));

        assertFalse(ignite0.affinity(CACHE_NAME).isPrimary(ignite0.cluster().localNode(), kOn1));
        assertFalse(ignite1.affinity(CACHE_NAME).isPrimary(ignite1.cluster().localNode(), kOn0));

        assertEquals(0, GridCacheQueryManager.executeFieldsQueryHitCount());
    }

    @Test
    public void testLocalSqlQueryDoesNotHitExecuteFieldsQuery() throws Exception {
        startGrids(2);

        Ignite ignite0 = grid(0);
        Ignite ignite1 = grid(1);

        awaitPartitionMapExchange();

        IgniteCache<Integer, Person> cache0 = ignite0.cache(CACHE_NAME);

        int[] keys = pickKeysForDifferentPrimaries(ignite0, ignite1);
        int k0 = keys[0];
        int k1 = keys[1];

        cache0.put(k0, new Person("On0", 10));
        cache0.put(k1, new Person("On1", 20));

        GridCacheQueryManager.resetExecuteFieldsQueryHitCount();

        SqlQuery<Integer, Person> qry = new SqlQuery<>(Person.class, "age >= ?");
        qry.setArgs(0);
        qry.setLocal(true);

        List<Cache.Entry<Integer, Person>> res0;
        try (QueryCursor<Cache.Entry<Integer, Person>> cur = ignite0.cache(CACHE_NAME).query(qry)) {
            res0 = cur.getAll();
        }

        List<Cache.Entry<Integer, Person>> res1;
        try (QueryCursor<Cache.Entry<Integer, Person>> cur = ignite1.cache(CACHE_NAME).query(qry)) {
            res1 = cur.getAll();
        }

        assertEquals(1, res0.size());
        assertEquals(1, res1.size());

        assertEquals(k0, res0.get(0).getKey().intValue());
        assertEquals(k1, res1.get(0).getKey().intValue());

        assertEquals(0, GridCacheQueryManager.executeFieldsQueryHitCount());
    }

    /**
     * The same intent, but also verify we got exactly the keys we inserted.
     */
    @Test
    public void testDistributedSqlQueryDoesNotHitExecuteFieldsQuery() throws Exception {
        Ignite ignite0 = startGrid(0);
        Ignite ignite1 = startGrid(1);

        awaitPartitionMapExchange();

        IgniteCache<Integer, Person> cache0 = ignite0.cache(CACHE_NAME);

        int[] keys = pickKeysForDifferentPrimaries(ignite0, ignite1);
        int k0 = keys[0];
        int k1 = keys[1];

        cache0.put(k0, new Person("Kirill", 10));
        cache0.put(k1, new Person("Michailo", 20));

        GridCacheQueryManager.resetExecuteFieldsQueryHitCount();

        SqlQuery<Integer, Person> qry = new SqlQuery<>(Person.class, "age >= ?");
        qry.setArgs(0);
        qry.setLocal(false);

        List<Cache.Entry<Integer, Person>> res;
        try (QueryCursor<Cache.Entry<Integer, Person>> cur = cache0.query(qry)) {
            res = cur.getAll();
        }

        assertEquals(2, res.size());

        Set<Integer> gotKeys = res.stream().map(Cache.Entry::getKey).collect(Collectors.toSet());

        assertTrue("Expected to get both keys back [k0 = " + k0 + ", k1 = " + k1 + ", got = " + gotKeys + ']',
                gotKeys.contains(k0) && gotKeys.contains(k1));

        assertEquals(0, GridCacheQueryManager.executeFieldsQueryHitCount());
    }

    private int[] pickKeysForDifferentPrimaries(Ignite ignite0, Ignite ignite1) {
        int k0 = -1, k1 = -1;

        UUID id0 = ignite0.cluster().localNode().id();
        UUID id1 = ignite1.cluster().localNode().id();

        for (int k = 1; k < 100000; k++) {
            UUID primaryId = ignite0.affinity(CACHE_NAME).mapKeyToNode(k).id();

            if (primaryId.equals(id0) && k0 < 0) k0 = k;
            if (primaryId.equals(id1) && k1 < 0) k1 = k;

            if (k0 > 0 && k1 > 0)
                break;
        }

        assertTrue("Failed to pick keys for different primaries [k0 = " + k0 + ", k1 = " + k1 + ']',
                k0 > 0 && k1 > 0);

        return new int[]{k0, k1};
    }

    /**
     * Simple SQL-mapped object.
     */
    private static class Person implements Serializable {
        @QuerySqlField(index = true)
        private String name;

        @QuerySqlField(index = true)
        private int age;

        Person(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }
}
