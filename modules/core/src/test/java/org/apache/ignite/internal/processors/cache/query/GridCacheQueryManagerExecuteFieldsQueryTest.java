package org.apache.ignite.internal.processors.cache.query;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
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
