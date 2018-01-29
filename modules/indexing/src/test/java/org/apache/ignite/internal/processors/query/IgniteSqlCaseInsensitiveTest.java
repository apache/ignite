package org.apache.ignite.internal.processors.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

public class IgniteSqlCaseInsensitiveTest extends GridCommonAbstractTest {

    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Name of client node. */
    private static String NODE_CLIENT = "client";

    /** Number of server nodes. */
    private static int NODE_COUNT = 2;

    /** Cache prefix. */
    private static String CACHE_PREFIX = "person1";

    /** Template of cache with read-through setting. */
    private static String CACHE_READ_THROUGH = "cacheReadThrough";

    /** Template of cache with interceptor setting. */
    private static String CACHE_INTERCEPTOR = "cacheInterceptor";

    /** Name of the node which configuration includes restricted cache config. */
    private static String READ_THROUGH_CFG_NODE_NAME = "nodeCacheReadThrough";

    /** Name of the node which configuration includes restricted cache config. */
    private static String INTERCEPTOR_CFG_NODE_NAME = "nodeCacheInterceptor";

    /** */
    private final int key1 = 1;

    /** */
    private final Person person1 = new Person("John", "Doe", 25);

    /** */
    private final Person person2 = new Person("Jane", "doe", 22);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);
        disco.setForceServerMode(true);

        c.setDiscoverySpi(disco);

        List<CacheConfiguration> ccfgs = new ArrayList<>(cacheConfigurations());

        if (gridName.equals(READ_THROUGH_CFG_NODE_NAME)) {
            ccfgs.add(buildCacheConfigurationRestricted("BadCfgTestCacheRT", true, false, true));

            c.setClientMode(true);
        }

        if (gridName.equals(INTERCEPTOR_CFG_NODE_NAME)) {
            ccfgs.add(buildCacheConfigurationRestricted("BadCfgTestCacheINT", false, true, true));

            c.setClientMode(true);
        }

        c.setCacheConfiguration(ccfgs.toArray(new CacheConfiguration[ccfgs.size()]));

        if (gridName.equals(NODE_CLIENT)) {
            c.setClientMode(true);

            // Not allowed to have local cache on client without memory config
            c.setDataStorageConfiguration(new DataStorageConfiguration());
        }

        return c;
    }

    /** */
    private List<CacheConfiguration> cacheConfigurations() {
        List<CacheConfiguration> res = new ArrayList<>();

        for (boolean wrt : new boolean[] {false, true}) {
            for (boolean annot : new boolean[] {false, true}) {
                res.add(buildCacheConfiguration(CacheMode.LOCAL, CacheAtomicityMode.ATOMIC, false, wrt, annot));
                res.add(buildCacheConfiguration(CacheMode.LOCAL, CacheAtomicityMode.TRANSACTIONAL, false, wrt, annot));

                res.add(buildCacheConfiguration(CacheMode.REPLICATED, CacheAtomicityMode.ATOMIC, false, wrt, annot));
                res.add(buildCacheConfiguration(CacheMode.REPLICATED, CacheAtomicityMode.TRANSACTIONAL, false, wrt, annot));

                res.add(buildCacheConfiguration(CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC, false, wrt, annot));
                res.add(buildCacheConfiguration(CacheMode.PARTITIONED, CacheAtomicityMode.ATOMIC, true, wrt, annot));
                res.add(buildCacheConfiguration(CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL, false, wrt, annot));
                res.add(buildCacheConfiguration(CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL, true, wrt, annot));
            }
        }

        return res;
    }

    /** */
    private CacheConfiguration buildCacheConfiguration(CacheMode mode,
        CacheAtomicityMode atomicityMode, boolean hasNear, boolean writeThrough, boolean isCaseInsensitive) {

        CacheConfiguration cfg = new CacheConfiguration(CACHE_PREFIX + "-" +
            mode.name() + "-" + atomicityMode.name() + (hasNear ? "-near" : "") +
            (writeThrough ? "-writethrough" : "") + (isCaseInsensitive ? "-annot" : ""));

        cfg.setCacheMode(mode);
        cfg.setAtomicityMode(atomicityMode);
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        QueryEntity qe = new QueryEntity(new QueryEntity(Integer.class, Person.class));

        if (!isCaseInsensitive)
            qe.setCaseInsensitiveFields(Collections.singleton("lastName"));

        cfg.setQueryEntities(F.asList(qe));

        if (hasNear)
            cfg.setNearConfiguration(new NearCacheConfiguration().setNearStartSize(100));

        if (writeThrough) {
            cfg.setCacheStoreFactory(singletonFactory(new TestStore()));
            cfg.setWriteThrough(true);
        }

        return cfg;
    }

    /** */
    private CacheConfiguration buildCacheConfigurationRestricted(String cacheName, boolean readThrough,
        boolean interceptor, boolean hasQueryEntity) {
        CacheConfiguration cfg = new CacheConfiguration<Integer, Person>()
            .setName(cacheName)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        if (readThrough) {
            cfg.setCacheStoreFactory(singletonFactory(new TestStore()));
            cfg.setReadThrough(true);
        }

        if (interceptor)
            cfg.setInterceptor(new TestInterceptor());

        if (hasQueryEntity) {
            cfg.setQueryEntities(F.asList(new QueryEntity(Integer.class, Person.class)
                .setCaseInsensitiveFields(Collections.singleton("lastName"))));
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODE_COUNT);

        startGrid(NODE_CLIENT);

        // Add cache template with read-through cache store.
        grid(NODE_CLIENT).addCacheConfiguration(
            buildCacheConfigurationRestricted(CACHE_READ_THROUGH, true, false, false));

        // Add cache template with cache interceptor.
        grid(NODE_CLIENT).addCacheConfiguration(
            buildCacheConfigurationRestricted(CACHE_INTERCEPTOR, false, true, false));

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanup();
    }


    /** */
    public void testQueryEntityGetSetCaseInsensitiveFields() throws Exception {
        QueryEntity qe = new QueryEntity();

        assertNull(qe.getCaseInsensitiveFields());

        Set<String> val = Collections.singleton("test");

        qe.setCaseInsensitiveFields(val);

        assertEquals(val, Collections.singleton("test"));

        qe.setCaseInsensitiveFields(null);

        assertNull(qe.getCaseInsensitiveFields());
    }

    /** */
    public void testQueryEntityEquals() throws Exception {
        QueryEntity a = new QueryEntity();

        QueryEntity b = new QueryEntity();

        assertEquals(a, b);

        a.setCaseInsensitiveFields(Collections.singleton("test"));

        assertFalse(a.equals(b));

        b.setCaseInsensitiveFields(Collections.singleton("test"));

        assertTrue(a.equals(b));
    }

    /** */
    public void testAtomicOrImplicitTxPut() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                cache.put(IgniteSqlCaseInsensitiveTest.this.key1, person1);
                assertEquals(1, getDoe(cache).size());
            }
        });
    }

    /** */
    public void testAtomicOrImplicitTxPutIfAbsent() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                cache.putIfAbsent(IgniteSqlCaseInsensitiveTest.this.key1, person1);
                assertEquals(1, getDoe(cache).size());
            }
        });
    }

    /** */
    public void testAtomicOrImplicitTxGetAndPut() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                cache.getAndPut(IgniteSqlCaseInsensitiveTest.this.key1, person1);
                assertEquals(1, getDoe(cache).size());
            }
        });
    }

    /** */
    public void testAtomicOrImplicitTxGetAndPutIfAbsent() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                cache.getAndPutIfAbsent(IgniteSqlCaseInsensitiveTest.this.key1, person1);
                assertEquals(1, getDoe(cache).size());
            }
        });
    }

    /** */
    public void testAtomicOrImplicitTxReplace() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                cache.put(IgniteSqlCaseInsensitiveTest.this.key1, person1);
                assertEquals(1, getDoe(cache).size());
            }
        });
    }

    /** */
    public void testAtomicOrImplicitTxGetAndReplace() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                cache.put(IgniteSqlCaseInsensitiveTest.this.key1, person1);
                assertEquals(1, getDoe(cache).size());
            }
        });
    }

    /** */
    public void testAtomicOrImplicitTxPutAll() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                cache.putAll(F.asMap(key1, person1, key2, person2));
                assertEquals(2, getDoe(cache).size());
            }
        });
    }

    /** */
    public void testAtomicOrImplicitTxInvoke() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                cache.invoke(key1, new TestEntryProcessor(person1));
                assertEquals(1, getDoe(cache).size());
            }
        });
    }

    /** */
    public void testAtomicOrImplicitTxInvokeAll() throws Exception {
        executeWithAllCaches(new TestClosure() {
            @Override public void run() throws Exception {
                final Map<Integer, EntryProcessorResult<Object>> r = cache.invokeAll(F.asMap(
                    key1, new TestEntryProcessor(person1),
                    key2, new TestEntryProcessor(person2)));

                List<List<?>> doe = getDoe(cache);
                assertEquals(2, doe.size());
            }
        });
    }

    /** */
    public void testTxPutCreate() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                    cache.put(key1, person1);
                    cache.put(key2, person2);

                    tx.commit();
                }
                assertEquals(2, getDoe(cache).size());
            }
        });
    }

    /** */
    public void testTxPutUpdate() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                    cache.put(key1, person1);
                    cache.put(key2, person1);
                    cache.put(key2, person2);

                    tx.commit();
                }
                assertEquals(2, getDoe(cache).size());
            }
        });
    }

    /** */
    public void testTxPutIfAbsent() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                    cache.putIfAbsent(key1, person1);

                    tx.commit();
                }
                assertEquals(1, getDoe(cache).size());
            }
        });
    }

    /** */
    public void testTxGetAndPut() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                    cache.getAndPut(key1, person1);

                    tx.commit();
                }
                assertEquals(1, getDoe(cache).size());
            }
        });
    }

    /** */
    public void testTxGetAndPutIfAbsent() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                    cache.getAndPutIfAbsent(key1, person1);

                    tx.commit();
                }
                assertEquals(1, getDoe(cache).size());
            }
        });
    }

    /** */
    public void testTxReplace() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                cache.put(1, person1);

                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                    cache.replace(key1, person2);

                    tx.commit();
                }

                assertEquals(1, getDoe(cache).size());
            }
        });
    }

    /** */
    public void testTxGetAndReplace() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                cache.put(key1, person1);

                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                    cache.getAndReplace(key1, person2);

                    tx.commit();
                }
                assertEquals(1, getDoe(cache).size());
            }
        });
    }

    /** */
    public void testTxPutAll() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                    cache.putAll(F.asMap(key1, person1, key2, person2));

                    tx.commit();
                }
                assertEquals(2, getDoe(cache).size());
            }
        });
    }

    /** */
    public void testTxInvoke() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {
                cache.put(key1, person1);
                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                    cache.invoke(key1, new TestEntryProcessor(person2));

                    tx.commit();
                }
                assertEquals(1, getDoe(cache).size());
            }
        });
    }

    /** */
    public void testTxInvokeAll() throws Exception {
        executeWithAllTxCaches(new TestClosure() {
            @Override public void run() throws Exception {

                try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                    final Map<Integer, EntryProcessorResult<Object>> r = cache.invokeAll(F.asMap(
                        key1, new TestEntryProcessor(person1),
                        key2, new TestEntryProcessor(person2)));

                    tx.rollback();
                }

                assertEquals(0, getDoe(cache).size());
            }
        });
    }

    /** */
    public void testDynamicTableCreate() throws Exception {
        executeSql("CREATE TABLE test(id INT PRIMARY KEY, field VARCHAR_IGNORECASE)");

        String cacheName = QueryUtils.createTableCacheName("PUBLIC", "TEST");

        IgniteEx client = grid(NODE_CLIENT);

        CacheConfiguration ccfg = client.context().cache().cache(cacheName).configuration();

        QueryEntity qe = (QueryEntity)F.first(ccfg.getQueryEntities());

        assertEquals(Collections.singleton("FIELD"), qe.getCaseInsensitiveFields());

        checkState("PUBLIC", "TEST", "FIELD");
    }

    /** */
    public void testAtomicCaseInsensitiveDmlInsertValues() throws Exception {
        checkCaseInsensitiveDmlInsertValues(CacheAtomicityMode.ATOMIC);
    }

    /** */
    public void testTransactionalCaseInsensitiveDmlInsertValues() throws Exception {
        checkCaseInsensitiveDmlInsertValues(CacheAtomicityMode.TRANSACTIONAL);
    }

    /** */
    private void checkCaseInsensitiveDmlInsertValues(CacheAtomicityMode atomicityMode) throws Exception {
        executeSql("CREATE TABLE test(id INT PRIMARY KEY, firstName VARCHAR, lastName VARCHAR_IGNORECASE) WITH \"atomicity="
            + atomicityMode.name() + "\"");

        executeSql("INSERT INTO test(id, firstName, lastName) VALUES (1, 'john', 'doe'), (2, 'John', 'Doe'), (3, 'Jane', 'Poe')");

        List<List<?>> result = executeSql("SELECT id, lastName FROM test ORDER BY id");
        assertEquals(3, result.size());

        result = executeSql("SELECT id, lastName FROM test where lastName = ? ORDER BY id", "doe");
        assertEquals(2, result.size());

        result = executeSql("SELECT id, lastName FROM test where lastName = ? ORDER BY id", "Doe");
        assertEquals(2, result.size());

        result = executeSql("SELECT id, lastName FROM test where firstName = ? ORDER BY id", "john");
        assertEquals(1, result.size());

        result = executeSql("SELECT id, lastName FROM test where firstName = ? and lastName = ? ORDER BY id", "john", "doe");
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get(0));
    }

    /** */
    public void testCaseInsensitiveDmlUpdateValues() throws Exception {
        executeSql("CREATE TABLE test(id INT PRIMARY KEY, firstName VARCHAR, lastName VARCHAR_IGNORECASE)");

        executeSql("INSERT INTO test(id, firstName, lastName) VALUES (1, 'john', 'doe'), (2, 'jane', 'roe')");

        List<List<?>> result = executeSql("SELECT id, firstName FROM test where lastName = ?", "doe");

        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get(0));
        assertEquals("john", result.get(0).get(1));

        executeSql("UPDATE test SET lastName = ? WHERE id = ?", "Doe", 2);

        result = executeSql("SELECT id, lastName FROM test where lastName = ?", "doe");

        assertEquals(2, result.size());
    }

    /** */
    public void testAlterTableAddColumn() throws Exception {
        executeSql("CREATE TABLE test(id INT PRIMARY KEY, age INT)");

        executeSql("ALTER TABLE test ADD COLUMN name VARCHAR_IGNORECASE");

        checkState("PUBLIC", "TEST", "NAME");
    }

    /** */
    public void testAtomicAddColumnCaseInsensitiveDmlInsertValues() throws Exception {
        checkAddColumnCaseInsensitiveDmlInsertValues(CacheAtomicityMode.ATOMIC);
    }

    /** */
    public void testTransactionalAddColumnCaseInsensitiveDmlInsertValues() throws Exception {
        checkAddColumnCaseInsensitiveDmlInsertValues(CacheAtomicityMode.TRANSACTIONAL);
    }

    /** */
    private void checkAddColumnCaseInsensitiveDmlInsertValues(CacheAtomicityMode atomicityMode) throws Exception {
        executeSql("CREATE TABLE test(id INT PRIMARY KEY, age INT) WITH \"atomicity="
            + atomicityMode.name() + "\"");

        executeSql("ALTER TABLE test ADD COLUMN name VARCHAR_IGNORECASE");

        executeSql("INSERT INTO test(id, name) VALUES (1, 'ok'), (2, 'OK')");

        List<List<?>> result = executeSql("SELECT id, name FROM test where name = ?", "ok");

        assertEquals(2, result.size());
    }

    /** */
    private List<List<?>> executeSql(String sqlText, Object... args) throws Exception {
        GridQueryProcessor qryProc = grid(NODE_CLIENT).context().query();

        SqlFieldsQuery qry = new SqlFieldsQuery(sqlText);
        if(!F.isEmpty(args))
            qry = qry.setArgs(args);
        return qryProc.querySqlFieldsNoCache(qry, true).getAll();
    }

    /** */
    private List<List<?>> getDoe(IgniteCache cache) throws Exception {
        return cache.query(new SqlFieldsQuery("select * from Person where lastName = ?").setArgs("doe")).getAll();
    }

    /** */
    private void cleanup() throws Exception {
        for (CacheConfiguration ccfg : cacheConfigurations()) {
            String cacheName = ccfg.getName();

            if (ccfg.getCacheMode() == CacheMode.LOCAL) {
                grid(NODE_CLIENT).cache(cacheName).clear();

                for (int node = 0; node < NODE_COUNT; node++)
                    grid(node).cache(cacheName).clear();
            }
            else {
                if (ccfg.getCacheMode() == CacheMode.PARTITIONED && ccfg.getNearConfiguration() != null) {
                    IgniteCache cache = grid(NODE_CLIENT).getOrCreateNearCache(cacheName, ccfg.getNearConfiguration());

                    cache.clear();
                }

                grid(NODE_CLIENT).cache(cacheName).clear();
            }
        }

        executeSql("DROP TABLE test IF EXISTS");
    }

    /** */
    private void checkState(String schemaName, String tableName, String fieldName) {
        IgniteEx client = grid(NODE_CLIENT);

        checkNodeState(client, schemaName, tableName, fieldName);

        for (int i = 0; i < NODE_COUNT; i++)
            checkNodeState(grid(i), schemaName, tableName, fieldName);
    }

    /** */
    private void checkNodeState(IgniteEx node, String schemaName, String tableName, String fieldName) {
        String cacheName = F.eq(schemaName, QueryUtils.DFLT_SCHEMA) ?
            QueryUtils.createTableCacheName(schemaName, tableName) : schemaName;

        DynamicCacheDescriptor desc = node.context().cache().cacheDescriptor(cacheName);

        assertNotNull("Cache descriptor not found", desc);

        QuerySchema schema = desc.schema();

        assertNotNull(schema);

        QueryEntity entity = null;

        for (QueryEntity e : schema.entities()) {
            if (F.eq(tableName, e.getTableName())) {
                entity = e;

                break;
            }
        }

        assertNotNull(entity);

        assertNotNull(entity.getCaseInsensitiveFields());

        assertTrue(entity.getCaseInsensitiveFields().contains(fieldName));
    }

    /** */
    private void executeWithAllCaches(TestClosure clo) throws Exception {
        for (CacheConfiguration ccfg : cacheConfigurations())
            executeForCache(ccfg, clo, TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);
    }

    /** */
    private void executeWithAllTxCaches(final TestClosure clo) throws Exception {
        for (CacheConfiguration ccfg : cacheConfigurations()) {
            if (ccfg.getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL) {
                for (TransactionConcurrency con : TransactionConcurrency.values()) {
                    for (TransactionIsolation iso : TransactionIsolation.values())
                        executeForCache(ccfg, clo, con, iso);
                }
            }
        }
    }

    /** */
    private void executeForCache(CacheConfiguration ccfg, TestClosure clo, TransactionConcurrency concurrency,
        TransactionIsolation isolation) throws Exception {

        Ignite ignite = grid(NODE_CLIENT);
        executeForNodeAndCache(ccfg, ignite, clo, concurrency, isolation);

        for (int node = 0; node < NODE_COUNT; node++) {
            ignite = grid(node);

            executeForNodeAndCache(ccfg, ignite, clo, concurrency, isolation);
        }
    }

    /** */
    private void executeForNodeAndCache(CacheConfiguration ccfg, Ignite ignite, TestClosure clo,
        TransactionConcurrency concurrency, TransactionIsolation isolation) throws Exception {
        String cacheName = ccfg.getName();

        IgniteCache cache;

        if (ignite.configuration().isClientMode() &&
            ccfg.getCacheMode() == CacheMode.PARTITIONED &&
            ccfg.getNearConfiguration() != null)
            cache = ignite.getOrCreateNearCache(ccfg.getName(), ccfg.getNearConfiguration());
        else
            cache = ignite.cache(ccfg.getName());

        cache.removeAll();

        assertEquals(0, cache.size());

        clo.configure(ignite, cache, concurrency, isolation);

        log.info("Running test with node " + ignite.name() + ", cache " + cacheName);

        clo.key1 = 1;
        clo.key2 = 4;

        clo.run();
    }

    /**
     * Test cache store stub.
     */
    private static class TestStore extends CacheStoreAdapter<Integer, Person> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, Person> clo, @Nullable Object... args) {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public Person load(Integer key) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(javax.cache.Cache.Entry<? extends Integer, ? extends Person> e) {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op
        }
    }

    /** */
    public static class Person {
        /** */
        @QuerySqlField
        private String firstName;
        /** */
        @QuerySqlField(caseInsensitive = true)
        private String lastName;

        /** */
        @QuerySqlField
        private int age;

        public Person(String firstName, String lastName, int age) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.age = age;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Person person = (Person)o;
            return age == person.age &&
                Objects.equals(firstName, person.firstName) &&
                Objects.equals(lastName, person.lastName);
        }

        @Override public int hashCode() {
            return Objects.hash(firstName, lastName, age);
        }
    }

    /**
     * Test interceptor stub.
     */
    private static class TestInterceptor implements CacheInterceptor<Integer, Person> {
        /** {@inheritDoc} */
        @Nullable @Override public Person onGet(Integer key, @Nullable Person val) {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Person onBeforePut(Cache.Entry<Integer, Person> entry, Person newVal) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Cache.Entry<Integer, Person> entry) {
            // No-op
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteBiTuple<Boolean, Person> onBeforeRemove(Cache.Entry<Integer, Person> entry) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Cache.Entry<Integer, Person> entry) {
            // No-op
        }
    }

    /** */
    public abstract class TestClosure {
        /** */
        protected Ignite ignite;

        /** */
        protected IgniteCache<Integer, Person> cache;

        /** */
        protected TransactionConcurrency concurrency;

        /** */
        protected TransactionIsolation isolation;

        /** */
        public int key1;

        /** */
        public int key2;

        /** */
        public void configure(Ignite ignite, IgniteCache<Integer, Person> cache, TransactionConcurrency concurrency,
            TransactionIsolation isolation) {
            this.ignite = ignite;
            this.cache = cache;
            this.concurrency = concurrency;
            this.isolation = isolation;
        }

        /** */
        protected boolean isLocalAtomic() {
            CacheConfiguration cfg = cache.getConfiguration(CacheConfiguration.class);

            return cfg.getCacheMode() == CacheMode.LOCAL && cfg.getAtomicityMode() == CacheAtomicityMode.ATOMIC;
        }

        /** */
        public abstract void run() throws Exception;
    }

    /** */
    public class TestEntryProcessor implements EntryProcessor<Integer, Person, Object> {
        /** Value to set. */
        private Person value;

        /** */
        public TestEntryProcessor(Person value) {
            this.value = value;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Integer, Person> entry,
            Object... objects) throws EntryProcessorException {

            entry.setValue(value);

            return null;
        }
    }
}
