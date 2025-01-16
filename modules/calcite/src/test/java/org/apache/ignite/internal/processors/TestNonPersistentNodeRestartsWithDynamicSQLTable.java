package org.apache.ignite.internal.processors;

import java.util.List;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class TestNonPersistentNodeRestartsWithDynamicSQLTable extends GridCommonAbstractTest {
    /** */
    private static final int NODES_CNT = 3;

    /** */
    private static final int NODE_TO_RESTART_ID = 1;

    /** */
    private static final int EXEC_NODE_ID = 2;

    /** */
    private static final String TEST_CACHE_NAME = "TEST_CACHE";

    /** */
    private static volatile boolean TEST;

    /** */
    private boolean persistence;

    /** */
    private int backups = 2;

    /** */
    private boolean predefineCaches = true;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // Enable persistence, make the cluster ACTIVe at the start and the test will work.
        DataRegionConfiguration dataRegionConfiguration = new DataRegionConfiguration()
            .setName("default")
            .setPersistenceEnabled(persistence);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(dataRegionConfiguration));

        // Or comment this ant the test will also work.
        if (predefineCaches)
            cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /** */
    private CacheConfiguration<?, ?> cacheConfiguration() {
        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(TEST_CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setBackups(backups);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);

        return ccfg;
    }

    /** */
    @Test
    public void testChangeCacheConfig() throws Exception {
        startGrid(0);
        startGrid(1);
        startGrid(2);

        backups = 1;

        stopGrid(2);

        awaitPartitionMapExchange();

        TEST = true;

        startGrid(2);
    }

    /** */
    @Test
    public void testChangeSchema() throws Exception {
        persistence = true;
        predefineCaches = false;

        startGrids(NODES_CNT);

        grid(0).cluster().state(ClusterState.ACTIVE);

        query("CREATE TABLE IF NOT EXISTS TEST_TBL(ID INTEGER PRIMARY KEY, VAL VARCHAR) WITH \"CACHE_NAME=" + TEST_CACHE_NAME + '"');

        assertEquals(0, query("SELECT * FROM TEST_TBL").size());

        stopGrid(0);
        stopGrid(1);

        //grid(2).destroyCache(TEST_CACHE_NAME);
        query("DROP TABLE TEST_TBL");
        assertNull(grid(2).cache(TEST_CACHE_NAME));

        query("CREATE TABLE IF NOT EXISTS TEST_TBL(ID INTEGER PRIMARY KEY, VAL VARCHAR, VAL2 DOUBLE) WITH \"CACHE_NAME=" + TEST_CACHE_NAME + '"');

        assertEquals(0, query("SELECT * FROM TEST_TBL").size());

        stopGrid(2);

        startGrid(0);
        startGrid(1);
        grid(0).cluster().state(ClusterState.ACTIVE);

        assertEquals(0, query("SELECT * FROM TEST_TBL", 1).size());
    }

    /** */
    @Test
    public void testNodeRejoinsAndUsesTheTable() throws Exception {
        startGrids(NODES_CNT);

        query("CREATE TABLE IF NOT EXISTS TEST_TBL(ID INTEGER PRIMARY KEY, VAL VARCHAR) WITH \"CACHE_NAME=" + TEST_CACHE_NAME + '"');

        assertEquals(0, query("SELECT * FROM TEST_TBL").size());

        TEST = true;

        stopGrid(NODE_TO_RESTART_ID);
        startGrid(NODE_TO_RESTART_ID);

        // Btw, comment `awaitPartitionMapExchange()` and the inserts to prevent failure with 'table not found'.
        awaitPartitionMapExchange();

        // Inserts works.
        for (int i = 0; i < 100; ++i)
            assertEquals(1, query("INSERT INTO TEST_TBL VALUES(" + (i + 1) + ", '" + (i + 1000) + "')").size());

        assertEquals(100, grid(EXEC_NODE_ID).cache(TEST_CACHE_NAME).size());

        //Thread.sleep(1000);

        // Failes with 'table not found'.
        assertEquals(100, query("SELECT * FROM TEST_TBL").size());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        G.stopAll(false);

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** */
    private List<List<?>> query(String sql, int nodeIdx) {
        return grid(nodeIdx).context().query().querySqlFields(new SqlFieldsQuery(sql).setSchema("PUBLIC"), true).getAll();
    }

    /** */
    private List<List<?>> query(String sql) {
        return query(sql, EXEC_NODE_ID);
    }
}
