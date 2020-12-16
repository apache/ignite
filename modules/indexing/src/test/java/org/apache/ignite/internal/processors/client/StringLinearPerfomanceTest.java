package org.apache.ignite.internal.processors.client;

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class StringLinearPerfomanceTest extends GridCommonAbstractTest {
    /** Ignite instance. */
    private Ignite ignite;

    private final String cacheName = "default_cache";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        ignite = startGrids(1);

        createTable();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        stopAllClients(true);

        cleanPersistenceDir();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        return new IgniteConfiguration();
    }

    private void createTable() throws Exception {
        try (Ignite client = startClientGrid()) {
            IgniteCache c = client.getOrCreateCache(cacheName);

            c.query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS T1 (ID INT PRIMARY KEY, V VARCHAR)")).getAll();
        }
    }

    private IgniteConfiguration getClientConfuguration() throws Exception {
        return new IgniteConfiguration(getConfiguration()).setClientMode(true);
    }

    @Test
    public void test() throws Exception {
        int len = 400_000;

        long t1 = getTime(len);
        long t2 = getTime(len * 10);
        long t3 = getTime(len * 100);
        long t4 = getTime(len * 1000);

        assertTrue(t2 <= t1 * 10);
        assertTrue(t3 <= t1 * 100);
        assertTrue(t4 <= t1 * 1000);
    }

    public long getTime(int len) throws Exception {
        try (Ignite client = startClientGrid()) {
            IgniteCache c = client.getOrCreateCache("default");

            c.query(new SqlFieldsQuery("DELETE FROM T1"));

            /*
             *    400 000 ->      92 ms
             *  4 000 000 ->     863 ms
             * 40 000 000 -> 767 991 ms
             * */
            String bigValue = StringUtils.leftPad("1", len, "_");

            c.query(new SqlFieldsQuery("INSERT INTO T1(ID, V) VALUES (?, ?)").setArgs(0, bigValue));

            long d1 = System.currentTimeMillis();
            c.query(new SqlFieldsQuery("SELECT T1.ID, T1.V FROM T1")).getAll().size();
            long d2 = System.currentTimeMillis();

            return d2 - d1;
        }
    }
}
