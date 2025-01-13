package org.apache.ignite.internal.processors.cache.index;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

import static org.apache.ignite.configuration.DataPageEvictionMode.RANDOM_2_LRU;

/** Tests for CREATE TABLE IF NOT EXISTS */
public class CreateTableIfNotExistsSelfTest extends AbstractSchemaSelfTest {
    /** Node to restart id. */
    private static final int NODE_TO_RESTART_ID = 2;

    /** Client node index. */
    private static final int CLIENT_ID = 3;

    /** */
    private static final String TEST_CACHE_NAME = "SQL_PUBLIC_DMSOPERATIONAMOUNTATTRIBUTE";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        for (IgniteConfiguration cfg : configurations())
            Ignition.start(cfg);

        client().addCacheConfiguration(cacheConfiguration());
        client().getOrCreateCache(cacheConfiguration());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        execute("DROP TABLE IF EXISTS PUBLIC.SQL_PUBLIC_DMSOPERATIONAMOUNTATTRIBUTE");

        super.afterTest();
    }

    /**
     * Test that attempting to {@code CREATE TABLE} that already exists does not yield an error if the statement
     *     contains {@code IF NOT EXISTS} clause.
     */
    @Test
    public void testCreateTableIfNotExists() throws Exception {
        execute(
            "CREATE TABLE IF NOT EXISTS DmsOperationAmountAttribute " +
                "(objectId VARCHAR PRIMARY KEY, version BIGINT, dateTime TIMESTAMP, amount DECIMAL, " +
                "currencyAmount DECIMAL, currency VARCHAR, decision INT, serviceCode VARCHAR, channel VARCHAR, " +
                "payerId VARCHAR, payerTin VARCHAR, receiverId VARCHAR, receiverTin VARCHAR, receiverAccount VARCHAR, " +
                "payerAccount VARCHAR) " +
                "WITH \"VALUE_TYPE=ru.sbrf.pprb.compliance.database.entities.cumulative.DmsOperationAmountAttribute, " +
                "CACHE_NAME=SQL_PUBLIC_DMSOPERATIONAMOUNTATTRIBUTE, " +
                "BACKUPS=1, " +
                "WRITE_SYNCHRONIZATION_MODE=PRIMARY_SYNC\""
        );

        execute("SELECT * FROM PUBLIC.DMSOPERATIONAMOUNTATTRIBUTE");

        restartNode(NODE_TO_RESTART_ID);

//        execute(
//            "CREATE TABLE IF NOT EXISTS DmsOperationAmountAttribute " +
//                "(objectId VARCHAR PRIMARY KEY, version BIGINT, dateTime TIMESTAMP, amount DECIMAL, " +
//                "currencyAmount DECIMAL, currency VARCHAR, decision INT, serviceCode VARCHAR, channel VARCHAR, " +
//                "payerId VARCHAR, payerTin VARCHAR, receiverId VARCHAR, receiverTin VARCHAR, receiverAccount VARCHAR, " +
//                "payerAccount VARCHAR) " +
//                "WITH \"VALUE_TYPE=ru.sbrf.pprb.compliance.database.entities.cumulative.DmsOperationAmountAttribute, " +
//                "CACHE_NAME=SQL_PUBLIC_DMSOPERATIONAMOUNTATTRIBUTE, " +
//                "BACKUPS=1, " +
//                "WRITE_SYNCHRONIZATION_MODE=PRIMARY_SYNC\""
//        );

        Thread.sleep(1000);

        execute("SELECT * FROM PUBLIC.DMSOPERATIONAMOUNTATTRIBUTE");
    }

    /**
     * Execute DDL statement on client node.
     *
     * @param sql Statement.
     */
    private void execute(String sql) {
        execute(client(), sql);
    }

    /**
     * @return Client node.
     */
    private IgniteEx client() {
        return grid(CLIENT_ID);
    }

    /**
     * @param idx Index.
     */
    private void restartNode(int idx) throws Exception {
        grid(idx).close();

        Ignition.start(serverConfiguration(idx));
    }

    /**
     * Get configurations to be used in test.
     *
     * @return Configurations.
     * @throws Exception If failed.
     */
    private List<IgniteConfiguration> configurations() throws Exception {
        return Arrays.asList(
            serverConfiguration(0),
            serverConfiguration(1),
            serverConfiguration(2),
            clientConfiguration(3)
        );
    }

    /**
     * Create server configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    private IgniteConfiguration serverConfiguration(int idx) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(idx));

        DataRegionConfiguration dataRegionConfiguration = new DataRegionConfiguration()
            .setName("default")
            .setPersistenceEnabled(false)
            .setMetricsEnabled(true)
            .setCdcEnabled(true)
            .setPageEvictionMode(RANDOM_2_LRU);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(dataRegionConfiguration));

        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * Create client configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    private IgniteConfiguration clientConfiguration(int idx) throws Exception {
        return commonConfiguration(idx).setClientMode(true);
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration<?, ?> cacheConfiguration() {
        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(TEST_CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setBackups(1);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
        ccfg.setStatisticsEnabled(true);

        return ccfg;
    }
}