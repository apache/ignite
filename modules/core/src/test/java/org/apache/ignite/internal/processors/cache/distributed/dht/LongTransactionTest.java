package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;

/**
 */
public class LongTransactionTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 3;
    /** Client. */
    private static final String CLIENT = "client";
    /** Client. */
    private IgniteEx client;
    /** Ignite. */
    private IgniteEx ignite;
    /** Logging. */
    private boolean logging;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        boolean isClient = CLIENT.equals(igniteInstanceName);

        return super.getConfiguration(igniteInstanceName)
            .setClientMode(isClient)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setWalMode(WALMode.DEFAULT)
                .setCheckpointFrequency(1_000L)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setCheckpointPageBufferSize(10L * 1024 * 1024)
                    .setMaxSize(200L * 1024 * 1024)))
            .setActiveOnStart(false)
            .setClientMode(CLIENT.equals(igniteInstanceName))
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                    .setBackups(2),
                new CacheConfiguration(DEFAULT_CACHE_NAME + 1)
                    .setCacheMode(CacheMode.REPLICATED)
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                    .setAffinity(new RendezvousAffinityFunction(false, 3))
                    .setBackups(0))
            .setTransactionConfiguration(new TransactionConfiguration()
                .setDefaultTxConcurrency(OPTIMISTIC)
                .setDefaultTxIsolation(TransactionIsolation.SERIALIZABLE));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite = (IgniteEx)startGrids(GRID_CNT);
        client = (IgniteEx)startGrid(CLIENT);
    }

    public void test() throws Exception {
        GridTestUtils.runAsync(() -> {
            for (; ; ) {
                if (logging)
                    info(dumpStack("exchange-worker"));
                    info(dumpStack("grid-timeout-worker"));

                Thread.sleep(500);
            }
        });

        if (!client.cluster().active())
            client.cluster().active(true);

        CountDownLatch latch = new CountDownLatch(1);

        GridTestUtils.runAsync(() -> {
            IgniteCache cache = client.cache(DEFAULT_CACHE_NAME);

            for (int i = 0; i < 100; i++)
                cache.put(i, "init");

            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, TransactionIsolation.READ_COMMITTED)) {

                for (int i = 0; i < 100; i++)
                    cache.put(i, String.valueOf(i));

                latch.countDown();

                logging = true;
                try {
                    Thread.sleep(20_000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        latch.await();

        startGrid(GRID_CNT);

        client.cluster().setBaselineTopology(client.cluster().topologyVersion());

        awaitPartitionMapExchange();

        logging = false;
    }

    public void testCheckpoint() throws Exception {
        GridTestUtils.runAsync(() -> {
            for (; ; ) {
                if (logging)
                    info(dumpStack("checkpoint"));
                    info(dumpStack("sys-stripe"));

                Thread.sleep(500);
            }
        });

        if (!client.cluster().active())
            client.cluster().active(true);

        CountDownLatch latch = new CountDownLatch(1);

        GridTestUtils.runAsync(() -> {
            IgniteCache cache = client.cache(DEFAULT_CACHE_NAME);
            logging = true;

            for (long i = 0; ; i++) {
                if (i == 1_000)
                    latch.countDown();

                cache.put(i, "init");
            }

        });

        latch.await();

        startGrid(GRID_CNT);

        client.cluster().setBaselineTopology(client.cluster().topologyVersion());

        awaitPartitionMapExchange();

        logging = false;
    }

    public void testWalArchiver() throws Exception {
        GridTestUtils.runAsync(() -> {
            for (; ; ) {
                if (logging)
//                    info(dumpStack("wal-file-archiver"));
                    info(dumpStack("wal-write-worker"));

                Thread.sleep(500);
            }
        });

        if (!client.cluster().active())
            client.cluster().active(true);

        CountDownLatch latch = new CountDownLatch(1);

        GridTestUtils.runAsync(() -> {
            IgniteCache cache = client.cache(DEFAULT_CACHE_NAME);
            logging = true;

            for (long i = 0; ; i++) {
                if (i == 1_000)
                    latch.countDown();

                cache.put(i, "init");
            }

        });

        latch.await();

        startGrid(GRID_CNT);

        client.cluster().setBaselineTopology(client.cluster().topologyVersion());

        awaitPartitionMapExchange();

        logging = false;
    }

    public void testTxTimeout() throws Exception {
        GridTestUtils.runAsync(() -> {
            for (; ; ) {
                if (logging)
                    info(dumpStack("grid-timeout-worker"));

                Thread.sleep(500);
            }
        });

        if (!client.cluster().active())
            client.cluster().active(true);

        CountDownLatch latch = new CountDownLatch(1);

        GridTestUtils.runAsync(() -> {
            IgniteCache cache = client.cache(DEFAULT_CACHE_NAME);
            logging = true;

            for (long i = 0; ; i++) {
                if (i % 10 == 0)
                    client.transactions().txStart(PESSIMISTIC, TransactionIsolation.READ_COMMITTED, 100, 10);

                if (i == 1_000)
                    latch.countDown();

                cache.put(i, "init");

            }

        });

        Thread.sleep(1_000);

        latch.await();

        client.close();

        Thread.sleep(1_000);

//        startGrid(GRID_CNT);
//
//        client.cluster().setBaselineTopology(client.cluster().topologyVersion());
//
//        awaitPartitionMapExchange();

        logging = false;
    }

    public static String dumpStack(String tName) {
        StringBuilder sb = new StringBuilder()
            .append("dump:\n");

        for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
            if (!entry.getKey().getName().contains(tName))
                continue;

            sb.append(entry.getKey())
                .append(" ")
                .append(entry.getKey().getState())
                .append("\n");

            for (StackTraceElement ste : entry.getValue())
                sb.append("\tat ")
                    .append(ste)
                    .append("\n");
        }

        return sb.toString();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        logging = false;
    }
}
