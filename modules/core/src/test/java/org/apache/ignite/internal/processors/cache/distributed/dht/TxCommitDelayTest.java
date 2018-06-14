package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.NotNull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;

/**
 */
public class TxCommitDelayTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 3;
    /** Client. */
    private static final String CLIENT = "client";

    private CountDownLatch lastMsgSend = new CountDownLatch(1);

    private UUID firstNodeId = null;

    /** Logging. */
    private boolean logging;

    /** Client. */
    private IgniteEx client;

    /** {@inheritDoc} */
    protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(GRID_CNT);
        client = (IgniteEx)startGrid(CLIENT);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        boolean isClient = CLIENT.equals(igniteInstanceName);

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setClientMode(isClient)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
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

        cfg.setCommunicationSpi(new TcpCommunicationSpi() {
            private int sentFinishRequest = 0;

            @Override public void sendMessage(ClusterNode node, Message msg,
                IgniteInClosure<IgniteException> ackClosure) throws IgniteSpiException {

//                if (isClient && logging)
//                    log.info("555 " + ((GridIoMessage)msg).message());

                if (isClient && logging && ((GridIoMessage)msg).message().getClass() == GridNearTxFinishRequest.class) {
                    sentFinishRequest++;

                    if (!firstNodeId.equals(node.id())) {
                        log.info("Block message GridDistributedTxFinishRequest to " + node.id() + ": " + ((GridIoMessage)msg).message());

                        lastMsgSend.countDown();

                        try {
                            Thread.sleep(60_000);
                        }
                        catch (InterruptedException e) {
                            e.printStackTrace();
                        }
//                        if (sentFinishRequest == GRID_CNT)
                        return;

                    } else
                        log.info("Send message GridDistributedTxFinishRequest to " + node.id() + ": " + ((GridIoMessage)msg).message());

//                    if (sentFinishRequest == GRID_CNT) {
//                        log.info("Last GridDistributedTxFinishRequest to " + node.id() + " hangs: " + ((GridIoMessage)msg).message());
//
//                        try {
//                            Thread.sleep(1_000);
//                        }
//                        catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
//
//                        lastMsgSend.countDown();
//
//                        try {
//                            Thread.sleep(100_000);
//                        }
//                        catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
//                    }
                }

                super.sendMessage(node, msg, ackClosure);
            }
        });

        return cfg;
    }

    /**
     * @return Index of node starting transaction.
     */
    private int originatingNode() {
        return 0;
    }

    /**
     *
     */
    public void test() throws Exception {
        if (!client.cluster().active())
            client.cluster().active(true);

        firstNodeId = grid(originatingNode()).localNode().id();

        Collection<Integer> keys = prepareKeys();

        final String initVal = "initialValue";

        final Map<Integer, String> map = new HashMap<>();

        for (Integer key : keys) {
            grid(originatingNode()).cache(DEFAULT_CACHE_NAME).put(key, initVal);

            map.put(key, String.valueOf(key));
        }

        logging = true;

        GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteCache<Integer, String> cache = client.cache(DEFAULT_CACHE_NAME);
                IgniteCache<Integer, String> cache1 = client.cache(DEFAULT_CACHE_NAME + 1);

                assertNotNull(cache);

                TransactionProxyImpl tx = (TransactionProxyImpl)client.transactions().txStart(OPTIMISTIC, TransactionIsolation.SERIALIZABLE, 1_000, 10);

                GridNearTxLocal txEx = tx.tx();

                assertTrue(txEx.optimistic());

                assertTrue(txEx.serializable());

                cache.putAll(map);

                cache1.put(1312312, "112321");

//                try {
//                    txEx.prepareNearTxLocal().get(3, TimeUnit.SECONDS);
//                }
//                catch (IgniteFutureTimeoutCheckedException ignored) {
//                    info("Failed to wait for prepare future completion");
//                }

                tx.commit();

                return null;
            }
        })/*.get()*/;

        lastMsgSend.await();

        try {
            Thread.sleep(1_000);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        G.stop(CLIENT, true);

        logging = false;

        final Collection<IgniteKernal> grids = new ArrayList<>();

        for (int i = 1; i < gridCount(); i++)
            grids.add((IgniteKernal)grid(i));

        boolean txFinished = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (IgniteKernal g : grids) {
                    GridCacheSharedContext<Object, Object> ctx = g.context().cache().context();

                    int txNum = ctx.tm().idMapSize();

                    if (txNum > 0) {
                        StringBuilder sb = new StringBuilder();

                        for (HashMap.Entry<Integer, String> entry : map.entrySet()) {
                            IgniteCache<Integer, String> cache = ignite(originatingNode()).cache(DEFAULT_CACHE_NAME);

                            sb.append(entry.getKey())
                                .append(": ")
                                .append(cache.get(entry.getKey()))
                                .append(", ");
                        }

                        log.info("Num: " + txNum + " " + ctx.tm().activeTransactions().iterator().next().state() + "\n" + sb);
                    }

                    if (txNum != 0)
                        return false;
                }

                return true;
            }
        }, 10000);

        assertTrue(txFinished);

        info("Transactions finished.");

        for (HashMap.Entry<Integer, String> entry : map.entrySet()) {
            IgniteCache<Integer, String> cache = ignite(originatingNode()).cache(DEFAULT_CACHE_NAME);

            assertNotNull(cache);

            assertEquals(entry.getValue(), cache.get(entry.getKey()));
        }
    }

    /**
     *
     */
    @NotNull private Collection<Integer> prepareKeys() {
        List<ClusterNode> allNodes = new ArrayList<>(GRID_CNT);

        for (int i = 0; i < GRID_CNT; i++)
            allNodes.add(grid(i).localNode());

        Collection<Integer> keys = new ArrayList<>();

        for (int i = 0; i < Integer.MAX_VALUE && !allNodes.isEmpty(); i++) {
            for (Iterator<ClusterNode> iter = allNodes.iterator(); iter.hasNext(); ) {
                ClusterNode node = iter.next();

                if (grid(originatingNode()).affinity(DEFAULT_CACHE_NAME).isPrimary(node, i)) {
                    keys.add(i);

                    iter.remove();

                    break;
                }
            }
        }

        assertEquals(GRID_CNT, keys.size());
        return keys;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        logging = false;
    }
}
