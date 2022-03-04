package org.apache.ignite.util;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.platform.cache.expiry.PlatformExpiryPolicyFactory;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 *
 */
public class ThinClientExpirePolicyTest extends GridCommonAbstractTest {
    /** */
    public static volatile boolean isReady;

    /** */
    private final Consumer<IgniteClient> clientConsumer = client -> {
        ClientCache<UUID, UUID> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        while (!Thread.currentThread().isInterrupted()) {
            try {
//                TimeUnit.MILLISECONDS.sleep(10);

                ClientTransaction tx = client.transactions().txStart();

                UUID uuid = UUID.randomUUID();

                cache.put(uuid, uuid);

                tx.commit();
            } catch (ClientException e) {
                e.printStackTrace();
                System.err.println(cache.size(CachePeekMode.PRIMARY));
                // No-op.
            }
        }
    };

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc}
     * @return*/
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        long time = TimeUnit.SECONDS.toMillis(5);

        PlatformExpiryPolicyFactory plc = new PlatformExpiryPolicyFactory(time, time, time);

        CacheConfiguration<UUID, UUID> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cacheCfg.setExpiryPolicyFactory(plc);
        cacheCfg.setBackups(2);

        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(cacheCfg)
            .setConnectorConfiguration(new ConnectorConfiguration())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));
    }

    /**
     * @throws Exception
     */
    @Test
    public void expirePolicyTest() throws Exception {
        IgniteEx igniteEx = startGrids(2);

        igniteEx.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = igniteEx.getOrCreateCache(DEFAULT_CACHE_NAME);

        startIgniteClientDaemonThread(clientConsumer);
        startIgniteClientDaemonThread(clientConsumer);
        startIgniteClientDaemonThread(clientConsumer);
        startIgniteClientDaemonThread(clientConsumer);
        startIgniteClientDaemonThread(clientConsumer);
        startIgniteClientDaemonThread(clientConsumer);
        startIgniteClientDaemonThread(clientConsumer);
        startIgniteClientDaemonThread(clientConsumer);
        startIgniteClientDaemonThread(clientConsumer);
        startIgniteClientDaemonThread(clientConsumer);
        startIgniteClientDaemonThread(clientConsumer);
        startIgniteClientDaemonThread(clientConsumer);
        startIgniteClientDaemonThread(clientConsumer);
        startIgniteClientDaemonThread(clientConsumer);
        startIgniteClientDaemonThread(clientConsumer);
        startIgniteClientDaemonThread(clientConsumer);

        TimeUnit.SECONDS.sleep(30);

        System.err.println(">>>cache.size");
        System.err.println(cache.size());

        isReady = true;

        igniteEx.cluster().state(ClusterState.INACTIVE);

        TimeUnit.SECONDS.sleep(60);

        igniteEx.cluster().state(ClusterState.ACTIVE);

        System.err.println("igniteEx.cluster().topologyVersion");
        System.err.println(igniteEx.cluster().topologyVersion());
        System.err.println(igniteEx.cache(DEFAULT_CACHE_NAME).size());

        TimeUnit.SECONDS.sleep(20);

        assertEquals(3, igniteEx.context().discovery().aliveServerNodes().size());
        assertEquals(3, G.allGrids().size());
        assertEquals(ClusterState.ACTIVE, G.allGrids().get(0).cluster().state());
        assertEquals(ClusterState.ACTIVE, G.allGrids().get(1).cluster().state());
        assertEquals(ClusterState.ACTIVE, G.allGrids().get(2).cluster().state());
    }

    /**
     * @return Thin client for specified user.
     */
    private Thread startIgniteClientDaemonThread(Consumer<IgniteClient> consumer) {
        Thread thread = new Thread(() -> {
            try (IgniteClient igniteClient = Ignition.startClient(
                new ClientConfiguration().setAddresses(Config.SERVER))) {
                consumer.accept(igniteClient);
            }
        });

        thread.setDaemon(true);

        thread.start();

        return thread;
    }
}
