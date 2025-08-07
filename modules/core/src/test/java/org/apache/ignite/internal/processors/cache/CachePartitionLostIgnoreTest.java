package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.ClusterNodeAttributeAffinityBackupFilter;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;


public class CachePartitionLostIgnoreTest extends GridCommonAbstractTest {

   private static IgniteConfiguration getCfg(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

       DataStorageConfiguration storageCfg = new DataStorageConfiguration();

       DataRegionConfiguration defaultRegion = new DataRegionConfiguration();
       defaultRegion.setPersistenceEnabled(true);
       defaultRegion.setName("Default_Region");
       defaultRegion.setInitialSize(100 * 1024 * 1024);
       storageCfg.setDefaultDataRegionConfiguration(defaultRegion);

       cfg.setDataStorageConfiguration(storageCfg);

       CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setPartitionLossPolicy(PartitionLossPolicy.IGNORE);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setRebalanceMode(CacheRebalanceMode.ASYNC);

        RendezvousAffinityFunction affinityFunction = new RendezvousAffinityFunction();
        affinityFunction.setPartitions(32);
        affinityFunction.setAffinityBackupFilter(new ClusterNodeAttributeAffinityBackupFilter("CELL"));

        ccfg.setAffinity(affinityFunction);
        ccfg.setBackups(1);

        cfg.setCacheConfiguration(ccfg);
        cfg.setClientMode(false);

        ClientConnectorConfiguration clientCfg = new ClientConnectorConfiguration();
        clientCfg.setPort(11211);
        clientCfg.setHandshakeTimeout(30_000);
        clientCfg.setSslEnabled(false);
        clientCfg.setUseIgniteSslContextFactory(false); // Убедиться, что SSL не активирован
        cfg.setClientConnectorConfiguration(clientCfg);


        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        spi.setIpFinder(new TcpDiscoveryMulticastIpFinder().setAddresses(Collections.singletonList("127.0.0.1:47500..47509")));
        cfg.setDiscoverySpi(spi);

        cfg.setIgniteInstanceName(igniteInstanceName);

       return cfg;
    }

    /** */
    private static final int PARTS_CNT = 4;

    /** */
    private PartitionLossPolicy lossPlc;

    /** */
    private boolean persistence;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setActiveOnStart(false);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(
                new DataStorageConfiguration()
                        .setWalMode(WALMode.LOG_ONLY)
                        .setWalSegmentSize(4 * 1024 * 1024)
                        .setDefaultDataRegionConfiguration(
                                new DataRegionConfiguration()
                                        .setPersistenceEnabled(persistence)
                                        .setMaxSize(100L * 1024 * 1024))
        );

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
                setAtomicityMode(ATOMIC).
                setCacheMode(PARTITIONED).
                setBackups(1).
                setPartitionLossPolicy(lossPlc).
                setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT)));

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }


    @Test
    public void test() throws Exception {
        lossPlc = PartitionLossPolicy.IGNORE;
        persistence = false;

        doTest_lostNode(PARTS_CNT);
    }

    private void doTest(int cnt) throws Exception {
        IgniteEx crd = startGrids(cnt);
        crd.cluster().baselineAutoAdjustEnabled(true);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        createDataAndPut(10000, cache);

        CacheConfiguration<?, ?> cfg = cache.getConfiguration(CacheConfiguration.class);

        PartitionLossPolicy policy = cfg.getPartitionLossPolicy();
        System.out.println("PartitionLossPolicy: " + policy);

        DataRegionConfiguration dcfg = crd
                .configuration()
                .getDataStorageConfiguration()
                .getDefaultDataRegionConfiguration();

        System.out.println("isPersistenceEnabled " + dcfg.isPersistenceEnabled());


        partitionsDistr(crd);

        Thread.sleep(5000);



    }


    private void doTest_lostNode(int cnt) throws Exception {
        IgniteEx crd = startGrids(cnt);
        crd.cluster().baselineAutoAdjustEnabled(true);
        crd.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = crd.cache(DEFAULT_CACHE_NAME);

        createDataAndPut(10000, cache);

        CacheConfiguration<?, ?> cfg = cache.getConfiguration(CacheConfiguration.class);
        PartitionLossPolicy policy = cfg.getPartitionLossPolicy();
        System.out.println("PartitionLossPolicy: " + policy);

        DataRegionConfiguration dcfg = crd
                .configuration()
                .getDataStorageConfiguration()
                .getDefaultDataRegionConfiguration();

        System.out.println("isPersistenceEnabled " + dcfg.isPersistenceEnabled());

        partitionsDistr(crd);

        // Симулируем потерю узлов
        System.out.println("ssss");
        stopGrid(2);
        stopGrid(3);

        Thread.sleep(2000);

        partitionsDistr(crd);

        Collection<Integer> lostPartitions = cache.lostPartitions();
        System.out.println("Потерянные партиции: " + lostPartitions);

        if (!lostPartitions.isEmpty()) {
            System.out.println("Кластер потерял партиции.");
        } else {
            System.out.println("Все партиции доступны, ошибка не воспроизвелась.");
        }

        startGrid(4);
        startGrid(5);
        //crd2.cluster().state(ClusterState.ACTIVE);

        Thread.sleep(2000);

        createDataAndPut(10000, cache);

        partitionsDistr(crd);

    }

    private static void partitionsDistr(IgniteEx crd) {
        Affinity<Integer> affinity = crd.affinity(DEFAULT_CACHE_NAME);
        int partitions = affinity.partitions();
        System.out.println("Partition distribution for cache: " + partitions);

        for (int i = 0; i < partitions; i++) {
            try {
                ClusterNode node = affinity.mapPartitionToNode(i);
                System.out.printf("Partition %d -> Node %s%n", i, node.id());
            }
            catch (Exception e)
            {
                System.out.printf("Partition %d -> lost?%n", i);
            }
        }
    }


    private static void createDataAndPut(int x, IgniteCache<Object, Object> cache) {
        Random random = new Random();
        Map<Integer, String> data = new HashMap<>();

        for (int i = 0; i < x; i++) {
            data.put(i, "value_" + random.nextInt(100000));
        }

        cache.putAll(data);
        System.out.println("В кеш вставлено " + data.size() + " записей.");
    }

    //==============================================================================================================================


    public static void main(String[] args) throws Exception {
      // startClient();
        try (Ignite ign = Ignition.start(getConfig())) {
            IgniteCache<Long, Long> cache = ign.getOrCreateCache("terminator");

            for (long i = 1; i < 10_000; ++i)
                cache.put(getRandomLong(i), getRandomLong(i));
        }
    }

private static ClientConfiguration getClientConfiguration() {
return new ClientConfiguration()
.setAddresses("127.0.0.1:10800", "127.0.0.1:10801", "127.0.0.1:10802", "127.0.0.1:10803");
}


private static IgniteConfiguration getConfig() {
return new IgniteConfiguration()
.setClientMode(true)
.setIgniteInstanceName(UUID.randomUUID().toString())
.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(
new TcpDiscoveryVmIpFinder().setAddresses(Collections.singletonList("127.0.0.1"))))
.setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true))
.setPeerClassLoadingEnabled(true);
}

    private static Long getRandomLong(long i) {
        return (long)(Math.random() * i);
    }



    public static void startNodes() throws Exception {

       List<Ignite> list = new ArrayList<>();
       String[] cellNames = {"cell1", "cell2", "cell3", "cell4"};

        for (int i = 0; i < 2; i++) {
            IgniteConfiguration cfg = getCfg("sss_" + i);

            Map<String, String> userAttrs = new HashMap<>();
            userAttrs.put("CELL", cellNames[i / 2]); // Каждые 2 узла попадают в одну и ту же ячейку
            cfg.setUserAttributes(userAttrs);

            Ignite node = Ignition.start(cfg);

            list.add(node);

            System.out.println("Запущен узел " + node.name() + " в " + userAttrs.get("CELL"));

            System.out.println(System.getProperty("IGNITE_HOME"));

            Thread.sleep(1000);


        }

        Ignite ignite = list.get(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        // Генерация и вставка случайных данных
        createDataAndPut(50000, cache);


        System.out.println("ddddd" + ignite.cache(DEFAULT_CACHE_NAME).metrics());
//


    }


    public static void startClient() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(true);

        cfg.setDiscoverySpi(new org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi()
                .setIpFinder(new org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder()
                        .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"))));

        // Запуск клиента
        Ignite ignite = Ignition.start(cfg);

        ignite.cluster().state(ClusterState.ACTIVE);


       // IgniteCache<Integer, String> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);


        CacheConfiguration cfg_1 = new CacheConfiguration("CoolCacheName").
                setAtomicityMode(ATOMIC).
                setCacheMode(PARTITIONED).
                setBackups(1).
                setPartitionLossPolicy(PartitionLossPolicy.IGNORE).
                setAffinity(new RendezvousAffinityFunction(false, 12));

        IgniteCache<Integer, String> cache_2 = ignite.createCache(cfg_1);




        // Генерация и вставка случайных данных
        Random random = new Random();
        Map<Integer, String> data = new HashMap<>();

        for (int i = 0; i < 500000; i++) {
            data.put(i, "value_" + random.nextInt(500000));
        }

        cache_2.putAll(data);
        System.out.println("В кеш вставлено " + data.size() + " записей.");

    }


}
