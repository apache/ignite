/*
 * Copyright (C) GridGain Systems. All Rights Reserved.
 * _________        _____ __________________        _____
 * __  ____/___________(_)______  /__  ____/______ ____(_)_______
 * _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 * / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 * \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 *
 */

package wal;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.CleanCacheStoresMaintenanceAction;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CORRUPTED_DATA_FILES_MNTC_TASK_NAME;

public class WalDisableTest {
    private static final String CACHE_NAME = "MY_CACHE";
    private static final String CACHE_NAME_2 = "MY_CACHE_2";

    public static void main(String[] args) {
        LinkedList<Ignite> nodes = new LinkedList<>();

        for (int i = 0; i < 4; i++) {
            nodes.add(Ignition.start(igniteCfg(false, "server_" + i)));
        }
        nodes.getFirst().active(true);

        long testStart = System.currentTimeMillis();

        new Thread(new Runnable() {
            @Override
            public void run() {
                Ignite client = Ignition.start(igniteCfg(false, "client"));

                try {
                    while (!Thread.interrupted()) {
                        client.cluster().disableWal(CACHE_NAME);

                        Thread.sleep(2_000);

                        client.cluster().enableWal(CACHE_NAME);
                    }
                } catch (IgniteException ex) {
                    if(ex.getMessage().contains("Operation result is unknown because nodes reported different results")) {
                        System.out.println("TEST FAILED");
                        ex.printStackTrace(System.out);
                        System.exit(-1);
                    }
                    else
                        ex.printStackTrace();
                }
                catch (InterruptedException e) {
                    return;
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                int waitTime = 2 * 60_000;

                try {
                    Thread.sleep(waitTime);
                }
                catch (InterruptedException e) {

                }

                System.exit(0);
            }
        });

        while (true) {
            try {
                Ignite ignite = nodes.removeFirst();

                String consistentId = (String)ignite.cluster().localNode().consistentId();

                ignite.close();

                Thread.sleep(1000);

                nodes.add(startNodeWithMaintenance(consistentId));
            }
            catch (Exception ex) {
                ex.printStackTrace();

                System.exit(1);
            }
        }

        //Ignition.stopAll(true);
    }

    /** */
    private static Ignite startNodeWithMaintenance(String consistentId) throws Exception {
        Ignite node;

        try {
            node = Ignition.start(igniteCfg(false, consistentId));
        }
        catch (Exception ex) {
            if (!X.hasCause(ex, "Cache groups with potentially corrupted partition files", IgniteException.class))
                throw ex;

            node = Ignition.start(igniteCfg(false, consistentId));

            node.compute().run(new IgniteRunnable() {
                /** */
                @IgniteInstanceResource
                private Ignite ignite;

                /** */
                @Override public void run() {
                    MaintenanceRegistry mntcRegistry = ((IgniteEx) ignite).context().maintenanceRegistry();

                    List<MaintenanceAction<?>> actions = mntcRegistry
                        .actionsForMaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_NAME);

                    actions.stream()
                        .filter(a -> a.name().equals(CleanCacheStoresMaintenanceAction.ACTION_NAME)).findFirst()
                        .get().execute();

                    mntcRegistry.unregisterMaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_NAME);
                }
            });

            node.close();

            node = Ignition.start(igniteCfg(false, consistentId));
        }

        return node;
    }

    private static IgniteConfiguration igniteCfg(boolean client, String name) {
        IgniteConfiguration igniteCfg = new IgniteConfiguration();

        igniteCfg.setIgniteInstanceName(name);
        igniteCfg.setConsistentId(name);

        igniteCfg.setClientMode(client);

        TcpCommunicationSpi comm = new TcpCommunicationSpi();

//        comm.setSharedMemoryPort(47800);

        igniteCfg.setCommunicationSpi(comm);

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("localhost:47500..47550"));

        TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi();
        tcpDiscoverySpi.setLocalPort(47500);

        igniteCfg.setDiscoverySpi(tcpDiscoverySpi.setIpFinder(ipFinder));

        CacheConfiguration configuration = new CacheConfiguration(CACHE_NAME);
        configuration.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        configuration.setBackups(0);
        configuration.setCacheMode(CacheMode.PARTITIONED);

        CacheConfiguration configuration2 = new CacheConfiguration(CACHE_NAME_2);
        configuration2.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        configuration2.setBackups(0);
        configuration2.setCacheMode(CacheMode.PARTITIONED);

        igniteCfg.setCacheConfiguration(configuration, configuration2);

        igniteCfg
                .setDataStorageConfiguration(
                        new DataStorageConfiguration()
                                .setDefaultDataRegionConfiguration(
                                        new DataRegionConfiguration().setMaxSize(1 * 1024L * 1024 * 1024)
                                                .setPersistenceEnabled(true)
                                ));
        return igniteCfg;
    }
}
