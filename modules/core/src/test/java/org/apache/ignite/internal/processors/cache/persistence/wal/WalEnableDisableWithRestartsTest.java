package org.apache.ignite.internal.processors.cache.persistence.wal;

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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CORRUPTED_DATA_FILES_MNTC_TASK_NAME;

public class WalEnableDisableWithRestartsTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "MY_CACHE";

    /** */
    private static final String CACHE_NAME_2 = "MY_CACHE_2";

    /** */
    private static final int CYCLES = 3;

    /** */
    public static final int NODES = 4;

    /** */
    private static volatile boolean shutdown = false;

    /** */
    private static volatile boolean failure = false;

    /** */
    @Test
    public void test() throws Exception {
        LinkedList<Ignite> nodes = new LinkedList<>();

        for (int i = 0; i < NODES; i++)
            nodes.add(Ignition.start(igniteCfg(false, "server_" + i)));

        nodes.getFirst().active(true);

        Ignite client = Ignition.start(igniteCfg(true, "client"));

        new Thread(new Runnable() {
            public void run() {
                try {
                    for (int i = 0; i < CYCLES; i++) {
                        System.err.println("*** CYCLE " + i);

                        client.cluster().disableWal(CACHE_NAME);

                        Thread.sleep(1_000);

                        client.cluster().enableWal(CACHE_NAME);

                        Thread.sleep(1_000);
                    }
                }
                catch (IgniteException ex) {
                    if (ex.getMessage().contains("Operation result is unknown because nodes reported different results")) {
                        System.out.println("TEST FAILED");

                        ex.printStackTrace(System.out);

                        failure = true;
                    }
                }
                catch (InterruptedException ex) {
                    return;
                }
                catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                finally {
                    shutdown = true;
                }
            }
        }).start();

        while (!shutdown) {
            Thread.sleep(1_000);

            Ignite ignite = nodes.removeFirst();

            String consistentId = (String) ignite.cluster().localNode().consistentId();

            ignite.close();

            Thread.sleep(1_000);

            nodes.add(startNodeWithMaintenance(consistentId));
        }

        assertFalse(failure);
    }

    @After
    public void cleanup() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    private Ignite startNodeWithMaintenance(String consistentId) throws Exception {
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

    /** */
    private IgniteConfiguration igniteCfg(boolean client, String name) throws Exception {
        IgniteConfiguration igniteCfg = getConfiguration(name);

        igniteCfg.setConsistentId(name);

        igniteCfg.setClientMode(client);

        CacheConfiguration configuration = new CacheConfiguration(CACHE_NAME);
        configuration.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        configuration.setBackups(0);
        configuration.setCacheMode(CacheMode.PARTITIONED);

        CacheConfiguration configuration2 = new CacheConfiguration(CACHE_NAME_2);
        configuration2.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        configuration2.setBackups(0);
        configuration2.setCacheMode(CacheMode.PARTITIONED);

        igniteCfg.setCacheConfiguration(configuration, configuration2);

        igniteCfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(1 * 1024L * 1024 * 1024).setPersistenceEnabled(true)));

        return igniteCfg;
    }
}
