package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.io.File;
import java.io.FileFilter;

public class WalCompactionSwitchOnTest extends GridCommonAbstractTest {
    private boolean compactionEnabled;

    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                            .setMaxSize(256 * 1024 * 1024))
                .setWalSegmentSize(512 * 1024)
                .setWalSegments(100)
                .setWalCompactionEnabled(compactionEnabled));

        return cfg;
    }

    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    public void testWalCompactionSwitchWithGap() throws Exception {
        IgniteEx ex = startGrid(0);

        ex.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ex.getOrCreateCache(
            new CacheConfiguration<Integer, Integer>()
                    .setName("c1")
                    .setGroupName("g1")
                    .setCacheMode(CacheMode.PARTITIONED)
        );

        for (int i = 0; i < 500; i++)
            cache.put(i, i);

        File walDir = U.resolveWorkDirectory(
                ex.configuration().getWorkDirectory(),
                "db/wal/node00-" + ex.localNode().consistentId(),
                false
        );

        forceCheckpoint();

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override
            public boolean apply() {
                File[] archivedFiles = walDir.listFiles(new FileFilter() {
                    @Override
                    public boolean accept(File pathname) {
                        return pathname.getName().endsWith(".wal");
                    }
                });


                return archivedFiles.length == 39;
            }
        }, 5000);

        stopGrid(0);

        compactionEnabled = true;

        ex = startGrid(0);

        ex.cluster().active(true);


        File archiveDir = U.resolveWorkDirectory(
                ex.configuration().getWorkDirectory(),
                "db/wal/archive/node00-" + ex.localNode().consistentId(),
                false
        );

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override
            public boolean apply() {
                File[] archivedFiles = archiveDir.listFiles(new FileFilter() {
                    @Override
                    public boolean accept(File pathname) {
                        return pathname.getName().endsWith(FilePageStoreManager.ZIP_SUFFIX);
                    }
                });


                return archivedFiles.length == 20;
            }
        }, 5000);

        File[] tmpFiles = archiveDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(FilePageStoreManager.TMP_SUFFIX);
            }
        });

        assertEquals(0, tmpFiles.length);
    }

    @Override
    protected void afterTest() throws Exception {
        stopAllGrids();
    }
}
