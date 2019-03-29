package org.apache.ignite.internal.processors.cache.persistence.db;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.WalSegmentArchivedEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.aware.SegmentAware;
import org.apache.ignite.internal.processors.cache.persistence.wal.filehandle.FileWriteHandle;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_WAL_SEGMENT_ARCHIVED;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_SEGMENT_FILE_COMPACTED_PATTERN;

public class IgnitePdsStartWIthEmptyArchive extends GridCommonAbstractTest {

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
        );

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setMaxWalArchiveSize(Long.MAX_VALUE)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        return cfg;
    }

    @Before
    public void before() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    @Test
    public void test() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        FileWriteAheadLogManager walMgr = (FileWriteAheadLogManager)ig.context().cache().context().wal();

        try (IgniteDataStreamer<Integer, byte[]> st = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
            int entries = 1000;

            for (int i = 0; i < entries; i++) {
                st.addData(i, new byte[1024 * 1024]);
            }
        }

        File archiveDir = U.field(walMgr, "walArchiveDir");

        stopGrid(0, false);

        SegmentAware beforeSaw = U.field(walMgr, "segmentAware");

        long beforeLastArchivedAbsoluteIndex = beforeSaw.lastArchivedAbsoluteIndex();

        File[] files = archiveDir.listFiles(WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER);

        Arrays.sort(files);

        for (File f : files) {
            if (f.delete())
                log.info("File " + f.getAbsolutePath() + " deleted");
        }

        Assert.assertEquals(0, archiveDir.listFiles().length);

        ig = startGrid(0);

        walMgr = (FileWriteAheadLogManager)ig.context().cache().context().wal();

        SegmentAware afterSaw = U.field(walMgr, "segmentAware");

        long afterLastArchivedAbsoluteIndex = afterSaw.lastArchivedAbsoluteIndex();

        int segments = ig.configuration().getDataStorageConfiguration().getWalSegments();

        Assert.assertTrue(
            "lastArchivedBefore=" + beforeLastArchivedAbsoluteIndex +
                ", lastArchivedAfter=" + afterLastArchivedAbsoluteIndex + ",  segments=" + segments,
            afterLastArchivedAbsoluteIndex >=
            (beforeLastArchivedAbsoluteIndex - segments));

        ig.cluster().active(true);

        FileWriteHandle fh = U.field(walMgr, "currHnd");

        Assert.assertNotNull(fh);

        long idx = fh.getSegmentId();

        Assert.assertTrue(idx >= beforeLastArchivedAbsoluteIndex);

        // Future for await all current available semgment will be archived.
        CountDownFuture awaitAchviedSegmentsLatch = new CountDownFuture(
            (int)(idx - afterLastArchivedAbsoluteIndex - 2/** one is a last archived, secod is a current write segment */)
        );

        log.info("idx=" + idx + ", lastArchivedBefore=" + beforeLastArchivedAbsoluteIndex +
            ", lastArchivedAfter=" + afterLastArchivedAbsoluteIndex + ",  segments=" + segments);

        ig.events().localListen(e -> {
            WalSegmentArchivedEvent archComplEvt = (WalSegmentArchivedEvent)e;

            log.info("EVT_WAL_SEGMENT_ARCHIVED:" + archComplEvt);

            if (archComplEvt.getAbsWalSegmentIdx() > afterLastArchivedAbsoluteIndex){
                awaitAchviedSegmentsLatch.onDone();

                return false;
            }

            if (archComplEvt.getAbsWalSegmentIdx() < afterLastArchivedAbsoluteIndex){
                awaitAchviedSegmentsLatch.onDone(new IgniteException("Unexected segment for archivation. idx="
                    + archComplEvt.getAbsWalSegmentIdx()));

                return false;
            }

            return true;
        }, EVT_WAL_SEGMENT_ARCHIVED);

        awaitAchviedSegmentsLatch.get();
    }
}
