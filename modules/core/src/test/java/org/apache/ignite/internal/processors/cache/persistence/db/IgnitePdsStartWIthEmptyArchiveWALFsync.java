/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.io.File;
import java.util.Arrays;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.events.WalSegmentArchivedEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FsyncModeFileWriteAheadLogManager;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_WAL_SEGMENT_ARCHIVED;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER;

/**
 *
 */
public class IgnitePdsStartWIthEmptyArchiveWALFsync extends IgnitePdsStartWIthEmptyArchive {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
        );

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                // Checkpoint should not remove any WAL archive files.
                .setWalHistorySize(Integer.MAX_VALUE)
                .setWalMode(WALMode.FSYNC)
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

        IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();

        // Populate data for generate WAL archive segments.
        try (IgniteDataStreamer<Integer, byte[]> st = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
            int entries = 1000;

            for (int i = 0; i < entries; i++) {
                st.addData(i, new byte[1024 * 1024]);
            }
        }

        File archiveDir = U.field(walMgr, "walArchiveDir");

        stopGrid(0, false);

        long beforeLastArchivedAbsoluteIndex = U.field((Object)U.field(walMgr, "archiver"),
            "lastAbsArchivedIdx");

        FsyncModeFileWriteAheadLogManager.FileWriteHandle fhBefore = U.field(walMgr, "currentHnd");

        long idxBefore = fhBefore.getSegmentId();

        File[] files = archiveDir.listFiles(WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER);

        Arrays.sort(files);

        // Cleanup archive directory.
        for (File f : files) {
            if (f.delete())
                log.info("File " + f.getAbsolutePath() + " deleted");
        }

        Assert.assertEquals(0, archiveDir.listFiles().length);

        // Restart grid again after archive was removed.
        ig = startGrid(0);

        walMgr = ig.context().cache().context().wal();

        long afterLastArchivedAbsoluteIndex = U.field((Object)U.field(walMgr, "archiver"),
            "lastAbsArchivedIdx");

        int segments = ig.configuration().getDataStorageConfiguration().getWalSegments();

        Assert.assertTrue(
            "lastArchivedBeforeIdx=" + beforeLastArchivedAbsoluteIndex +
                ", lastArchivedAfterIdx=" + afterLastArchivedAbsoluteIndex + ",  segments=" + segments,
            afterLastArchivedAbsoluteIndex >=
                (beforeLastArchivedAbsoluteIndex - segments));

        ig.cluster().active(true);

        FsyncModeFileWriteAheadLogManager.FileWriteHandle fhAfter = U.field(walMgr, "currentHnd");

        Assert.assertNotNull(fhAfter);

        long idxAfter = fhAfter.getSegmentId();

        Assert.assertEquals(idxBefore, idxAfter);
        Assert.assertTrue(idxAfter >= beforeLastArchivedAbsoluteIndex);

        // Future for await all current available semgment will be archived.
        CountDownFuture awaitAchviedSegmentsLatch = new CountDownFuture(
            // One is a last archived, secod is a current write segment.
            (int)(idxAfter - afterLastArchivedAbsoluteIndex - 2)
        );

        log.info("currentIdx=" + idxAfter + ", lastArchivedBeforeIdx=" + beforeLastArchivedAbsoluteIndex +
            ", lastArchivedAfteridx=" + afterLastArchivedAbsoluteIndex + ",  segments=" + segments);

        ig.events().localListen(e -> {
            WalSegmentArchivedEvent archComplEvt = (WalSegmentArchivedEvent)e;

            log.info("EVT_WAL_SEGMENT_ARCHIVED:" + archComplEvt.getAbsWalSegmentIdx());

            if (archComplEvt.getAbsWalSegmentIdx() > afterLastArchivedAbsoluteIndex){
                awaitAchviedSegmentsLatch.onDone();

                return true;
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
