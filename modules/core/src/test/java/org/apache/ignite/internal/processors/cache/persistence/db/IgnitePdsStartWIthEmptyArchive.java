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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.WalSegmentArchivedEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.aware.SegmentAware;
import org.apache.ignite.internal.processors.cache.persistence.wal.filehandle.FileWriteHandle;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_WAL_SEGMENT_ARCHIVED;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER;

/**
 *
 */
@Ignore("https://issues.apache.org/jira/browse/IGNITE-11908")
public class IgnitePdsStartWIthEmptyArchive extends GridCommonAbstractTest {
    /** Mapping of WAL segment idx to WalSegmentArchivedEvent. */
    private final Map<Long, WalSegmentArchivedEvent> evts = new ConcurrentHashMap<>();

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
                .setMaxWalArchiveSize(Long.MAX_VALUE)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        Map<IgnitePredicate<? extends Event>, int[]> lsnrs = new HashMap<>();

        lsnrs.put((e) -> {
            WalSegmentArchivedEvent archComplEvt = (WalSegmentArchivedEvent)e;

            log.info("EVT_WAL_SEGMENT_ARCHIVED: " + archComplEvt.getAbsWalSegmentIdx());

            evts.put(archComplEvt.getAbsWalSegmentIdx(), archComplEvt);

            return true;
        }, new int[] {EVT_WAL_SEGMENT_ARCHIVED});

        cfg.setLocalEventListeners(lsnrs);

        return cfg;
    }

    /**
     * Executes initial steps before test execution.
     * @throws Exception If failed.
     */
    @Before
    public void before() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Stops all nodes and cleans work dir after a test.
     */
    @After
    public void cleanup() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        FileWriteAheadLogManager walMgr = (FileWriteAheadLogManager)ig.context().cache().context().wal();

        // Populate data for generate WAL archive segments.
        try (IgniteDataStreamer<Integer, byte[]> st = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
            int entries = 1000;

            for (int i = 0; i < entries; i++)
                st.addData(i, new byte[1024 * 1024]);
        }

        File archiveDir = U.field(walMgr, "walArchiveDir");

        stopGrid(0, false);

        SegmentAware beforeSaw = U.field(walMgr, "segmentAware");

        long beforeLastArchivedAbsoluteIdx = beforeSaw.lastArchivedAbsoluteIndex();

        FileWriteHandle fhBefore = U.field(walMgr, "currHnd");

        long idxBefore = fhBefore.getSegmentId();

        File[] files = archiveDir.listFiles(WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER);

        Arrays.sort(files);

        // Cleanup archive directory.
        for (File f : files) {
            if (f.delete())
                log.info("File " + f.getAbsolutePath() + " deleted");
        }

        Assert.assertEquals(0, archiveDir.listFiles().length);

        evts.clear();

        // Restart grid again after archive was removed.
        ig = startGrid(0);

        walMgr = (FileWriteAheadLogManager)ig.context().cache().context().wal();

        SegmentAware afterSaw = U.field(walMgr, "segmentAware");

        long afterLastArchivedAbsoluteIndex = afterSaw.lastArchivedAbsoluteIndex();

        int segments = ig.configuration().getDataStorageConfiguration().getWalSegments();

        Assert.assertTrue(
            "lastArchivedBeforeIdx=" + beforeLastArchivedAbsoluteIdx +
                ", lastArchivedAfterIdx=" + afterLastArchivedAbsoluteIndex + ",  segments=" + segments,
            afterLastArchivedAbsoluteIndex >=
            (beforeLastArchivedAbsoluteIdx - segments));

        ig.cluster().active(true);

        FileWriteHandle fhAfter = U.field(walMgr, "currHnd");

        Assert.assertNotNull(fhAfter);

        long idxAfter = fhAfter.getSegmentId();

        Assert.assertEquals(idxBefore, idxAfter);
        Assert.assertTrue(idxAfter >= beforeLastArchivedAbsoluteIdx);

        log.info("currentIdx=" + idxAfter + ", lastArchivedBeforeIdx=" + beforeLastArchivedAbsoluteIdx +
            ", lastArchivedAfteridx=" + afterLastArchivedAbsoluteIndex + ",  segments=" + segments);

        // One is a last archived, secod is a current write segment.
        final long awaitAchviedSegments = idxAfter - afterLastArchivedAbsoluteIndex - 2;

        // Await all current available semgment will be archived.
        assertTrue(GridTestUtils.waitForCondition(
            new GridAbsPredicate() {
                @Override public boolean apply() {
                    long cut = evts.keySet().stream().filter(e -> e > afterLastArchivedAbsoluteIndex).count();

                    return cut >= awaitAchviedSegments;
                }
            }, 10_000));
    }
}
