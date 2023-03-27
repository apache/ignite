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

package org.apache.ignite.util;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.cdc.AbstractCdcTest;
import org.apache.ignite.cdc.CdcConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.commandline.CommandList;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.cdc.AbstractCdcTest.KEYS_CNT;
import static org.apache.ignite.cdc.CdcSelfTest.addData;
import static org.apache.ignite.events.EventType.EVT_WAL_SEGMENT_ARCHIVED;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.commandline.cdc.CdcCommand.DELETE_LOST_SEGMENT_LINKS;
import static org.apache.ignite.internal.commandline.cdc.CdcCommand.NODE_ID;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * CDC command tests.
 */
public class CdcCommandTest extends GridCommandHandlerAbstractTest {
    /** */
    private IgniteEx srv0;

    /** */
    private IgniteEx srv1;

    /** */
    private DistributedChangeableProperty<Serializable> cdcDisabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setBackups(1));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalForceArchiveTimeout(1000)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setCdcEnabled(true)));

        cfg.setIncludeEventTypes(EVT_WAL_SEGMENT_ARCHIVED);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        srv0 = startGrid(0);
        srv1 = startGrid(1);

        cdcDisabled = srv0.context().distributedConfiguration().property(FileWriteAheadLogManager.CDC_DISABLED);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testParseDeleteLostSegmentLinks() {
        injectTestSystemOut();

        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS,
                CommandList.CDC.text(), "unexpected_command"),
            "Unexpected command: unexpected_command");

        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS,
                CommandList.CDC.text(), DELETE_LOST_SEGMENT_LINKS, NODE_ID),
            "Failed to parse " + NODE_ID + " command argument.");

        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS,
                CommandList.CDC.text(), DELETE_LOST_SEGMENT_LINKS, NODE_ID, "10"),
            "Failed to parse " + NODE_ID + " command argument.");
    }

    /** */
    @Test
    public void testDeleteLostSegmentLinksApplicationNotClosed() throws Exception {
        injectTestSystemOut();

        CountDownLatch appStarted = new CountDownLatch(1);

        CdcConfiguration cfg = new CdcConfiguration();

        cfg.setConsumer(new AbstractCdcTest.UserCdcConsumer() {
            @Override public void start(MetricRegistry mreg) {
                appStarted.countDown();
            }
        });

        CdcMain cdc = new CdcMain(getConfiguration(getTestIgniteInstanceName(0)), null, cfg);

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(cdc);

        appStarted.await(getTestTimeout(), TimeUnit.MILLISECONDS);

        assertContains(log, executeCommand(EXIT_CODE_UNEXPECTED_ERROR,
                CommandList.CDC.text(), DELETE_LOST_SEGMENT_LINKS, NODE_ID, srv0.localNode().id().toString()),
            "Failed to delete lost segment CDC links. Unable to acquire lock to lock CDC folder.");

        assertFalse(fut.isDone());

        fut.cancel();
    }

    /** */
    @Test
    public void testDeleteLostSegmentLinks() throws Exception {
        checkDeleteLostSegmentLinks(F.asList(0L, 2L), F.asList(2L), true);
    }

    /** */
    @Test
    public void testDeleteLostSegmentLinksOneNode() throws Exception {
        checkDeleteLostSegmentLinks(F.asList(0L, 2L), F.asList(2L), false);
    }

    /** */
    @Test
    public void testDeleteLostSegmentLinksMultipleGaps() throws Exception {
        checkDeleteLostSegmentLinks(F.asList(0L, 3L, 5L), F.asList(5L), true);
    }

    /** */
    private void checkDeleteLostSegmentLinks(List<Long> expBefore, List<Long> expAfter, boolean allNodes) throws Exception {
        archiveSegmentLinks(expBefore);

        checkLinks(srv0, expBefore);
        checkLinks(srv1, expBefore);

        String[] args = allNodes ? new String[] {CommandList.CDC.text(), DELETE_LOST_SEGMENT_LINKS} :
            new String[] {CommandList.CDC.text(), DELETE_LOST_SEGMENT_LINKS, NODE_ID, srv0.localNode().id().toString()};

        executeCommand(EXIT_CODE_OK, args);

        checkLinks(srv0, expAfter);
        checkLinks(srv1, allNodes ? expAfter : expBefore);
    }

    /** */
    private void checkLinks(IgniteEx srv, List<Long> expLinks) {
        FileWriteAheadLogManager wal0 = (FileWriteAheadLogManager)srv.context().cache().context().wal(true);

        File[] links = wal0.walCdcDirectory().listFiles(WAL_SEGMENT_FILE_FILTER);

        assertEquals(expLinks.size(), links.length);
        Arrays.stream(links).map(File::toPath).map(FileWriteAheadLogManager::segmentIndex)
            .allMatch(expLinks::contains);
    }

    /** Archive given segments links with possible gaps. */
    private void archiveSegmentLinks(List<Long> idxs) throws Exception {
        for (long idx = 0; idx <= idxs.stream().mapToLong(v -> v).max().getAsLong(); idx++) {
            cdcDisabled.propagate(!idxs.contains(idx));

            archiveSegment();
        }
    }

    /** */
    private void archiveSegment() throws Exception {
        CountDownLatch latch = new CountDownLatch(G.allGrids().size());

        for (Ignite srv : G.allGrids()) {
            srv.events().localListen(evt -> {
                latch.countDown();

                return false;
            }, EVT_WAL_SEGMENT_ARCHIVED);
        }

        addData(srv1.cache(DEFAULT_CACHE_NAME), 0, KEYS_CNT);

        latch.await(getTestTimeout(), TimeUnit.MILLISECONDS);
    }
}
