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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.UnaryOperator;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.maintenance.DefragmentationParameters;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.maintenance.MaintenanceTask;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;

/** */
public class GridCommandHandlerDefragmentationTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** */
    private static CountDownLatch blockCdl;

    /** */
    private static CountDownLatch waitCdl;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getDataStorageConfiguration().setWalSegmentSize(512 * 1024).setWalSegments(3);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDefragmentationSchedule() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().state(ACTIVE);

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--defragmentation", "schedule"));

        String grid0ConsId = grid(0).configuration().getConsistentId().toString();
        String grid1ConsId = grid(1).configuration().getConsistentId().toString();

        ListeningTestLogger testLog = new ListeningTestLogger();

        TestCommandHandler cmd = createCommandHandler(testLog);

        LogListener logLsnr = LogListener.matches("Scheduling completed successfully.").build();

        testLog.registerListener(logLsnr);

        assertEquals(EXIT_CODE_OK, execute(
            cmd,
            "--defragmentation",
            "schedule",
            "--nodes",
            grid0ConsId
        ));

        assertTrue(logLsnr.check());

        MaintenanceTask mntcTask = DefragmentationParameters.toStore(Collections.emptyList());

        assertNotNull(grid(0).context().maintenanceRegistry().registerMaintenanceTask(mntcTask));
        assertNull(grid(1).context().maintenanceRegistry().registerMaintenanceTask(mntcTask));

        stopGrid(0);
        startGrid(0);

        logLsnr = LogListener.matches("Node is already in Maintenance Mode").build();

        testLog.clearListeners();

        testLog.registerListener(logLsnr);

        assertEquals(EXIT_CODE_OK, execute(
            cmd,
            "--defragmentation",
            "schedule",
            "--nodes",
            grid0ConsId
        ));

        assertTrue(logLsnr.check());

        stopGrid(0);
        startGrid(0);

        stopGrid(1);
        startGrid(1);

        stopAllGrids();

        startGrids(2);

        logLsnr = LogListener.matches("Scheduling completed successfully.").times(2).build();

        testLog.clearListeners();

        testLog.registerListener(logLsnr);

        assertEquals(EXIT_CODE_OK, execute(
            cmd,
            "--defragmentation",
            "schedule",
            "--nodes",
            String.join(",", grid0ConsId, grid1ConsId)
        ));

        assertTrue(logLsnr.check());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDefragmentationCancel() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().state(ACTIVE);

        String grid0ConsId = grid(0).configuration().getConsistentId().toString();

        ListeningTestLogger testLog = new ListeningTestLogger();

        TestCommandHandler cmd = createCommandHandler(testLog);

        assertEquals(EXIT_CODE_OK, execute(
            cmd,
            "--defragmentation",
            "schedule",
            "--nodes",
            grid0ConsId
        ));

        LogListener logLsnr = LogListener.matches("Scheduled defragmentation task cancelled successfully.").atLeast(1).build();

        testLog.registerListener(logLsnr);

        assertEquals(EXIT_CODE_OK, execute(
            cmd,
            "--port",
            connectorPort(grid(0)),
            "--defragmentation",
            "cancel"
        ));

        assertTrue(logLsnr.check());

        testLog.clearListeners();

        logLsnr = LogListener.matches("Scheduled defragmentation task is not found.").build();

        testLog.registerListener(logLsnr);

        assertEquals(EXIT_CODE_OK, execute(
            cmd,
            "--port",
            connectorPort(grid(1)),
            "--defragmentation",
            "cancel"
        ));

        assertTrue(logLsnr.check());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDefragmentationCancelInProgress() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = ig.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 1024; i++)
            cache.put(i, i);

        forceCheckpoint(ig);

        String grid0ConsId = ig.configuration().getConsistentId().toString();

        ListeningTestLogger testLog = new ListeningTestLogger();

        TestCommandHandler cmd = createCommandHandler(testLog);

        assertEquals(EXIT_CODE_OK, execute(
            cmd,
            "--defragmentation",
            "schedule",
            "--nodes",
            grid0ConsId
        ));

        String port = connectorPort(grid(0));

        stopGrid(0);

        blockCdl = new CountDownLatch(128);

        UnaryOperator<IgniteConfiguration> cfgOp = cfg -> {
            DataStorageConfiguration dsCfg = cfg.getDataStorageConfiguration();

            FileIOFactory delegate = dsCfg.getFileIOFactory();

            dsCfg.setFileIOFactory((file, modes) -> {
                if (file.getName().contains("dfrg")) {
                    if (blockCdl.getCount() == 0) {
                        try {
                            // Slow down defragmentation process.
                            // This'll be enough for the test since we have, like, 900 partitions left.
                            Thread.sleep(100);
                        }
                        catch (InterruptedException ignore) {
                            // No-op.
                        }
                    }
                    else
                        blockCdl.countDown();
                }

                return delegate.create(file, modes);
            });

            return cfg;
        };

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            try {
                startGrid(0, cfgOp);
            }
            catch (Exception e) {
                // No-op.
                throw new RuntimeException(e);
            }
        });

        blockCdl.await();

        LogListener logLsnr = LogListener.matches("Defragmentation cancelled successfully.").build();

        testLog.registerListener(logLsnr);

        assertEquals(EXIT_CODE_OK, execute(
            cmd,
            "--port",
            port,
            "--defragmentation",
            "cancel"
        ));

        assertTrue(logLsnr.check());

        fut.get();

        testLog.clearListeners();

        logLsnr = LogListener.matches("Defragmentation is already completed or has been cancelled previously.").build();

        testLog.registerListener(logLsnr);

        assertEquals(EXIT_CODE_OK, execute(
            cmd,
            "--port",
            port,
            "--defragmentation",
            "cancel"
        ));

        assertTrue(logLsnr.check());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDefragmentationStatus() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().state(ClusterState.ACTIVE);

        ig.getOrCreateCache(DEFAULT_CACHE_NAME + "1");

        IgniteCache<Object, Object> cache = ig.getOrCreateCache(DEFAULT_CACHE_NAME + "2");

        ig.getOrCreateCache(DEFAULT_CACHE_NAME + "3");

        for (int i = 0; i < 1024; i++)
            cache.put(i, i);

        forceCheckpoint(ig);

        String grid0ConsId = ig.configuration().getConsistentId().toString();

        ListeningTestLogger testLog = new ListeningTestLogger();

        TestCommandHandler cmd = createCommandHandler(testLog);

        assertEquals(EXIT_CODE_OK, execute(
            cmd,
            "--defragmentation",
            "schedule",
            "--nodes",
            grid0ConsId
        ));

        String port = connectorPort(grid(0));

        stopGrid(0);

        blockCdl = new CountDownLatch(128);
        waitCdl = new CountDownLatch(1);

        UnaryOperator<IgniteConfiguration> cfgOp = cfg -> {
            DataStorageConfiguration dsCfg = cfg.getDataStorageConfiguration();

            FileIOFactory delegate = dsCfg.getFileIOFactory();

            dsCfg.setFileIOFactory((file, modes) -> {
                if (file.getName().contains("dfrg")) {
                    if (blockCdl.getCount() == 0) {
                        try {
                            waitCdl.await();
                        }
                        catch (InterruptedException ignore) {
                            // No-op.
                        }
                    }
                    else
                        blockCdl.countDown();
                }

                return delegate.create(file, modes);
            });

            return cfg;
        };

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            try {
                startGrid(0, cfgOp);
            }
            catch (Exception e) {
                // No-op.
                throw new RuntimeException(e);
            }
        });

        blockCdl.await();

        List<LogListener> logLsnrs = Arrays.asList(
            LogListener.matches("default1 - size before/after: 0MB/0MB").build(),
            LogListener.matches("default2 - partitions processed/all:").build(),
            LogListener.matches("Awaiting defragmentation: default3").build()
        );

        for (LogListener logLsnr : logLsnrs)
            testLog.registerListener(logLsnr);

        assertEquals(EXIT_CODE_OK, execute(
            cmd,
            "--port",
            port,
            "--defragmentation",
            "status"
        ));

        waitCdl.countDown();

        for (LogListener logLsnr : logLsnrs)
            assertTrue(logLsnr.check());

        fut.get();

        ((GridCacheDatabaseSharedManager)grid(0).context().cache().context().database())
            .defragmentationManager()
            .completionFuture()
            .get();

        testLog.clearListeners();

        logLsnrs = Arrays.asList(
            LogListener.matches("default1 - size before/after: 0MB/0MB").build(),
            LogListener.matches(Pattern.compile("default2 - size before/after: (\\S+)/\\1")).build(),
            LogListener.matches("default3 - size before/after: 0MB/0MB").build()
        );

        for (LogListener logLsnr : logLsnrs)
            testLog.registerListener(logLsnr);

        assertEquals(EXIT_CODE_OK, execute(
            cmd,
            "--port",
            port,
            "--defragmentation",
            "status"
        ));

        for (LogListener logLsnr : logLsnrs)
            assertTrue(logLsnr.check());
    }

    /** */
    private TestCommandHandler createCommandHandler(ListeningTestLogger testLog) {
        Logger log = GridCommandHandlerAbstractTest.initLogger(null);

        log.addHandler(new StreamHandler(System.out, new Formatter() {
            /** {@inheritDoc} */
            @Override public String format(LogRecord record) {
                String msg = record.getMessage();

                testLog.info(msg);

                return msg + "\n";
            }
        }));

        return newCommandHandler(new JavaLogger(log, false));
    }
}
