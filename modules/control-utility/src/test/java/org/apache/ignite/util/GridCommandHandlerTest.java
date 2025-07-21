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
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.OfflineCommand;
import org.apache.ignite.internal.management.cache.FindAndDeleteGarbageInPersistenceTaskResult;
import org.apache.ignite.internal.processors.cache.persistence.db.IgniteCacheGroupsWithRestartsTest;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.DataStreamerUpdatesHandler;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotPartitionsQuickVerifyHandler;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.io.File.separatorChar;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_COMPLETED_WITH_WARNINGS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.DEFAULT_TARGET_FOLDER;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.util.TestCommandsProvider.registerCommands;
import static org.apache.ignite.util.TestCommandsProvider.unregisterAll;

/**
 * Command line handler test.
 * You can use this class if you need create nodes for each test.
 * If you not necessary create nodes for each test you can try use {@link GridCommandHandlerClusterByClassTest}
 */
@RunWith(Parameterized.class)
public class GridCommandHandlerTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** Partitioned cache name. */
    protected static final String PARTITIONED_CACHE_NAME = "part_cache";

    /** Replicated cache name. */
    protected static final String REPLICATED_CACHE_NAME = "repl_cache";

    /** */
    public static final String CACHE_GROUP_KEY_IDS = "cache_key_ids";

    /** */
    public static final String CHANGE_CACHE_GROUP_KEY = "change_cache_key";

    /** */
    public static final String REENCRYPTION_RATE = "reencryption_rate_limit";

    /** */
    public static final String REENCRYPTION_RESUME = "resume_reencryption";

    /** */
    public static final String REENCRYPTION_STATUS = "reencryption_status";

    /** */
    public static final String REENCRYPTION_SUSPEND = "suspend_reencryption";

    /** */
    protected static File defaultDiagnosticDir;

    /** */
    protected static File customDiagnosticDir;

    /** */
    protected ListeningTestLogger listeningLog;

    /** */
    @Parameterized.Parameter(1)
    public int cnt;

    /** */
    @Parameterized.Parameters(name = "cmdHnd={0}, cnt={1}")
    public static List<Object[]> data() {
        List<String> hnds = commandHandlers();

        List<Object[]> params = new ArrayList<>();

        for (String hnd : hnds) {
            for (int cnt = 0; cnt < 100; cnt++)
                params.add(new Object[]{ hnd, cnt });
        }

        return params;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        initDiagnosticDir();

        cleanDiagnosticDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        listeningLog = null;
    }

    /** {@inheritDoc} */
    @Override protected void cleanPersistenceDir() throws Exception {
        super.cleanPersistenceDir();

        cleanDiagnosticDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (listeningLog != null)
            cfg.setGridLogger(listeningLog);

        return cfg;
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    protected void initDiagnosticDir() throws IgniteCheckedException {
        defaultDiagnosticDir = new File(U.defaultWorkDirectory()
            + separatorChar + DEFAULT_TARGET_FOLDER + separatorChar);

        customDiagnosticDir = new File(U.defaultWorkDirectory()
            + separatorChar + "diagnostic_test_dir" + separatorChar);
    }

    /**
     * Clean diagnostic directories.
     */
    protected void cleanDiagnosticDir() {
        U.delete(defaultDiagnosticDir);
        U.delete(customDiagnosticDir);
    }

    /**
     * Test that 'not OK' status of snapshot operation is set if the operation produces a warning.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClusterCreateSnapshotWarning() throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0));
        cfg.getConnectorConfiguration().setHost("localhost");
        cfg.getClientConnectorConfiguration().setHost("localhost");

        IgniteEx ig = startGrid(cfg);

        cfg = getConfiguration(getTestIgniteInstanceName(1));
        cfg.getConnectorConfiguration().setHost("localhost");
        cfg.getClientConnectorConfiguration().setHost("localhost");

        startGrid(cfg);

        ig.cluster().state(ACTIVE);
        createCacheAndPreload(ig, 100);

        TestRecordingCommunicationSpi cm = (TestRecordingCommunicationSpi)grid(0).configuration().getCommunicationSpi();

        cm.blockMessages(DataStreamerRequest.class, grid(1).name());

        AtomicBoolean stopLoading = new AtomicBoolean();

        IgniteInternalFuture<?> loadFut = runAsync(() -> {
            try (IgniteDataStreamer<Integer, Integer> ds = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
                int i = 100;

                while (!stopLoading.get()) {
                    ds.addData(i, i);

                    i++;
                }
            }
        });

        cm.waitForBlocked(IgniteDataStreamer.DFLT_PARALLEL_OPS_MULTIPLIER);

        try {
            injectTestSystemOut();

            String snpName = "testDsSnp";

            int code = execute(new ArrayList<>(F.asList("--snapshot", "create", snpName, "--sync")));

            assertEquals(EXIT_CODE_COMPLETED_WITH_WARNINGS, code);

            String out = testOut.toString();

            assertNotContains(log, out, "Failed to perform operation");

            assertContains(log, out, "Snapshot create operation completed with warnings [name=" + snpName);

            boolean dataStmrDetected = out.contains(DataStreamerUpdatesHandler.WRN_MSG);

            String expWarn = dataStmrDetected
                ? DataStreamerUpdatesHandler.WRN_MSG
                : String.format("Cache partitions differ for cache groups [%s]. ", CU.cacheId(DEFAULT_CACHE_NAME))
                + SnapshotPartitionsQuickVerifyHandler.WRN_MSG;

            assertContains(log, out, expWarn);

            code = execute(new ArrayList<>(F.asList("--snapshot", "check", snpName)));

            assertEquals(EXIT_CODE_OK, code);

            out = testOut.toString();

            assertContains(log, out, expWarn);

            assertContains(log, out, dataStmrDetected
                ? "The check procedure has failed, conflict partitions has been found"
                : "The check procedure has finished, no conflicts have been found");
        }
        finally {
            stopLoading.set(true);
            cm.stopBlock();
            loadFut.get();
        }
    }

    /** @throws Exception If failed. */
    @Test
    @WithSystemProperty(key = IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP, value = "true")
    public void testCleaningGarbageAfterCacheDestroyedAndNodeStop_ControlConsoleUtil() throws Exception {
        new IgniteCacheGroupsWithRestartsTest().testFindAndDeleteGarbage(this::executeTaskViaControlConsoleUtil);
    }

    /** */
    @Test
    public void testOfflineCommand() throws Exception {
        try {
            registerCommands(new OfflineTestCommand());

            startGrid(0);

            injectTestSystemOut();

            String input = "Test Offline Command";

            assertEquals(EXIT_CODE_OK, execute("--offline-test", "--input", input));

            assertTrue(testOut.toString().contains(input));
        }
        finally {
            unregisterAll();
        }
    }

    /**
     * @param ignite Ignite to execute task on.
     * @param delFoundGarbage If clearing mode should be used.
     * @return Result of task run.
     */
    private FindAndDeleteGarbageInPersistenceTaskResult executeTaskViaControlConsoleUtil(
        IgniteEx ignite,
        boolean delFoundGarbage
    ) {
        TestCommandHandler hnd = newCommandHandler();

        List<String> args = new ArrayList<>(Arrays.asList("--yes", "--port", connectorPort(ignite),
            "--cache", "find_garbage", ignite.localNode().id().toString()));

        if (delFoundGarbage)
            args.add("--delete");

        hnd.execute(args);

        return hnd.getLastOperationResult();
    }

    /**
     * @param str String.
     * @param substr Substring to find in the specified string.
     * @return The number of substrings found in the specified string.
     */
    private int countSubstrs(String str, String substr) {
        int cnt = 0;

        for (int off = 0; (off = str.indexOf(substr, off)) != -1; off++)
            ++cnt;

        return cnt;
    }

    /** Test IO factory that slows down file creation. */
    private static class SlowDownFileIoFactory implements FileIOFactory {
        /** Delegated factory. */
        private final FileIOFactory delegate;

        /** Max slowdown interval. */
        private final long maxTimeout;

        /** Latch to notify when the first file will be created. */
        private final CountDownLatch ioStartLatch;

        /** Next file slowdown interval. */
        private long timeout = 10;

        /**
         * @param delegate Delegated factory.
         * @param maxTimeout Max slowdown interval.
         * @param ioStartLatch Latch to notify when the first file will be created.
         */
        private SlowDownFileIoFactory(FileIOFactory delegate, long maxTimeout, CountDownLatch ioStartLatch) {
            this.delegate = delegate;
            this.maxTimeout = maxTimeout;
            this.ioStartLatch = ioStartLatch;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            try {
                if (ioStartLatch.getCount() > 0)
                    ioStartLatch.countDown();

                long currTimeout = maxTimeout;

                synchronized (this) {
                    if (timeout < maxTimeout) {
                        currTimeout = timeout;

                        timeout += timeout;
                    }
                }

                U.sleep(currTimeout);

                return delegate.create(file, modes);
            }
            catch (IgniteInterruptedCheckedException e) {
                Thread.currentThread().interrupt();

                throw new RuntimeException(e);
            }
        }
    }

    /**
     * @param state Current state of the cluster.
     * @param logOutput Logger output where current cluster state is supposed to be specified.
     */
    public static void assertClusterState(ClusterState state, String logOutput) {
        assertTrue(Pattern.compile("Cluster state: " + state + "\\s+").matcher(logOutput).find());
    }

    /** */
    public static class OfflineTestCommand implements OfflineCommand<OfflineTestCommandArg, Void> {
        /** {@inheritDoc} */
        @Override public String description() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Class<OfflineTestCommandArg> argClass() {
            return OfflineTestCommandArg.class;
        }

        /** {@inheritDoc} */
        @Override public Void execute(OfflineTestCommandArg arg, Consumer<String> printer) {
            printer.accept(arg.input());

            return null;
        }
    }

    /** */
    public static class OfflineTestCommandArg extends IgniteDataTransferObject {
        /** */
        @Argument
        private String input;

        /** */
        public String input() {
            return input;
        }

        /** */
        public void input(String input) {
            this.input = input;
        }

        /** {@inheritDoc} */
        @Override protected void writeExternalData(ObjectOutput out) throws IOException {
            U.writeString(out, input);
        }

        /** {@inheritDoc} */
        @Override protected void readExternalData(ObjectInput in) throws IOException {
            input = U.readString(in);
        }
    }
}
