/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.processors.cache.GridCacheFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareFutureAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.nio.file.Files.delete;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.Arrays.asList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.internal.processors.cache.verify.VerifyBackupPartitionsDumpTask.IDLE_DUMP_FILE_PREFIX;

/**
 *
 */
public class GridCommandHandlerAbstractTest extends GridCommonAbstractTest {
    /** Option is used for auto confirmation. */
    protected static final String CMD_AUTO_CONFIRMATION = "--yes";

    /** System out. */
    protected PrintStream sysOut;

    /**
     * Test out - can be injected via {@link #injectTestSystemOut()} instead of System.out and analyzed in test.
     * Will be as well passed as a handler output for an anonymous logger in the test.
     */
    protected ByteArrayOutputStream testOut;

    /** Atomic configuration. */
    protected AtomicConfiguration atomicConfiguration;

    /** Additional data region configuration. */
    protected DataRegionConfiguration dataRegionConfiguration;

    /** Checkpoint frequency. */
    protected long checkpointFreq;

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        GridTestUtils.cleanIdleVerifyLogFiles();

        System.clearProperty(IGNITE_BASELINE_AUTO_ADJUST_ENABLED);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(IGNITE_BASELINE_AUTO_ADJUST_ENABLED, "false");

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        System.setProperty(IGNITE_ENABLE_EXPERIMENTAL_COMMAND, "true");

        cleanPersistenceDir();

        stopAllGrids();

        sysOut = System.out;

        testOut = new ByteArrayOutputStream(16 * 1024);

        checkpointFreq = DataStorageConfiguration.DFLT_CHECKPOINT_FREQ;
    }

    /**
     * @return True if system out was already injected on test initialization.
     */
    protected boolean isSystemOutAlreadyInjected() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        log.info("Test output for " + currentTestMethod());
        log.info("----------------------------------------");

        System.setOut(sysOut);

        if (testOut != null)
            System.out.println(testOut.toString());

        testOut = null;

        stopAllGrids();

        cleanPersistenceDir();

        // Delete idle-verify dump files.
        try (DirectoryStream<Path> files = newDirectoryStream(Paths.get(U.defaultWorkDirectory()), this::idleVerifyRes)) {
            for (Path path : files)
                delete(path);
        }

        System.clearProperty(IGNITE_ENABLE_EXPERIMENTAL_COMMAND);
    }

    /**
     * @return Logger.
     */
    private Logger createTestLogger() {
        Logger log = CommandHandler.initLogger(null);

        // Adding logging to console.
        log.addHandler(CommandHandler.setupStreamHandler());

        return log;
    }

    /** */
    private boolean idleVerifyRes(Path p) {
        return p.toFile().getName().startsWith(IDLE_DUMP_FILE_PREFIX);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (atomicConfiguration != null)
            cfg.setAtomicConfiguration(atomicConfiguration);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setCheckpointFrequency(checkpointFreq)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setMaxSize(50L * 1024 * 1024));

        if (dataRegionConfiguration != null)
            memCfg.setDataRegionConfigurations(dataRegionConfiguration);

        cfg.setDataStorageConfiguration(memCfg);

        DataStorageConfiguration dsCfg = cfg.getDataStorageConfiguration();
        dsCfg.setWalMode(WALMode.LOG_ONLY);
        dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setClientMode(igniteInstanceName.startsWith("client"));

        return cfg;
    }

    /**
     * @param args Arguments.
     * @return Result of execution.
     */
    protected int execute(String... args) {
        return execute(new ArrayList<>(asList(args)));
    }

    /**
     * @param args Arguments.
     * @return Result of execution
     */
    protected int execute(List<String> args) {
        return execute(new CommandHandler(createTestLogger()), args);
    }

    /**
     * @param hnd Handler.
     * @param args Arguments.
     * @return Result of execution
     */
    protected int execute(CommandHandler hnd, String... args) {
        return execute(hnd, new ArrayList<>(asList(args)));
    }

    /** */
    protected int execute(CommandHandler hnd, List<String> args) {
        if (!F.isEmpty(args) && !"--help".equalsIgnoreCase(args.get(0)))
            addExtraArguments(args);

        int exitCode = hnd.execute(args);

        // Flush all Logger handlers to make log data available to test.
        Logger logger = U.field(hnd, "logger");
        Arrays.stream(logger.getHandlers()).forEach(Handler::flush);

        return exitCode;
    }

    /**
     * Adds extra arguments required for tests.
     *
     * @param args Incoming arguments;
     */
    protected void addExtraArguments(List<String> args) {
        // Add force to avoid interactive confirmation.
        args.add(CMD_AUTO_CONFIRMATION);
    }

    /** */
    protected void injectTestSystemOut() {
        System.setOut(new PrintStream(testOut));
    }

    /**
     * Checks if all non-system txs and non-system mvcc futures are finished.
     */
    protected void checkUserFutures() {
        for (Ignite ignite : G.allGrids()) {
            IgniteEx ig = (IgniteEx)ignite;

            final Collection<GridCacheFuture<?>> futs = ig.context().cache().context().mvcc().activeFutures();

            boolean hasFutures = false;

            for (GridCacheFuture<?> fut : futs) {
                if (!fut.isDone()) {
                    //skipping system tx futures if possible
                    if (fut instanceof GridNearTxPrepareFutureAdapter
                        && ((GridNearTxPrepareFutureAdapter) fut).tx().system())
                        continue;

                    if (fut instanceof GridDhtTxPrepareFuture
                        && ((GridDhtTxPrepareFuture) fut).tx().system())
                        continue;

                    log.error("Expecting no active future [node=" + ig.localNode().id() + ", fut=" + fut + ']');

                    hasFutures = true;
                }
            }

            if (hasFutures)
                fail("Some mvcc futures are not finished");

            Collection<IgniteInternalTx> txs = ig.context().cache().context().tm().activeTransactions()
                .stream()
                .filter(tx -> !tx.system())
                .collect(Collectors.toSet());

            for (IgniteInternalTx tx : txs)
                log.error("Expecting no active transaction [node=" + ig.localNode().id() + ", tx=" + tx + ']');

            if (!txs.isEmpty())
                fail("Some transaction are not finished");
        }
    }
}
