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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.EncryptionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.management.cache.CacheIdleVerifyCommand;
import org.apache.ignite.internal.processors.cache.GridCacheFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareFutureAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.logger.java.JavaLoggerFileHandler;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.jetbrains.annotations.Nullable;

import static java.lang.String.join;
import static java.lang.System.lineSeparator;
import static java.nio.file.Files.delete;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.Arrays.asList;
import static java.util.Objects.nonNull;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_CHECKPOINT_FREQ;
import static org.apache.ignite.configuration.EncryptionConfiguration.DFLT_REENCRYPTION_BATCH_SIZE;
import static org.apache.ignite.configuration.EncryptionConfiguration.DFLT_REENCRYPTION_RATE_MBPS;
import static org.apache.ignite.events.EventType.EVT_CONSISTENCY_VIOLATION;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.internal.encryption.AbstractEncryptionTest.KEYSTORE_PASSWORD;
import static org.apache.ignite.internal.encryption.AbstractEncryptionTest.KEYSTORE_PATH;
import static org.apache.ignite.internal.management.cache.VerifyBackupPartitionsDumpTask.IDLE_DUMP_FILE_PREFIX;

/**
 * Common abstract class for testing {@link CommandHandler}.
 * I advise you to look at the heirs classes:
 * {@link GridCommandHandlerClusterPerMethodAbstractTest}
 * {@link GridCommandHandlerClusterByClassAbstractTest}
 */
@WithSystemProperty(key = IGNITE_ENABLE_EXPERIMENTAL_COMMAND, value = "true")
public abstract class GridCommandHandlerAbstractTest extends GridCommandHandlerFactoryAbstractTest {
    /** */
    protected static final String CLIENT_NODE_NAME_PREFIX = "client";

    /** System out. */
    protected static PrintStream sysOut;

    /** System in. */
    private static InputStream sysIn;

    /**
     * Test out - can be injected via {@link #injectTestSystemOut()} instead of System.out and analyzed in test.
     * Will be as well passed as a handler output for an anonymous logger in the test.
     */
    protected static ByteArrayOutputStream testOut;

    /** Atomic configuration. */
    protected AtomicConfiguration atomicConfiguration;

    /** Additional data region configuration. */
    protected DataRegionConfiguration dataRegionConfiguration;

    /** Checkpoint frequency. */
    protected long checkpointFreq = DFLT_CHECKPOINT_FREQ;

    /** Enable automatic confirmation to avoid user interaction. */
    protected boolean autoConfirmation = true;

    /** {@code True} if encription is enabled. */
    protected boolean encryptionEnabled;

    /**  Re-encryption rate limit in megabytes per second. */
    protected double reencryptSpeed = DFLT_REENCRYPTION_RATE_MBPS;

    /** The number of pages that is scanned during re-encryption under checkpoint lock. */
    protected int reencryptBatchSize = DFLT_REENCRYPTION_BATCH_SIZE;

    /** Last operation result. */
    protected Object lastOperationResult;

    /** Persistence flag. */
    private boolean persistent = true;

    /** WAL compaction flag. */
    private boolean walCompaction;

    /** Listening logger. */
    protected ListeningTestLogger listeningLog;

    /**
     * Persistence setter.
     *
     * @param pr {@code True} If persistence enable.
     **/
    protected void persistenceEnable(boolean pr) {
        persistent = pr;
    }

    /**
     * WAL compaction setter.
     *
     * @param walCompaction {@code True} If WAL compaction enable.
     **/
    protected void walCompactionEnabled(boolean walCompaction) {
        this.walCompaction = walCompaction;
    }

    /**
     * Persistence getter.
     *
     * @return Persistence enable flag.
     */
    protected boolean persistenceEnable() {
        return persistent;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        testOut = new ByteArrayOutputStream(16 * 1024);
        sysOut = System.out;
        sysIn = System.in;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        File logDir = JavaLoggerFileHandler.logDirectory(U.defaultWorkDirectory());

        // Clean idle_verify log files.
        for (File f : logDir.listFiles(n -> n.getName().startsWith(CacheIdleVerifyCommand.IDLE_VERIFY_FILE_PREFIX)))
            U.delete(f);

        GridClientFactory.stopAll(false);

        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        log.info("Test output for " + currentTestMethod());
        log.info("----------------------------------------");

        System.setOut(sysOut);
        System.setIn(sysIn);

        log.info(testOut.toString());

        testOut.reset();

        encryptionEnabled = false;

        GridClientFactory.stopAll(false);
    }

    /** {@inheritDoc} */
    @Override public String getTestIgniteInstanceName() {
        return "gridCommandHandlerTest";
    }

    /**
     * @return Logger.
     */
    public static IgniteLogger createTestLogger() {
        JavaLogger logger = new JavaLogger(initLogger(null), false);

        logger.addConsoleAppender(true);

        return logger;
    }

    /**
     * Initialises JULs logger with basic settings
     * @param loggerName logger name. If {@code null} anonymous logger is returned.
     * @return logger
     */
    public static Logger initLogger(@Nullable String loggerName) {
        Logger result;

        if (loggerName == null)
            result = Logger.getAnonymousLogger();
        else
            result = Logger.getLogger(loggerName);

        result.setLevel(Level.INFO);
        result.setUseParentHandlers(false);

        return result;
    }

    /** */
    protected boolean sslEnabled() {
        return false;
    }

    /** */
    protected boolean idleVerifyRes(Path p) {
        return p.toFile().getName().startsWith(IDLE_DUMP_FILE_PREFIX);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (atomicConfiguration != null)
            cfg.setAtomicConfiguration(atomicConfiguration);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setConnectorConfiguration(new ConnectorConfiguration().setSslEnabled(sslEnabled()));

        if (sslEnabled())
            cfg.setSslContextFactory(sslFactory());

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setWalMode(WALMode.LOG_ONLY)
            .setWalCompactionEnabled(walCompaction)
            .setCheckpointFrequency(checkpointFreq)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(50L * 1024 * 1024).setPersistenceEnabled(persistent)
            );

        if (dataRegionConfiguration != null)
            dsCfg.setDataRegionConfigurations(dataRegionConfiguration);

        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setClientMode(igniteInstanceName.startsWith(CLIENT_NODE_NAME_PREFIX));

        cfg.setIncludeEventTypes(EVT_CONSISTENCY_VIOLATION); // Extend if necessary.

        if (encryptionEnabled) {
            KeystoreEncryptionSpi encSpi = new KeystoreEncryptionSpi();

            encSpi.setKeyStorePath(KEYSTORE_PATH);
            encSpi.setKeyStorePassword(KEYSTORE_PASSWORD.toCharArray());

            cfg.setEncryptionSpi(encSpi);

            EncryptionConfiguration encCfg = new EncryptionConfiguration();

            encCfg.setReencryptionRateLimit(reencryptSpeed);
            encCfg.setReencryptionBatchSize(reencryptBatchSize);

            dsCfg.setEncryptionConfiguration(encCfg);
        }

        if (listeningLog != null)
            cfg.setGridLogger(listeningLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void cleanPersistenceDir() throws Exception {
        super.cleanPersistenceDir();

        try (DirectoryStream<Path> files = newDirectoryStream(Paths.get(U.defaultWorkDirectory()), this::idleVerifyRes)) {
            for (Path path : files)
                delete(path);
        }
    }

    /**
     * Before command executed {@link #testOut} reset.
     *
     * @param args Arguments.
     * @return Result of execution.
     */
    protected int execute(String... args) {
        return execute(new ArrayList<>(asList(args)));
    }

    /**
     * Executes command and checks its exit code.
     *
     * @param expExitCode Expected exit code.
     * @param args Command lines arguments.
     * @return Result of command execution.
     */
    protected String executeCommand(int expExitCode, String... args) {
        int res = execute(args);

        assertEquals(expExitCode, res);

        return testOut.toString();
    }

    /**
     * Before command executed {@link #testOut} reset.
     *
     * @param args Arguments.
     * @return Result of execution
     */
    protected int execute(List<String> args) {
        return execute(newCommandHandler(createTestLogger()), args);
    }

    /**
     * Before command executed {@link #testOut} reset.
     *
     * @param hnd Handler.
     * @param args Arguments.
     * @return Result of execution
     */
    protected int execute(TestCommandHandler hnd, String... args) {
        return execute(hnd, new ArrayList<>(asList(args)));
    }

    /**
     * Before command executed {@link #testOut} reset.
     */
    protected int execute(TestCommandHandler hnd, List<String> args) {
        if (!F.isEmpty(args) && !"--help".equalsIgnoreCase(args.get(0)))
            addExtraArguments(args);

        testOut.reset();

        int exitCode = hnd.execute(args);
        lastOperationResult = hnd.getLastOperationResult();

        // Flush all Logger handlers to make log data available to test.
        hnd.flushLogger();

        return exitCode;
    }

    /**
     * Adds extra arguments required for tests.
     *
     * @param args Incoming arguments;
     */
    protected void addExtraArguments(List<String> args) {
        if (autoConfirmation)
            args.add(CMD_AUTO_CONFIRMATION);

        if (sslEnabled()) {
            // We shouldn't add extra args for --cache help.
            if (args.size() < 2 || !args.get(0).equals("--cache") || !args.get(1).equals("help"))
                extendSslParams(args);
        }
    }

    /** Custom SSL params. */
    protected void extendSslParams(List<String> params) {
        params.add("--keystore");
        params.add(GridTestUtils.keyStorePath("node01"));
        params.add("--keystore-password");
        params.add(GridTestUtils.keyStorePassword());
    }

    /** Custom SSL factory. */
    protected Factory<SSLContext> sslFactory() {
        return GridTestUtils.sslFactory();
    }

    /** */
    protected void injectTestSystemOut() {
        System.setOut(new PrintStream(testOut));
    }

    /**
     * Emulates user input.
     *
     * @param inputStrings User input strings.
     * */
    protected void injectTestSystemIn(String... inputStrings) {
        assert nonNull(inputStrings);

        String inputStr = join(lineSeparator(), inputStrings);
        System.setIn(new ByteArrayInputStream(inputStr.getBytes()));
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
                        && ((GridNearTxPrepareFutureAdapter)fut).tx().system())
                        continue;

                    if (fut instanceof GridDhtTxPrepareFuture
                        && ((GridDhtTxPrepareFuture)fut).tx().system())
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

    /**
     * Creates default cache and preload some data entries.
     * <br/>
     * <table class="doctable">
     * <th>Cache parameter</th>
     * <th>Value</th>
     * <tr>
     *     <td>Name</td>
     *     <td>{@link #DEFAULT_CACHE_NAME}</td>
     * </tr>
     * <tr>
     *     <td>Affinity</td>
     *     <td>{@link RendezvousAffinityFunction} with exclNeighbors = false, parts = 32</td>
     * </tr>
     * <tr>
     *     <td>Number of backup</td>
     *     <td>1</td>
     * </tr>
     *
     * </table>
     *
     * @param ignite Ignite.
     * @param cacheName Cache name.
     * @param countEntries Count of entries.
     * @param partitions Partitions count.
     * @param filter Node filter.
     */
    protected void createCacheAndPreload(
        Ignite ignite,
        String cacheName,
        int countEntries,
        int partitions,
        @Nullable IgnitePredicate<ClusterNode> filter
    ) {
        assert nonNull(ignite);

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(cacheName)
            .setAffinity(new RendezvousAffinityFunction(false, partitions))
            .setBackups(1)
            .setEncryptionEnabled(encryptionEnabled);

        if (filter != null)
            ccfg.setNodeFilter(filter);

        ignite.createCache(ccfg);

        try (IgniteDataStreamer<Object, Object> streamer = ignite.dataStreamer(cacheName)) {
            for (int i = 0; i < countEntries; i++)
                streamer.addData(i, i);
        }
    }

    /**
     * Creates default cache and preload some data entries.
     *
     * @param ignite Ignite.
     * @param countEntries Count of entries.
     */
    protected void createCacheAndPreload(Ignite ignite, int countEntries) {
        createCacheAndPreload(ignite, DEFAULT_CACHE_NAME, countEntries, 32, null);
    }
}
