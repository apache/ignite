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

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.cache.CacheCommand;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheFuture;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.tx.VisorTxInfo;
import org.apache.ignite.internal.visor.tx.VisorTxTaskResult;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.jetbrains.annotations.NotNull;

import static java.nio.file.Files.delete;
import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.processors.cache.verify.VerifyBackupPartitionsDumpTask.IDLE_DUMP_FILE_PREMIX;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * Command line handler test.
 */
public class GridCommandHandlerTest extends GridCommonAbstractTest {
    /** System out. */
    protected PrintStream sysOut;

    /** Test out - can be injected via {@link #injectTestSystemOut()} instead of System.out and analyzed in test. */
    protected ByteArrayOutputStream testOut;

    /** Option is used for auto confirmation. */
    private static final String CMD_AUTO_CONFIRMATION = "--yes";

    /**
     * @return Folder in work directory.
     * @throws IgniteCheckedException If failed to resolve folder name.
     */
    protected File folder(String folder) throws IgniteCheckedException {
        return U.resolveWorkDirectory(U.defaultWorkDirectory(), folder, false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        System.setProperty(IGNITE_ENABLE_EXPERIMENTAL_COMMAND, "true");

        cleanPersistenceDir();

        stopAllGrids();

        sysOut = System.out;

        testOut = new ByteArrayOutputStream(128 * 1024);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        //delete idle-verify dump files.
        try (DirectoryStream<Path> files = newDirectoryStream(
            Paths.get(U.defaultWorkDirectory()),
            entry -> entry.toFile().getName().startsWith(IDLE_DUMP_FILE_PREMIX)
        )) {
            for (Path path : files)
                delete(path);
        }

        System.clearProperty(IGNITE_ENABLE_EXPERIMENTAL_COMMAND);

        System.setOut(sysOut);

        if (testOut != null)
            System.out.println(testOut.toString());
    }

    /**
     *
     */
    protected void injectTestSystemOut() {
        System.setOut(new PrintStream(testOut));
    }

    /** {@inheritDoc} */
    @Override public String getTestIgniteInstanceName() {
        return "bltTest";
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        DataStorageConfiguration memCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(50L * 1024 * 1024));

        cfg.setDataStorageConfiguration(memCfg);

        DataStorageConfiguration dsCfg = cfg.getDataStorageConfiguration();
        dsCfg.setWalMode(WALMode.LOG_ONLY);
        dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setClientMode(igniteInstanceName.startsWith("client"));

        return cfg;
    }

    /**
     * Test activation works via control.sh
     *
     * @throws Exception If failed.
     */
    public void testActivate() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        assertEquals(EXIT_CODE_OK, execute("--activate"));

        assertTrue(ignite.cluster().active());
    }

    /**
     * @param args Arguments.
     * @return Result of execution.
     */
    protected int execute(String... args) {
        return execute(new ArrayList<>(Arrays.asList(args)));
    }

    /**
     * @param args Arguments.
     * @return Result of execution
     */
    protected int execute(ArrayList<String> args) {
        // Add force to avoid interactive confirmation
        args.add(CMD_AUTO_CONFIRMATION);

        return new CommandHandler().execute(args);
    }

    /**
     * @param hnd Handler.
     * @param args Arguments.
     * @return Result of execution
     */
    protected int execute(CommandHandler hnd, ArrayList<String> args) {
        // Add force to avoid interactive confirmation
        args.add(CMD_AUTO_CONFIRMATION);

        return hnd.execute(args);
    }

    /**
     * @param hnd Handler.
     * @param args Arguments.
     * @return Result of execution
     */
    protected int execute(CommandHandler hnd, String... args) {
        ArrayList<String> args0 = new ArrayList<>(Arrays.asList(args));

        // Add force to avoid interactive confirmation
        args0.add(CMD_AUTO_CONFIRMATION);

        return hnd.execute(args0);
    }

    /**
     * Test deactivation works via control.sh
     *
     * @throws Exception If failed.
     */
    public void testDeactivate() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        assertTrue(ignite.cluster().active());

        assertEquals(EXIT_CODE_OK, execute("--deactivate"));

        assertFalse(ignite.cluster().active());
    }

    /**
     * Test cluster active state works via control.sh
     *
     * @throws Exception If failed.
     */
    public void testState() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        assertEquals(EXIT_CODE_OK, execute("--state"));

        ignite.cluster().active(true);

        assertEquals(EXIT_CODE_OK, execute("--state"));
    }

    /**
     * Test baseline collect works via control.sh
     *
     * @throws Exception If failed.
     */
    public void testBaselineCollect() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        assertEquals(EXIT_CODE_OK, execute("--baseline"));

        assertEquals(1, ignite.cluster().currentBaselineTopology().size());
    }

    /**
     * @param ignites Ignites.
     * @return Local node consistent ID.
     */
    private String consistentIds(Ignite... ignites) {
        String res = "";

        for (Ignite ignite : ignites) {
            String consistentId = ignite.cluster().localNode().consistentId().toString();

            if (!F.isEmpty(res))
                res += ", ";

            res += consistentId;
        }

        return res;
    }

    /**
     * Test baseline add items works via control.sh
     *
     * @throws Exception If failed.
     */
    public void testBaselineAdd() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        Ignite other = startGrid(2);

        assertEquals(EXIT_CODE_OK, execute("--baseline", "add", consistentIds(other)));
        assertEquals(EXIT_CODE_OK, execute("--baseline", "add", consistentIds(other)));

        assertEquals(2, ignite.cluster().currentBaselineTopology().size());
    }

    /**
     * Test baseline remove works via control.sh
     *
     * @throws Exception If failed.
     */
    public void testBaselineRemove() throws Exception {
        Ignite ignite = startGrids(1);
        Ignite other = startGrid("nodeToStop");

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        String offlineNodeConsId = consistentIds(other);

        stopGrid("nodeToStop");

        assertEquals(EXIT_CODE_OK, execute("--baseline"));
        assertEquals(EXIT_CODE_OK, execute("--baseline", "remove", offlineNodeConsId));

        assertEquals(1, ignite.cluster().currentBaselineTopology().size());
    }

    /**
     * Test baseline set works via control.sh
     *
     * @throws Exception If failed.
     */
    public void testBaselineSet() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        Ignite other = startGrid(2);

        assertEquals(EXIT_CODE_OK, execute("--baseline", "set", consistentIds(ignite, other)));

        assertEquals(2, ignite.cluster().currentBaselineTopology().size());

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--baseline", "set", "invalidConsistentId"));
    }

    /**
     * Test baseline set by topology version works via control.sh
     *
     * @throws Exception If failed.
     */
    public void testBaselineVersion() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        startGrid(2);

        assertEquals(EXIT_CODE_OK, execute("--baseline"));

        assertEquals(EXIT_CODE_OK, execute("--baseline", "version", String.valueOf(ignite.cluster().topologyVersion())));

        assertEquals(2, ignite.cluster().currentBaselineTopology().size());
    }

    /**
     * Test active transactions.
     *
     * @throws Exception If failed.
     */
    public void testActiveTransactions() throws Exception {
        Ignite ignite = startGridsMultiThreaded(2);

        ignite.cluster().active(true);

        Ignite client = startGrid("client");

        client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL).setWriteSynchronizationMode(FULL_SYNC));

        for (Ignite ig : G.allGrids())
            assertNotNull(ig.cache(DEFAULT_CACHE_NAME));

        CountDownLatch lockLatch = new CountDownLatch(1);
        CountDownLatch unlockLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = startTransactions(lockLatch, unlockLatch);

        U.awaitQuiet(lockLatch);

        doSleep(5000);

        CommandHandler h = new CommandHandler();

        final VisorTxInfo[] toKill = {null};

        // Basic test.
        validate(h, map -> {
            ClusterNode node = grid(0).cluster().localNode();

            VisorTxTaskResult res = map.get(node);

            for (VisorTxInfo info : res.getInfos()) {
                if (info.getSize() == 100) {
                    toKill[0] = info; // Store for further use.

                    break;
                }
            }

            assertEquals(3, map.size());
        }, "--tx");

        assertNotNull(toKill[0]);

        // Test filter by label.
        validate(h, map -> {
            ClusterNode node = grid(0).cluster().localNode();

            for (Map.Entry<ClusterNode, VisorTxTaskResult> entry : map.entrySet())
                assertEquals(entry.getKey().equals(node) ? 1 : 0, entry.getValue().getInfos().size());
        }, "--tx", "label", "label1");

        // Test filter by label regex.
        validate(h, map -> {
            ClusterNode node1 = grid(0).cluster().localNode();
            ClusterNode node2 = grid("client").cluster().localNode();

            for (Map.Entry<ClusterNode, VisorTxTaskResult> entry : map.entrySet()) {
                if (entry.getKey().equals(node1)) {
                    assertEquals(1, entry.getValue().getInfos().size());

                    assertEquals("label1", entry.getValue().getInfos().get(0).getLabel());
                }
                else if (entry.getKey().equals(node2)) {
                    assertEquals(1, entry.getValue().getInfos().size());

                    assertEquals("label2", entry.getValue().getInfos().get(0).getLabel());
                }
                else
                    assertTrue(entry.getValue().getInfos().isEmpty());

            }
        }, "--tx", "label", "^label[0-9]");

        // Test filter by empty label.
        validate(h, map -> {
            VisorTxTaskResult res = map.get(grid(0).localNode());

            for (VisorTxInfo info : res.getInfos())
                assertNull(info.getLabel());

        }, "--tx", "label", "null");

        // test check minSize
        int minSize = 10;

        validate(h, map -> {
            VisorTxTaskResult res = map.get(grid(0).localNode());

            assertNotNull(res);

            for (VisorTxInfo txInfo : res.getInfos())
                assertTrue(txInfo.getSize() >= minSize);
        }, "--tx", "minSize", Integer.toString(minSize));

        // test order by size.
        validate(h, map -> {
            VisorTxTaskResult res = map.get(grid(0).localNode());

            assertTrue(res.getInfos().get(0).getSize() >= res.getInfos().get(1).getSize());
        }, "--tx", "order", "SIZE");

        // test order by duration.
        validate(h, map -> {
            VisorTxTaskResult res = map.get(grid(0).localNode());

            assertTrue(res.getInfos().get(0).getDuration() >= res.getInfos().get(1).getDuration());
        }, "--tx", "order", "DURATION");

        // test order by start_time.
        validate(h, map -> {
            VisorTxTaskResult res = map.get(grid(0).localNode());

            for (int i = res.getInfos().size() - 1; i > 1; i--)
                assertTrue(res.getInfos().get(i - 1).getStartTime() >= res.getInfos().get(i).getStartTime());
        }, "--tx", "order", CommandHandler.CMD_TX_ORDER_START_TIME);

        // Trigger topology change and test connection.
        IgniteInternalFuture<?> startFut = multithreadedAsync(() -> {
            try {
                startGrid(2);
            }
            catch (Exception e) {
                fail();
            }
        }, 1, "start-node-thread");

        doSleep(5000); // Give enough time to reach exchange future.

        assertEquals(EXIT_CODE_OK, execute(h, "--tx"));

        // Test kill by xid.
        validate(h, map -> {
                assertEquals(1, map.size());

                Map.Entry<ClusterNode, VisorTxTaskResult> killedEntry = map.entrySet().iterator().next();

                VisorTxInfo info = killedEntry.getValue().getInfos().get(0);

                assertEquals(toKill[0].getXid(), info.getXid());
            }, "--tx", "kill",
            "xid", toKill[0].getXid().toString(), // Use saved on first run value.
            "nodes", grid(0).localNode().consistentId().toString());

        unlockLatch.countDown();

        startFut.get();

        fut.get();

        awaitPartitionMapExchange();

        checkFutures();
    }

    /**
     *
     */
    public void testKillHangingLocalTransactions() throws Exception {
        Ignite ignite = startGridsMultiThreaded(2);

        ignite.cluster().active(true);

        Ignite client = startGrid("client");

        client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME).
            setAtomicityMode(TRANSACTIONAL).
            setWriteSynchronizationMode(FULL_SYNC).
            setAffinity(new RendezvousAffinityFunction(false, 64)));

        Ignite prim = primaryNode(0L, DEFAULT_CACHE_NAME);

        // Blocks lock response to near node.
        TestRecordingCommunicationSpi.spi(prim).blockMessages(GridNearLockResponse.class, client.name());

        TestRecordingCommunicationSpi.spi(client).blockMessages(GridNearTxFinishRequest.class, prim.name());

        GridNearTxLocal clientTx = null;

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED, 2000, 1)) {
            clientTx = ((TransactionProxyImpl)tx).tx();

            client.cache(DEFAULT_CACHE_NAME).put(0L, 0L);

            fail();
        }
        catch (Exception e) {
            assertTrue(X.hasCause(e, TransactionTimeoutException.class));
        }

        assertNotNull(clientTx);

        IgniteEx primEx = (IgniteEx)prim;

        IgniteInternalTx tx0 = primEx.context().cache().context().tm().activeTransactions().iterator().next();

        assertNotNull(tx0);

        CommandHandler h = new CommandHandler();

        validate(h, map -> {
            ClusterNode node = grid(0).cluster().localNode();

            VisorTxTaskResult res = map.get(node);

            for (VisorTxInfo info : res.getInfos())
                assertEquals(tx0.xid(), info.getXid());

            assertEquals(1, map.size());
        }, "--tx", "kill");

        tx0.finishFuture().get();

        TestRecordingCommunicationSpi.spi(prim).stopBlock();

        TestRecordingCommunicationSpi.spi(client).stopBlock();

        IgniteInternalFuture<?> nearFinFut = U.field(clientTx, "finishFut");

        nearFinFut.get();

        checkFutures();
    }

    /**
     * Simulate uncommitted backup transactions and test rolling back using utility.
     */
    public void testKillHangingRemoteTransactions() throws Exception {
        final int cnt = 3;

        startGridsMultiThreaded(cnt);

        Ignite[] clients = new Ignite[] {
            startGrid("client1"),
            startGrid("client2"),
            startGrid("client3"),
            startGrid("client4")
        };

        clients[0].getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME).
            setBackups(2).
            setAtomicityMode(TRANSACTIONAL).
            setWriteSynchronizationMode(FULL_SYNC).
            setAffinity(new RendezvousAffinityFunction(false, 64)));

        for (Ignite client : clients) {
            assertTrue(client.configuration().isClientMode());

            assertNotNull(client.cache(DEFAULT_CACHE_NAME));
        }

        LongAdder progress = new LongAdder();

        AtomicInteger idx = new AtomicInteger();

        int tc = clients.length;

        CountDownLatch lockLatch = new CountDownLatch(1);
        CountDownLatch commitLatch = new CountDownLatch(1);

        Ignite prim = primaryNode(0L, DEFAULT_CACHE_NAME);

        TestRecordingCommunicationSpi primSpi = TestRecordingCommunicationSpi.spi(prim);

        primSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message message) {
                return message instanceof GridDhtTxFinishRequest;
            }
        });

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                int id = idx.getAndIncrement();

                Ignite client = clients[id];

                try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED, 0, 1)) {
                    IgniteCache<Long, Long> cache = client.cache(DEFAULT_CACHE_NAME);

                    if (id != 0)
                        U.awaitQuiet(lockLatch);

                    cache.invoke(0L, new IncrementClosure(), null);

                    if (id == 0) {
                        lockLatch.countDown();

                        U.awaitQuiet(commitLatch);

                        doSleep(500); // Wait until candidates will enqueue.
                    }

                    tx.commit();
                }
                catch (Exception e) {
                    assertTrue(X.hasCause(e, TransactionTimeoutException.class));
                }

                progress.increment();

            }
        }, tc, "invoke-thread");

        U.awaitQuiet(lockLatch);

        commitLatch.countDown();

        primSpi.waitForBlocked(clients.length);

        // Unblock only finish messages from clients from 2 to 4.
        primSpi.stopBlock(true, new IgnitePredicate<T2<ClusterNode, GridIoMessage>>() {
            @Override public boolean apply(T2<ClusterNode, GridIoMessage> objects) {
                GridIoMessage iom = objects.get2();

                Message m = iom.message();

                if (m instanceof GridDhtTxFinishRequest) {
                    GridDhtTxFinishRequest r = (GridDhtTxFinishRequest)m;

                    if (r.nearNodeId().equals(clients[0].cluster().localNode().id()))
                        return false;
                }

                return true;
            }
        });

        // Wait until queue is stable
        for (Ignite ignite : G.allGrids()) {
            if (ignite.configuration().isClientMode())
                continue;

            Collection<IgniteInternalTx> txs = ((IgniteEx)ignite).context().cache().context().tm().activeTransactions();

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    for (IgniteInternalTx tx : txs)
                        if (!tx.local()) {
                            IgniteTxEntry entry = tx.writeEntries().iterator().next();

                            GridCacheEntryEx cached = entry.cached();

                            Collection<GridCacheMvccCandidate> candidates = cached.remoteMvccSnapshot();

                            if (candidates.size() != clients.length)
                                return false;
                        }

                    return true;
                }
            }, 10_000);
        }

        CommandHandler h = new CommandHandler();

        // Check listing.
        validate(h, map -> {
            for (int i = 0; i < cnt; i++) {
                IgniteEx grid = grid(i);

                // Skip primary.
                if (grid.localNode().id().equals(prim.cluster().localNode().id()))
                    continue;

                VisorTxTaskResult res = map.get(grid.localNode());

                // Validate queue length on backups.
                assertEquals(clients.length, res.getInfos().size());
            }
        }, "--tx");

        // Check kill.
        validate(h, map -> {
            // No-op.
        }, "--tx", "kill");

        // Wait for all remote txs to finish.
        for (Ignite ignite : G.allGrids()) {
            if (ignite.configuration().isClientMode())
                continue;

            Collection<IgniteInternalTx> txs = ((IgniteEx)ignite).context().cache().context().tm().activeTransactions();

            for (IgniteInternalTx tx : txs)
                if (!tx.local())
                    tx.finishFuture().get();
        }

        // Unblock finish message from client1.
        primSpi.stopBlock(true);

        fut.get();

        Long cur = (Long)clients[0].cache(DEFAULT_CACHE_NAME).get(0L);

        assertEquals(tc - 1, cur.longValue());

        checkFutures();
    }

    /**
     * Test baseline add items works via control.sh
     *
     * @throws Exception If failed.
     */
    public void testBaselineAddOnNotActiveCluster() throws Exception {
        Ignite ignite = startGrid(1);

        assertFalse(ignite.cluster().active());

        String consistentIDs = getTestIgniteInstanceName(1);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--baseline", "add", consistentIDs));

        assertTrue(testOut.toString().contains("Changing BaselineTopology on inactive cluster is not allowed."));

        consistentIDs =
            getTestIgniteInstanceName(1) + ", " +
                getTestIgniteInstanceName(2) + "," +
                getTestIgniteInstanceName(3);

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--baseline", "add", consistentIDs));

        assertTrue(testOut.toString().contains("Node not found for consistent ID: bltTest2"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheHelp() throws Exception {
        Ignite ignite = startGrids(1);

        ignite.cluster().active(true);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "help"));

        for (CacheCommand cmd : CacheCommand.values()) {
            if (cmd != CacheCommand.HELP)
                assertTrue(cmd.text(), testOut.toString().contains(cmd.text()));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheIdleVerify() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().active(true);

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME));

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        assertTrue(testOut.toString().contains("no conflicts have been found"));

        HashSet<Integer> clearKeys = new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6));

        ((IgniteEx)ignite).context().cache().cache(DEFAULT_CACHE_NAME).clearLocallyAll(clearKeys, true, true, true);

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        assertTrue(testOut.toString().contains("conflict partitions"));
    }

    /**
     * Tests that both update counter and hash conflicts are detected.
     *
     * @throws Exception If failed.
     */
    public void testCacheIdleVerifyTwoConflictTypes() throws Exception {
        IgniteEx ignite = (IgniteEx)startGrids(2);

        ignite.cluster().active(true);

        int parts = 32;

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME));

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        assertTrue(testOut.toString().contains("no conflicts have been found"));

        GridCacheContext<Object, Object> cacheCtx = ignite.cachex(DEFAULT_CACHE_NAME).context();

        corruptDataEntry(cacheCtx, 1, true, false);

        corruptDataEntry(cacheCtx, 1 + parts / 2, false, true);

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        assertTrue(testOut.toString().contains("found 2 conflict partitions"));
    }

    /**
     * Tests that idle verify print partitions info.
     *
     * @throws Exception If failed.
     */
    public void testCacheIdleVerifyDump() throws Exception {
        IgniteEx ignite = (IgniteEx)startGrids(3);

        ignite.cluster().active(true);

        int parts = 32;

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME));

        ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME + "other"));

        injectTestSystemOut();

        int keysCount = 20;//less than parts number for ability to check skipZeros flag.

        for (int i = 0; i < keysCount; i++)
            cache.put(i, i);

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump", DEFAULT_CACHE_NAME));

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump", "--skipZeros", DEFAULT_CACHE_NAME));

        Matcher fileNameMatcher = dumpFileNameMatcher();

        if (fileNameMatcher.find()) {
            String dumpWithZeros = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

            assertTrue(dumpWithZeros.contains("idle_verify check has finished, found " + parts + " partitions"));
            assertTrue(dumpWithZeros.contains("Partition: PartitionKeyV2 [grpId=1544803905, grpName=default, partId=0]"));
            assertTrue(dumpWithZeros.contains("updateCntr=0, size=0, partHash=0"));
            assertTrue(dumpWithZeros.contains("no conflicts have been found"));

            assertSort(parts, dumpWithZeros);
        }

        if (fileNameMatcher.find()) {
            String dumpWithoutZeros = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

            assertTrue(dumpWithoutZeros.contains("idle_verify check has finished, found " + keysCount + " partitions"));
            assertTrue(dumpWithoutZeros.contains((parts - keysCount) + " partitions was skipped"));
            assertTrue(dumpWithoutZeros.contains("Partition: PartitionKeyV2 [grpId=1544803905, grpName=default, partId="));

            assertFalse(dumpWithoutZeros.contains("updateCntr=0, size=0, partHash=0"));

            assertTrue(dumpWithoutZeros.contains("no conflicts have been found"));

            assertSort(keysCount, dumpWithoutZeros);
        }
        else
            fail("Should be found both files");
    }

    /**
     * Checking sorting of partitions.
     *
     * @param expectedPartsCount Expected parts count.
     * @param output Output.
     */
    private void assertSort(int expectedPartsCount, String output) {
        Pattern partIdPattern = Pattern.compile(".*partId=([0-9]*)");
        Pattern primaryPattern = Pattern.compile("Partition instances: \\[PartitionHashRecordV2 \\[isPrimary=true");

        Matcher partIdMatcher = partIdPattern.matcher(output);
        Matcher primaryMatcher = primaryPattern.matcher(output);

        int i = 0;

        while (partIdMatcher.find()) {
            assertEquals(i++, Integer.parseInt(partIdMatcher.group(1)));
            assertTrue(primaryMatcher.find());//primary node should be first in every line
        }

        assertEquals(expectedPartsCount, i);
    }

    /**
     * Tests that idle verify print partitions info.
     *
     * @throws Exception If failed.
     */
    public void testCacheIdleVerifyDumpForCorruptedData() throws Exception {
        IgniteEx ignite = (IgniteEx)startGrids(3);

        ignite.cluster().active(true);

        int parts = 32;

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME));

        injectTestSystemOut();

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        GridCacheContext<Object, Object> cacheCtx = ignite.cachex(DEFAULT_CACHE_NAME).context();

        corruptDataEntry(cacheCtx, 0, true, false);

        corruptDataEntry(cacheCtx, 0 + parts / 2, false, true);

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump"));

        Matcher fileNameMatcher = dumpFileNameMatcher();

        if (fileNameMatcher.find()) {
            String dumpWithConflicts = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

            assertTrue(dumpWithConflicts.contains("found 2 conflict partitions: [counterConflicts=1, hashConflicts=1]"));
        }else
            fail("Should be found dump with conflicts");
    }

    /**
     * @return Build matcher for dump file name.
     */
    @NotNull private Matcher dumpFileNameMatcher() {
        Pattern fileNamePattern = Pattern.compile(".*VisorIdleVerifyDumpTask successfully written output to '(.*)'");

        return fileNamePattern.matcher(testOut.toString());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheIdleVerifyMovingParts() throws Exception {
        IgniteEx ignite = (IgniteEx)startGrids(2);

        ignite.cluster().active(true);

        int parts = 32;

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME)
            .setRebalanceDelay(10_000));

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        assertTrue(testOut.toString().contains("no conflicts have been found"));

        startGrid(2);

        resetBaselineTopology();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        assertTrue(testOut.toString().contains("MOVING partitions"));
    }

    /**
     *
     */
    public void testCacheContention() throws Exception {
        int cnt = 10;

        final ExecutorService svc = Executors.newFixedThreadPool(cnt);

        try {
            Ignite ignite = startGrids(2);

            ignite.cluster().active(true);

            final IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
                .setAffinity(new RendezvousAffinityFunction(false, 32))
                .setAtomicityMode(TRANSACTIONAL)
                .setBackups(1)
                .setName(DEFAULT_CACHE_NAME));

            final CountDownLatch l = new CountDownLatch(1);

            final CountDownLatch l2 = new CountDownLatch(1);

            svc.submit(new Runnable() {
                @Override public void run() {
                    try (final Transaction tx = ignite.transactions().txStart()) {
                        cache.put(0, 0);

                        l.countDown();

                        U.awaitQuiet(l2);

                        tx.commit();
                    }
                }
            });

            for (int i = 0; i < cnt - 1; i++) {
                svc.submit(new Runnable() {
                    @Override public void run() {
                        U.awaitQuiet(l);

                        try (final Transaction tx = ignite.transactions().txStart()) {
                            cache.get(0);

                            tx.commit();
                        }
                    }
                });
            }

            U.awaitQuiet(l);

            Thread.sleep(300);

            injectTestSystemOut();

            assertEquals(EXIT_CODE_OK, execute("--cache", "contention", "5"));

            l2.countDown();

            assertTrue(testOut.toString().contains("TxEntry"));
            assertTrue(testOut.toString().contains("op=READ"));
            assertTrue(testOut.toString().contains("op=CREATE"));
            assertTrue(testOut.toString().contains("id=" + ignite(0).cluster().localNode().id()));
            assertTrue(testOut.toString().contains("id=" + ignite(1).cluster().localNode().id()));
        }
        finally {
            svc.shutdown();
            svc.awaitTermination(100, TimeUnit.DAYS);
        }
    }

    /**
     *
     */
    public void testCacheSequence() throws Exception {
        Ignite ignite = startGrid();

        ignite.cluster().active(true);

        Ignite client = startGrid("client");

        final IgniteAtomicSequence seq1 = client.atomicSequence("testSeq", 1, true);
        seq1.get();

        final IgniteAtomicSequence seq2 = client.atomicSequence("testSeq2", 10, true);
        seq2.get();

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "list", "testSeq.*", "seq"));

        assertTrue(testOut.toString().contains("testSeq"));
        assertTrue(testOut.toString().contains("testSeq2"));
    }

    /**
     *
     */
    public void testCacheGroups() throws Exception {
        Ignite ignite = startGrid();

        ignite.cluster().active(true);

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1)
            .setGroupName("G100")
            .setName(DEFAULT_CACHE_NAME));

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "list", ".*", "groups"));

        assertTrue(testOut.toString().contains("G100"));
    }

    /**
     *
     */
    public void testCacheAffinity() throws Exception {
        Ignite ignite = startGrid();

        ignite.cluster().active(true);

        IgniteCache<Object, Object> cache1 = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME));

        for (int i = 0; i < 100; i++)
            cache1.put(i, i);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "list", ".*"));

        assertTrue(testOut.toString().contains("cacheName=" + DEFAULT_CACHE_NAME));
        assertTrue(testOut.toString().contains("prim=32"));
        assertTrue(testOut.toString().contains("mapped=32"));
        assertTrue(testOut.toString().contains("affCls=RendezvousAffinityFunction"));
    }

    /**
     * @param h Handler.
     * @param validateClo Validate clo.
     * @param args Args.
     */
    private void validate(CommandHandler h, IgniteInClosure<Map<ClusterNode, VisorTxTaskResult>> validateClo,
        String... args) {
        assertEquals(EXIT_CODE_OK, execute(h, args));

        validateClo.apply(h.getLastOperationResult());
    }

    /**
     * @param from From.
     * @param cnt Count.
     */
    private Map<Object, Object> generate(int from, int cnt) {
        Map<Object, Object> map = new TreeMap<>();

        for (int i = 0; i < cnt; i++)
            map.put(i + from, i + from);

        return map;
    }

    /**
     * Test execution of --wal print command.
     *
     * @throws Exception if failed.
     */
    public void testUnusedWalPrint() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().active(true);

        List<String> nodes = new ArrayList<>(2);

        for (ClusterNode node : ignite.cluster().forServers().nodes())
            nodes.add(node.consistentId().toString());

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--wal", "print"));

        for (String id : nodes)
            assertTrue(testOut.toString().contains(id));

        assertTrue(!testOut.toString().contains("error"));

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--wal", "print", nodes.get(0)));

        assertTrue(!testOut.toString().contains(nodes.get(1)));

        assertTrue(!testOut.toString().contains("error"));
    }

    /**
     * Test execution of --wal delete command.
     *
     * @throws Exception if failed.
     */
    public void testUnusedWalDelete() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().active(true);

        List<String> nodes = new ArrayList<>(2);

        for (ClusterNode node : ignite.cluster().forServers().nodes())
            nodes.add(node.consistentId().toString());

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--wal", "delete"));

        for (String id : nodes)
            assertTrue(testOut.toString().contains(id));

        assertTrue(!testOut.toString().contains("error"));

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--wal", "delete", nodes.get(0)));

        assertTrue(!testOut.toString().contains(nodes.get(1)));

        assertTrue(!testOut.toString().contains("error"));
    }

    /**
     * @param lockLatch Lock latch.
     * @param unlockLatch Unlock latch.
     */
    private IgniteInternalFuture<?> startTransactions(CountDownLatch lockLatch,
        CountDownLatch unlockLatch) throws Exception {
        IgniteEx client = grid("client");

        AtomicInteger idx = new AtomicInteger();

        return multithreadedAsync(new Runnable() {
            @Override public void run() {
                int id = idx.getAndIncrement();

                switch (id) {
                    case 0:
                        try (Transaction tx = grid(0).transactions().txStart()) {
                            grid(0).cache(DEFAULT_CACHE_NAME).putAll(generate(0, 100));

                            lockLatch.countDown();

                            U.awaitQuiet(unlockLatch);

                            tx.commit();

                            fail("Commit must fail");
                        }
                        catch (Exception e) {
                            // No-op.
                            assertTrue(X.hasCause(e, TransactionRollbackException.class));
                        }

                        break;
                    case 1:
                        U.awaitQuiet(lockLatch);

                        doSleep(3000);

                        try (Transaction tx = grid(0).transactions().withLabel("label1").txStart(PESSIMISTIC, READ_COMMITTED, Integer.MAX_VALUE, 0)) {
                            grid(0).cache(DEFAULT_CACHE_NAME).putAll(generate(200, 110));

                            grid(0).cache(DEFAULT_CACHE_NAME).put(0, 0);
                        }

                        break;
                    case 2:
                        try (Transaction tx = grid(1).transactions().txStart()) {
                            U.awaitQuiet(lockLatch);

                            grid(1).cache(DEFAULT_CACHE_NAME).put(0, 0);
                        }

                        break;
                    case 3:
                        try (Transaction tx = client.transactions().withLabel("label2").txStart(OPTIMISTIC, READ_COMMITTED, 0, 0)) {
                            U.awaitQuiet(lockLatch);

                            client.cache(DEFAULT_CACHE_NAME).putAll(generate(100, 10));

                            client.cache(DEFAULT_CACHE_NAME).put(0, 0);

                            tx.commit();
                        }

                        break;
                }
            }
        }, 4, "tx-thread");
    }

    /** */
    private static class IncrementClosure implements EntryProcessor<Long, Long, Void> {
        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Long, Long> entry,
            Object... arguments) throws EntryProcessorException {
            entry.setValue(entry.exists() ? entry.getValue() + 1 : 0);

            return null;
        }
    }

    /**
     * Corrupts data entry.
     *
     * @param ctx Context.
     * @param key Key.
     * @param breakCntr Break counter.
     * @param breakData Break data.
     */
    private void corruptDataEntry(
        GridCacheContext<Object, Object> ctx,
        int key,
        boolean breakCntr,
        boolean breakData
    ) {
        int partId = ctx.affinity().partition(key);

        try {
            long updateCntr = ctx.topology().localPartition(partId).updateCounter();

            Object valToPut = ctx.cache().keepBinary().get(key);

            if (breakCntr)
                updateCntr++;

            if (breakData)
                valToPut = valToPut.toString() + " broken";

            // Create data entry
            DataEntry dataEntry = new DataEntry(
                ctx.cacheId(),
                new KeyCacheObjectImpl(key, null, partId),
                new CacheObjectImpl(valToPut, null),
                GridCacheOperation.UPDATE,
                new GridCacheVersion(),
                new GridCacheVersion(),
                0L,
                partId,
                updateCntr
            );

            GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)ctx.shared().database();

            db.checkpointReadLock();

            try {
                U.invoke(GridCacheDatabaseSharedManager.class, db, "applyUpdate", ctx, dataEntry);
            }
            finally {
                db.checkpointReadUnlock();
            }
        }
        catch (IgniteCheckedException e) {
            e.printStackTrace();
        }
    }
}
