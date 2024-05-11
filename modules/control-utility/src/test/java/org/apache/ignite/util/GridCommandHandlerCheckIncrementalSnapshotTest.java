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

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.management.cache.IdleVerifyResultV2;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;

/** */
@RunWith(Parameterized.class)
public class GridCommandHandlerCheckIncrementalSnapshotTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** */
    private static final String CACHE = "testCache";

    /** */
    private static final String SNP = "testSnapshot";

    /** */
    private TestCommandHandler cmd;

    /** */
    private volatile IgniteBiPredicate<Object, TxRecord> skipTxRec;

    /** */
    private volatile IgniteBiPredicate<Object, DataRecord> skipDataRec;

    /** Count of server nodes to start. */
    @Parameterized.Parameter(1)
    public int nodesCnt;

    /** Count of primary nodes participated in a transaction. */
    @Parameterized.Parameter(2)
    public int txPrimNodesCnt;

    /** Count of primary nodes participated in a transaction. */
    @Parameterized.Parameter(3)
    public int backupsCnt;

    /** */
    @Parameterized.Parameters(name = "cmdHnd={0},nodesCnt={1},primNodesCnt={2},backupsCnt={3}")
    public static List<Object[]> params() {
        return F.asList(
            new Object[]{2, 1, 1},
            new Object[]{2, 2, 1},
            new Object[]{3, 1, 1},
            new Object[]{3, 1, 2}
        ).stream().flatMap(row -> commandHandlers().stream().map(invoker -> {
            Object[] res = new Object[row.length + 1];

            res[0] = invoker;

            System.arraycopy(row, 0, res, 1, row.length);

            return res;
        })).collect(Collectors.toList());
    }


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        skipTxRec = null;

        walCompactionEnabled(true);

        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setPluginProviders(new TransactionFilterWALPluginProvider());

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setCacheConfiguration(new CacheConfiguration<>(CACHE)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(backupsCnt));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        IgniteEx ig = startGrids(nodesCnt);

        ig.cluster().state(ACTIVE);

        ig.snapshot().createSnapshot(SNP).get(getTestTimeout());

        injectTestSystemOut();

        cmd = newCommandHandler();
    }

    /** */
    @Test
    public void testDifferentBaselineTopology() throws Exception {
        grid(0).snapshot().createIncrementalSnapshot(SNP).get(getTestTimeout());

        String consId = (String)grid(1).configuration().getConsistentId();

        stopGrid(1);

        awaitPartitionMapExchange();

        assertEquals(EXIT_CODE_OK, execute(cmd, "--baseline", "remove", consId));

        startGrid(1);

        assertEquals(EXIT_CODE_OK, execute(cmd, "--snapshot", "check", SNP, "--increment", "1"));
        assertContains(log, testOut.toString(), "Topologies of snapshot and current cluster are different");
    }

    /** */
    @Test
    public void testWrongCommandParams() {
        grid(0).snapshot().createIncrementalSnapshot(SNP).get(getTestTimeout());

        autoConfirmation = false;

        // Missed increment index.
        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute(cmd, "--snapshot", "check", SNP, "--increment"));
        assertContains(log, testOut.toString(), "Please specify a value for argument: --increment");

        // Wrong params.
        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute(cmd, "--snapshot", "check", SNP, "--increment", "wrong"));
        assertContains(log, testOut.toString(), "Failed to parse --increment command argument. Can't parse number 'wrong'");

        // Missed increment index.
        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute(cmd, "--snapshot", "check", SNP, "--increment", "1", "--increment", "2"));
        assertContains(log, testOut.toString(), "--increment argument specified twice");

        // Non existent increment.
        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute(cmd, "--snapshot", "check", SNP, "--increment", "2"));

        autoConfirmation = true;
    }

    /** */
    @Test
    public void testSuccessfullCheckEmptySnapshots() {
        grid(0).snapshot().createIncrementalSnapshot(SNP).get(getTestTimeout());

        // Succesfull check.
        assertEquals(EXIT_CODE_OK, execute(cmd, "--snapshot", "check", SNP, "--increment", "1"));
        assertContains(log, testOut.toString(), "The check procedure has finished, no conflicts have been found.");

        // Specify full increment index value.
        assertEquals(EXIT_CODE_OK, execute(cmd, "--snapshot", "check", SNP, "--increment", "0000000000000001"));
        assertContains(log, testOut.toString(), "The check procedure has finished, no conflicts have been found.");
    }

    /** */
    @Test
    public void testSuccessfullCheckSnapshots() {
        load(null);

        grid(0).snapshot().createIncrementalSnapshot(SNP).get(getTestTimeout());

        assertEquals(EXIT_CODE_OK, execute(cmd, "--snapshot", "check", SNP, "--increment", "1"));
        assertContains(log, testOut.toString(), "The check procedure has finished, no conflicts have been found.");
    }

    /** */
    @Test
    public void testSecondSnapshotCorrupted() {
        load(null);

        grid(0).snapshot().createIncrementalSnapshot(SNP).get(getTestTimeout());

        load(xid -> {
            if (skipTxRec == null) {
                skipTxRec = (consId, rec) ->
                    grid(0).localNode().consistentId().equals(consId) && xid.equals(rec.nearXidVersion().asIgniteUuid());
            }
        });

        grid(0).snapshot().createIncrementalSnapshot(SNP).get(getTestTimeout());

        load(null);

        grid(0).snapshot().createIncrementalSnapshot(SNP).get(getTestTimeout());

        assertEquals(EXIT_CODE_OK, execute(cmd, "--snapshot", "check", SNP, "--increment", "1"));
        assertContains(log, testOut.toString(), "The check procedure has finished, no conflicts have been found.");

        assertEquals(EXIT_CODE_OK, execute(cmd, "--snapshot", "check", SNP, "--increment", "2"));
        assertContains(log, testOut.toString(), "The check procedure has failed");

        assertEquals(EXIT_CODE_OK, execute(cmd, "--snapshot", "check", SNP, "--increment", "3"));
        assertContains(log, testOut.toString(), "The check procedure has failed");
    }

    /** */
    @Test public void testSingleTransactionMissed() {
        load(xid -> {
            if (skipTxRec == null) {
                skipTxRec = (consId, rec) ->
                    grid(0).localNode().consistentId().equals(consId) && xid.equals(rec.nearXidVersion().asIgniteUuid());
            }
        });

        grid(0).snapshot().createIncrementalSnapshot(SNP).get(getTestTimeout());

        assertEquals(EXIT_CODE_OK, execute(cmd, "--snapshot", "check", SNP, "--increment", "1"));
        assertContains(log, testOut.toString(), "The check procedure has failed");
        assertContains(log, testOut.toString(), "partiallyCommittedSize=1");
    }

    /** Remove all transactions from single node. */
    @Test public void testSingleNodeMissed() {
        skipTxRec = (consId, rec) -> grid(0).localNode().consistentId().equals(consId);

        load(null);

        grid(0).snapshot().createIncrementalSnapshot(SNP).get(getTestTimeout());

        assertEquals(EXIT_CODE_OK, execute(cmd, "--snapshot", "check", SNP, "--increment", "1"));
        assertContains(log, testOut.toString(), "The check procedure has failed");
        assertContains(log, testOut.toString(), "txHashConflicts=" + (nodesCnt - 1));
    }

    /** */
    @Test public void testRollbackDoesNotMatterIfMissed() throws Exception {
        IgniteEx cln = startClientGrid(nodesCnt);

        TestRecordingCommunicationSpi.spi(cln).blockMessages(GridNearTxPrepareRequest.class, grid(0).name());
        TestRecordingCommunicationSpi.spi(grid(1)).record(GridNearTxPrepareResponse.class);

        runMultiThreadedAsync(() -> {
            try (Transaction tx = cln.transactions().txStart()) {
                cln.cache(CACHE).put(primaryKey(grid(0).cache(CACHE)), 0);
                cln.cache(CACHE).put(primaryKey(grid(1).cache(CACHE)), 0);

                tx.commit();
            }
        }, 1, "async-tx");

        TestRecordingCommunicationSpi.spi(cln).waitForBlocked();
        TestRecordingCommunicationSpi.spi(grid(1)).waitForRecorded();

        // Rollback transaction.
        cln.close();

        load(null);

        grid(0).snapshot().createIncrementalSnapshot(SNP).get(getTestTimeout());

        assertEquals(EXIT_CODE_OK, execute(cmd, "--snapshot", "check", SNP, "--increment", "1"));
        assertContains(log, testOut.toString(), "The check procedure has finished, no conflicts have been found.");
    }

    /** */
    @Test public void testDataRecordMissed() {
        load(xid -> {
            if (skipDataRec == null) {
                skipDataRec = (consId, rec) ->
                    grid(0).localNode().consistentId().equals(consId)
                        && xid.equals(rec.writeEntries().get(0).nearXidVersion().asIgniteUuid());
            }
        });

        grid(0).snapshot().createIncrementalSnapshot(SNP).get(getTestTimeout());

        assertEquals(EXIT_CODE_OK, execute(cmd, "--snapshot", "check", SNP, "--increment", "1"));
        assertContains(log, testOut.toString(), "The check procedure has failed");
        assertNotContains(log, testOut.toString(), "hashConflicts=0");
    }

    /** */
    @Test public void testDataAndTransactionMissed() {
        load(xid -> {
            if (skipDataRec == null) {
                skipDataRec = (consId, rec) ->
                    grid(0).localNode().consistentId().equals(consId)
                        && xid.equals(rec.writeEntries().get(0).nearXidVersion().asIgniteUuid());

                return;
            }

            if (skipTxRec == null) {
                skipTxRec = (consId, rec) ->
                    grid(0).localNode().consistentId().equals(consId) && xid.equals(rec.nearXidVersion().asIgniteUuid());
            }
        });

        grid(0).snapshot().createIncrementalSnapshot(SNP).get(getTestTimeout());

        assertEquals(EXIT_CODE_OK, execute(cmd, "--snapshot", "check", SNP, "--increment", "1"));
        assertContains(log, testOut.toString(), "The check procedure has failed");
        assertContains(log, testOut.toString(), "hashConflicts=" + txPrimNodesCnt);
        assertContains(log, testOut.toString(), "partiallyCommittedSize=1");
    }

    /** */
    @Test
    public void atomicCachesAreSkippedDuringTheCheck() throws Exception {
        String atomicSnp = "testAtomicSnapshot";
        String atomicCache = "testAtomicCache";

        grid(0).createCache(new CacheConfiguration<>()
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setName(atomicCache)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(backupsCnt));

        grid(0).snapshot().createSnapshot(atomicSnp).get();

        load(null);

        TestRecordingCommunicationSpi.spi(grid(0)).blockMessages((n, msg) -> msg instanceof GridDhtAtomicSingleUpdateRequest);

        int key = primaryKey(grid(0).cache(atomicCache));

        grid(0).cache(atomicCache).put(key, 0);

        TestRecordingCommunicationSpi.spi(grid(0)).waitForBlocked();

        grid(0).snapshot().createIncrementalSnapshot(atomicSnp).get(getTestTimeout());

        TestRecordingCommunicationSpi.spi(grid(0)).stopBlock();

        grid(0).destroyCaches(F.asList(CACHE, atomicCache));

        awaitPartitionMapExchange();

        grid(0).snapshot().restoreSnapshot(atomicSnp, null, 1).get();

        IdleVerifyResultV2 idleVerRes = idleVerify(grid(0), CACHE, atomicCache);

        idleVerRes.print(System.out::println, true);

        // Atomic cache has conflict.
        assertTrue(idleVerRes.hasConflicts());

        assertEquals(EXIT_CODE_OK, execute(cmd, "--snapshot", "check", atomicSnp, "--increment", "1"));
        assertContains(log, testOut.toString(), "The check procedure has finished, no conflicts have been found.");
    }

    /** */
    private void load(Consumer<IgniteUuid> txIdHnd) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int txNum = 0; txNum < 1_000; txNum++) {
            try (Transaction tx = grid(0).transactions().txStart()) {
                if (txIdHnd != null)
                    txIdHnd.accept(tx.xid());

                for (int primNode = 0; primNode < txPrimNodesCnt; primNode++) {
                    grid(0).cache(CACHE).put(primaryKey(grid(primNode)), 0);

                    grid(0).cache(CACHE).remove(primaryKey(grid(primNode)), 0);
                }

                // Let read operation run from any node.
                grid(0).cache(CACHE).get(primaryKey(grid(rnd.nextInt(nodesCnt))));

                tx.commit();
            }
        }
    }

    /** @return Key which primary partition is located on the specified node. */
    private int primaryKey(IgniteEx primNode) {
        return primaryKeys(primNode.cache(CACHE), 1, ThreadLocalRandom.current().nextInt(1_000)).get(0);
    }

    /** */
    private class TransactionFilterWALPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "TransactionFilterWALPluginProvider";
        }

        /** {@inheritDoc} */
        @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
            if (IgniteWriteAheadLogManager.class.equals(cls))
                return (T)new TransactionFilterWALManager(((IgniteEx)ctx.grid()).context());

            return null;
        }
    }

    /** Filter transaction records. */
    private class TransactionFilterWALManager extends FileWriteAheadLogManager {
        /** */
        public TransactionFilterWALManager(GridKernalContext ctx) {
            super(ctx);
        }

        /** {@inheritDoc} */
        @Override public WALPointer log(WALRecord record) throws IgniteCheckedException, StorageException {
            if (skipTxRec != null && record.type() == WALRecord.RecordType.TX_RECORD
                && skipTxRec.apply(cctx.localNode().consistentId(), (TxRecord)record))
                return null;

            if (skipDataRec != null && record.type() == WALRecord.RecordType.DATA_RECORD_V2
                && skipDataRec.apply(cctx.localNode().consistentId(), (DataRecord)record))
                return null;

            return super.log(record);
        }
    }
}
