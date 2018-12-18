package org.apache.ignite.internal.processors.cache.transactions;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteClosure2X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Print DHT transaction using tx callback framework.
 * TODO asserts on counted events.
 */
public class TxPartitionCounterStateBasicOrderingTest extends TxPartitionCounterStateAbstractTest {
    /** */
    public void testBasicTxCallback() throws Exception {
        int partId = 0;
        int backups = 1;
        int nodes = 2;
        int txSize = 5;

        runOnPartition(partId, -1, backups, nodes, new IgniteClosure2X<Ignite, List<Ignite>, TxCallback>() {
            @Override public TxCallback applyx(Ignite ignite,
                List<Ignite> ignites) throws IgniteCheckedException {
                return new TxCallback() {
                    @Override public boolean beforePrimaryPrepare(IgniteEx primary, IgniteUuid nearXidVer,
                        GridFutureAdapter<?> proceedFut) {
                        log.info("TX: beforePrimaryPrepare: prim=" + primary.name() + ", nearXidVer=" + nearXidVer);

                        return false;
                    }

                    @Override public boolean beforeBackupPrepare(IgniteEx primary, IgniteEx backup, IgniteInternalTx primaryTx,
                        GridFutureAdapter<?> proceedFut) {

                        log.info("TX: beforeBackupPrepare: prim=" + primary.name() + ", backup=" + backup.name() + ", nearXidVer=" + primaryTx.nearXidVersion().asGridUuid() + ", tx=" + CU.txString(primaryTx) );

                        return false;
                    }

                    @Override public boolean beforePrimaryFinish(IgniteEx primary, IgniteInternalTx primaryTx, GridFutureAdapter<?>
                        proceedFut) {

                        log.info("TX: beforePrimaryFinish: prim=" + primary.name() + ", nearXidVer=" + primaryTx.nearXidVersion().asGridUuid() + ", tx=" + CU.txString(primaryTx));

                        return false;
                    }

                    @Override public boolean afterPrimaryFinish(IgniteEx primary, IgniteUuid nearXidVer, GridFutureAdapter<?> proceedFut) {
                        log.info("TX: afterPrimaryFinish: prim=" + primary.name() + ", nearXidVer=" + nearXidVer);

                        return false;
                    }

                    @Override public boolean afterBackupPrepare(IgniteEx primary, IgniteEx backup,
                        @Nullable IgniteInternalTx tx,
                        IgniteUuid nearXidVer, GridFutureAdapter<?> fut) {
                        log.info("TX: afterBackupPrepare: backup=" + backup.name() + ", backupTx=" + CU.txString(tx) +
                            ", nearXidVer=" + nearXidVer);

                        return false;
                    }

                    @Override public boolean afterBackupFinish(IgniteEx primary, IgniteEx backup, IgniteUuid nearXidVer,
                        GridFutureAdapter<?> fut) {
                        log.info("TX: afterBackupFinish: backup=" + backup.name() + ", nearXidVer=" + nearXidVer);

                        return false;
                    }

                    @Override public boolean beforeBackupFinish(IgniteEx primary, IgniteEx backup,
                        @Nullable IgniteInternalTx primaryTx,
                        IgniteInternalTx backupTx,
                        IgniteUuid nearXidVer, GridFutureAdapter<?> fut) {
                        log.info("TX: beforeBackupFinish: prim=" + primary.name() + ", backup=" + backup.name() + ", nearXidVer=" + nearXidVer);

                        return false;
                    }

                    @Override public boolean afterPrimaryPrepare(IgniteEx prim, IgniteInternalTx tx,
                        IgniteUuid nearXidVer, GridFutureAdapter<?> fut) {
                        log.info("TX: afterPrimaryPrepare: prim=" + prim.name() + ", nearXidVer=" + nearXidVer + ", tx=" + CU.txString(tx));

                        return false;
                    }

                    @Override public void onTxStart(Transaction tx, int idx) {
                    }
                };
            }
        }, new int[] {txSize});

        assertEquals(txSize + PRELOAD_KEYS_CNT, grid("client").cache(DEFAULT_CACHE_NAME).size());
    }
}
