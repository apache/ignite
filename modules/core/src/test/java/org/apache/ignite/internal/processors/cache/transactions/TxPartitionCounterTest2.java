package org.apache.ignite.internal.processors.cache.transactions;

import java.util.stream.IntStream;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class TxPartitionCounterTest2 extends TxSinglePartitionAbstractTest {
    /**
     *
     */
    public void testBasicTxCallback() throws Exception {
        int partId = 0;
        int backups = 2;
        int nodes = 3;
        int[] sizes = new int[] {3};
        int total = IntStream.of(sizes).sum();

        runOnPartition(partId, backups, nodes, new TxCallback() {
            @Override public boolean beforePrimaryPrepare(IgniteEx node, IgniteUuid nearXidVer,
                GridFutureAdapter<?> proceedFut) {
                log.info("TX: beforePrimaryPrepare: prim=" + node.name() + ", nearXidVer=" + nearXidVer);

                return false;
            }

            @Override public boolean beforeBackupPrepare(IgniteEx prim, IgniteEx backup, IgniteInternalTx primaryTx,
                GridFutureAdapter<?> proceedFut) {

                log.info("TX: beforeBackupPrepare: prim=" + prim.name() + ", backup=" + backup.name() + ", nearXidVer=" + primaryTx.nearXidVersion().asGridUuid() + ", tx=" + CU.txString(primaryTx) );

                return false;
            }

            @Override public boolean beforePrimaryFinish(IgniteEx primaryNode, IgniteInternalTx primaryTx, GridFutureAdapter<?>
                proceedFut) {

                log.info("TX: beforePrimaryFinish: prim=" + primaryNode.name() + ", nearXidVer=" + primaryTx.nearXidVersion().asGridUuid() + ", tx=" + CU.txString(primaryTx));

                return false;
            }

            @Override public boolean afterPrimaryFinish(IgniteEx primaryNode, IgniteUuid nearXidVer, GridFutureAdapter<?> proceedFut) {
                log.info("TX: afterPrimaryFinish: prim=" + primaryNode.name() + ", nearXidVer=" + nearXidVer);

                return false;
            }

            @Override public boolean afterBackupPrepare(IgniteEx backup, IgniteInternalTx tx, GridFutureAdapter<?> fut) {
                log.info("TX: afterBackupPrepare: backup=" + backup.name() + ", backupTx=" + CU.txString(tx) + ", nearXidVer=" + tx.nearXidVersion().asGridUuid());

                return false;
            }

            @Override public boolean afterBackupFinish(IgniteEx backup, IgniteUuid nearXidVer, GridFutureAdapter<?> fut) {
                log.info("TX: afterBackupFinish: backup=" + backup.name() + ", nearXidVer=" + nearXidVer);

                return false;
            }

            @Override public boolean beforeBackupFinish(IgniteEx prim, IgniteEx backup, @Nullable IgniteInternalTx primTx,
                IgniteInternalTx backupTx,
                GridFutureAdapter<?> fut) {
                log.info("TX: beforeBackupFinish: prim=" + prim.name() + ", backup=" + backup.name() + ", primNearXidVer=" +
                    (primTx == null ? "NA" : primTx.nearXidVersion().asGridUuid()) + ", backupNearXidVer=" + backupTx.nearXidVersion().asGridUuid());

                return false;
            }

            @Override public void onTxStart(Transaction tx, int idx) {
            }
        }, sizes);

        int size = grid("client").cache(DEFAULT_CACHE_NAME).size();

        assertEquals(total, size);
    }
}
