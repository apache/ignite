/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.jta;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.jta.*;
import org.gridgain.grid.kernal.processors.cache.transactions.*;
import org.jetbrains.annotations.*;

import javax.transaction.*;

/**
 * Implementation of {@link GridCacheJtaManagerAdapter}.
 */
public class GridCacheJtaManager<K, V> extends GridCacheJtaManagerAdapter<K, V> {
    /** */
    private final ThreadLocal<GridCacheXAResource> xaRsrc = new ThreadLocal<>();

    /** */
    private TransactionManager jtaTm;

    /** */
    private GridCacheTmLookup tmLookup;

    /** {@inheritDoc} */
    @Override public void createTmLookup(GridCacheConfiguration ccfg) throws IgniteCheckedException {
        assert ccfg.getTransactionManagerLookupClassName() != null;

        try {
            Class<?> cls = Class.forName(ccfg.getTransactionManagerLookupClassName());

            tmLookup = (GridCacheTmLookup)cls.newInstance();
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to instantiate transaction manager lookup.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void checkJta() throws IgniteCheckedException {
        if (jtaTm == null)
            jtaTm = tmLookup.getTm();

        if (jtaTm != null) {
            GridCacheXAResource rsrc = xaRsrc.get();

            if (rsrc == null || rsrc.isFinished()) {
                try {
                    Transaction jtaTx = jtaTm.getTransaction();

                    if (jtaTx != null) {
                        IgniteTx tx = cctx.tm().userTx();

                        if (tx == null) {
                            TransactionsConfiguration tCfg = cctx.kernalContext().config()
                                .getTransactionsConfiguration();

                            tx = cctx.tm().newTx(
                                /*implicit*/false,
                                /*implicit single*/false,
                                /*system*/false,
                                tCfg.getDefaultTxConcurrency(),
                                tCfg.getDefaultTxIsolation(),
                                tCfg.getDefaultTxTimeout(),
                                /*invalidate*/false,
                                /*store enabled*/true,
                                /*tx size*/0,
                                /*group lock keys*/null,
                                /*partition lock*/false
                            );
                        }

                        rsrc = new GridCacheXAResource((IgniteTxEx)tx, cctx);

                        if (!jtaTx.enlistResource(rsrc))
                            throw new IgniteCheckedException("Failed to enlist XA resource to JTA user transaction.");

                        xaRsrc.set(rsrc);
                    }
                }
                catch (SystemException e) {
                    throw new IgniteCheckedException("Failed to obtain JTA transaction.", e);
                }
                catch (RollbackException e) {
                    throw new IgniteCheckedException("Failed to enlist XAResource to JTA transaction.", e);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object tmLookup() {
        return tmLookup;
    }
}
