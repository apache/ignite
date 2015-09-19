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

package org.apache.ignite.internal.processors.cache.jta;

import java.util.concurrent.atomic.AtomicReference;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.jta.CacheTmLookup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link CacheJtaManagerAdapter}.
 */
public class CacheJtaManager extends CacheJtaManagerAdapter {
    /** */
    private final ThreadLocal<GridCacheXAResource> xaRsrc = new ThreadLocal<>();

    /** */
    private TransactionManager jtaTm;

    /** */
    private final AtomicReference<CacheTmLookup> tmLookupRef = new AtomicReference<>();

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        if (cctx.txConfig() != null) {
            String txLookupClsName = cctx.txConfig().getTxManagerLookupClassName();

            if (txLookupClsName != null)
                tmLookupRef.set(createTmLookup(txLookupClsName));
        }
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        CacheTmLookup tmLookup = tmLookupRef.get();

        if (tmLookup instanceof LifecycleAware)
            ((LifecycleAware)tmLookup).stop();
    }

    /**
     * @throws IgniteCheckedException
     */
    private CacheTmLookup createTmLookup(String tmLookupClsName) throws IgniteCheckedException {
        try {
            Class<?> cls = Class.forName(tmLookupClsName);

            CacheTmLookup res = (CacheTmLookup)cls.newInstance();

            cctx.kernalContext().resource().injectGeneric(res);

            if (res instanceof LifecycleAware)
                ((LifecycleAware)res).start();

            return res;
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to instantiate transaction manager lookup.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void checkJta() throws IgniteCheckedException {
        if (jtaTm == null) {
            try {
                CacheTmLookup tmLookup = tmLookupRef.get();

                if (tmLookup == null)
                    return;

                jtaTm = tmLookup.getTm();
            }
            catch (Exception e) {
                throw new IgniteCheckedException("Failed to get transaction manager: " + e, e);
            }
        }

        if (jtaTm != null) {
            GridCacheXAResource rsrc = xaRsrc.get();

            if (rsrc == null || rsrc.isFinished()) {
                try {
                    Transaction jtaTx = jtaTm.getTransaction();

                    if (jtaTx != null) {
                        IgniteInternalTx tx = cctx.tm().userTx();

                        if (tx == null) {
                            TransactionConfiguration tCfg = cctx.kernalContext().config()
                                .getTransactionConfiguration();

                            tx = cctx.tm().newTx(
                                /*implicit*/false,
                                /*implicit single*/false,
                                null,
                                tCfg.getDefaultTxConcurrency(),
                                tCfg.getDefaultTxIsolation(),
                                tCfg.getDefaultTxTimeout(),
                                /*store enabled*/true,
                                /*tx size*/0
                            );
                        }

                        rsrc = new GridCacheXAResource(tx, cctx.kernalContext());

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
    @Override public void registerCache(CacheConfiguration<?, ?> cfg) throws IgniteCheckedException {
        String cacheLookupClsName = cfg.getTransactionManagerLookupClassName();

        if (cacheLookupClsName != null) {
            CacheTmLookup tmLookup = tmLookupRef.get();

            if (tmLookup == null) {
                tmLookup = createTmLookup(cacheLookupClsName);

                if (tmLookupRef.compareAndSet(null, tmLookup))
                    return;

                tmLookup = tmLookupRef.get();
            }

            if (!cacheLookupClsName.equals(tmLookup.getClass().getName()))
                throw new IgniteCheckedException("Failed to start cache with CacheTmLookup that specified in cache " +
                    "configuration, because node uses another CacheTmLookup [cache" + cfg.getName() +
                    ", tmLookupClassName=" + cacheLookupClsName + ", tmLookupUsedByNode="
                    + tmLookup.getClass().getName() + ']');
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object tmLookup() {
        return tmLookupRef.get();
    }
}