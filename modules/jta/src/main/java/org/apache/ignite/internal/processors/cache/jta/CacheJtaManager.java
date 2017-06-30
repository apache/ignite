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
import javax.cache.configuration.Factory;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.jta.CacheTmLookup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link CacheJtaManagerAdapter}.
 */
public class CacheJtaManager extends CacheJtaManagerAdapter {
    /** */
    private TransactionManager jtaTm;

    /** */
    private final AtomicReference<CacheTmLookup> tmLookupRef = new AtomicReference<>();

    /** */
    private Factory<TransactionManager> tmFactory;

    /** */
    private boolean useJtaSync;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        if (cctx.txConfig() != null) {
            tmFactory = cctx.txConfig().getTxManagerFactory();

            if (tmFactory != null) {
                cctx.kernalContext().resource().injectGeneric(tmFactory);

                if (tmFactory instanceof LifecycleAware)
                    ((LifecycleAware)tmFactory).start();

                Object txMgr;

                try {
                    txMgr = tmFactory.create();
                }
                catch (Exception e) {
                    throw new IgniteCheckedException("Failed to create transaction manager [tmFactory="
                        + tmFactory + "]", e);
                }

                if (txMgr == null)
                    throw new IgniteCheckedException("Failed to create transaction manager (transaction manager " +
                        "factory created null-value transaction manager) [tmFactory=" + tmFactory + "]");

                if (!(txMgr instanceof TransactionManager))
                    throw new IgniteCheckedException("Failed to create transaction manager (transaction manager " +
                        "factory created object that is not an instance of TransactionManager) [tmFactory="
                        + tmFactory + ", txMgr=" + txMgr + "]");

                jtaTm = (TransactionManager)txMgr;
            }
            else {
                String txLookupClsName = cctx.txConfig().getTxManagerLookupClassName();

                if (txLookupClsName != null)
                    tmLookupRef.set(createTmLookup(txLookupClsName));
            }

            useJtaSync = cctx.txConfig().isUseJtaSynchronization();
        }
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        CacheTmLookup tmLookup = tmLookupRef.get();

        if (tmLookup instanceof LifecycleAware)
            ((LifecycleAware)tmLookup).stop();

        if (tmFactory instanceof LifecycleAware)
            ((LifecycleAware)tmFactory).stop();
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
            try {
                Transaction jtaTx = jtaTm.getTransaction();

                if (jtaTx != null) {
                    GridNearTxLocal tx = cctx.tm().userTx();

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

                        CacheJtaResource rsrc = new CacheJtaResource(tx, cctx.kernalContext());

                        if (useJtaSync)
                            jtaTx.registerSynchronization(rsrc);
                        else if (!jtaTx.enlistResource(rsrc))
                            throw new IgniteCheckedException("Failed to enlist XA resource to JTA user transaction.");
                    }
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
