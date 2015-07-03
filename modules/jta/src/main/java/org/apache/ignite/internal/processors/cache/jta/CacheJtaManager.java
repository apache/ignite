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

import org.apache.ignite.*;
import org.apache.ignite.cache.jta.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.jetbrains.annotations.*;

import javax.transaction.*;
import java.lang.ref.*;
import java.util.*;

/**
 * Implementation of {@link CacheJtaManagerAdapter}.
 */
public class CacheJtaManager extends CacheJtaManagerAdapter {
    /** */
    private static final Map<TransactionManager, ThreadLocal<WeakReference<GridCacheXAResource>>> threadLocals =
            new WeakHashMap<>();

    /** */
    private volatile TransactionManager jtaTm;

    /** */
    private ThreadLocal<WeakReference<GridCacheXAResource>> xaRsrc;

    /** */
    private CacheTmLookup tmLookup;

    /** {@inheritDoc} */
    @Override public void createTmLookup(CacheConfiguration ccfg) throws IgniteCheckedException {
        assert ccfg.getTransactionManagerLookupClassName() != null;

        try {
            Class<?> cls = Class.forName(ccfg.getTransactionManagerLookupClassName());

            tmLookup = (CacheTmLookup)cls.newInstance();
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to instantiate transaction manager lookup.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void checkJta() throws IgniteCheckedException {
        TransactionManager jtaTm = this.jtaTm;

        if (jtaTm == null) {
            try {
                jtaTm = tmLookup.getTm();

                if (jtaTm == null)
                    return;

                synchronized (threadLocals) {
                    if (this.jtaTm != null)
                        jtaTm = this.jtaTm;
                    else {
                        xaRsrc = threadLocals.get(jtaTm);

                        if (xaRsrc == null) {
                            xaRsrc = new ThreadLocal<>();

                            threadLocals.put(jtaTm, xaRsrc);
                        }

                        this.jtaTm = jtaTm;
                    }
                }
            }
            catch (Exception e) {
                throw new IgniteCheckedException("Failed to get transaction manager: " + e, e);
            }
        }

        WeakReference<GridCacheXAResource> rsrcRef = xaRsrc.get();

        GridCacheXAResource rsrc = rsrcRef == null ? null : rsrcRef.get();

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

                    xaRsrc.set(new WeakReference<>(rsrc));
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
    @Nullable @Override public Object tmLookup() {
        return tmLookup;
    }
}
