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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.InvalidTransactionException;
import jakarta.transaction.NotSupportedException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionImple;
import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple;
import com.arjuna.ats.internal.jta.xa.TxInfo;

/**
 * Narayana TransactionManager wrapper that simulates JOTM-style suspend/resume.
 * <p>
 * Each wrapper instance has its own state (not shared between parallel tests).
 */
public class TransactionManagerWrapper implements TransactionManager {
    /** Narayana transaction manager. */
    private final TransactionManagerImple delegate = new TransactionManagerImple();

    /** Stores saved XA resources per suspended JTA Transaction (instance). */
    private final Map<Transaction, List<ResourceTxInfoPair>> txRsrcMap = new ConcurrentHashMap<>();

    /** Narayana TransactionImple._resources field (Hashtable<XAResource, TxInfo>). */
    private static final Field RESOURCES_FIELD;

    /** Narayana TxInfo._xid field. */
    private static final Field TXINFO_XID_FIELD;

    static {
        Field f;

        try {
            f = TransactionImple.class.getDeclaredField("_resources");

            f.setAccessible(true);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to access Narayana TransactionImple._resources", e);
        }

        RESOURCES_FIELD = f;

        try {
            f = TxInfo.class.getDeclaredField("_xid");

            f.setAccessible(true);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to access Narayana TxInfo._xid", e);
        }

        TXINFO_XID_FIELD = f;
    }

    /** {@inheritDoc} */
    @Override public void setRollbackOnly() throws IllegalStateException, SecurityException, SystemException {
        delegate.setRollbackOnly();
    }

    /** {@inheritDoc} */
    @Override public void setTransactionTimeout(int seconds) throws SystemException {
        delegate.setTransactionTimeout(seconds);
    }

    /** {@inheritDoc} */
    @Override public int getStatus() throws SystemException {
        return delegate.getStatus();
    }

    /** {@inheritDoc} */
    @Override public Transaction getTransaction() throws SystemException {
        return delegate.getTransaction();
    }

    /** {@inheritDoc} */
    @Override public void begin() throws NotSupportedException, SystemException {
        delegate.begin();
    }

    /** {@inheritDoc} */
    @Override public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
        SecurityException, IllegalStateException, SystemException {
        delegate.commit();
    }

    /** {@inheritDoc} */
    @Override public void rollback() throws IllegalStateException, SecurityException, SystemException {
        delegate.rollback();
    }

    /** {@inheritDoc} */
    @Override public Transaction suspend() throws SystemException {
        Transaction tx = delegate.getTransaction();

        if (tx instanceof TransactionImple)
            saveAndRemoveResources(tx, (TransactionImple)tx);

        return delegate.suspend();
    }

    /** {@inheritDoc} */
    @Override public void resume(Transaction tobj) throws InvalidTransactionException, IllegalStateException, SystemException {
        delegate.resume(tobj);

        // Restore XA resources first
        Transaction curTx = delegate.getTransaction();

        if (curTx instanceof TransactionImple)
            restoreResourcesAndResume(tobj, (TransactionImple)curTx);
    }

    /** Calls end (TMSUSPEND) and removes resources from Narayana's internal map. */
    private void saveAndRemoveResources(Transaction jtaTx, TransactionImple tx) {
        List<ResourceTxInfoPair> saved = new ArrayList<>();

        try {
            Hashtable<XAResource, Object> resources = (Hashtable<XAResource, Object>)RESOURCES_FIELD.get(tx);

            if (resources != null && !resources.isEmpty()) {
                Enumeration<XAResource> resEnum = resources.keys();
                List<XAResource> toRemove = new ArrayList<>();

                while (resEnum.hasMoreElements()) {
                    XAResource res = resEnum.nextElement();
                    Object txInfo = resources.get(res);

                    Xid xid = null;

                    if (txInfo != null) {
                        try {
                            xid = (Xid)TXINFO_XID_FIELD.get(txInfo);
                        }
                        catch (Exception ignored) {
                            // No-op.
                        }
                    }

                    saved.add(new ResourceTxInfoPair(res, txInfo, xid));
                    toRemove.add(res);

                    try {
                        res.end(xid, XAResource.TMSUSPEND);
                    }
                    catch (Exception ignored) {
                        // No-op.
                    }
                }

                for (XAResource res : toRemove)
                    resources.remove(res);
            }
        }
        catch (Exception ignored) {
            // No-op.
        }

        txRsrcMap.put(jtaTx, saved);
    }

    /** Restores resources and calls start (TMRESUME). */
    private void restoreResourcesAndResume(Transaction jtaTx, TransactionImple tx) {
        List<ResourceTxInfoPair> saved = txRsrcMap.remove(jtaTx);

        if (saved == null)
            return;

        try {
            Hashtable<XAResource, Object> resources = (Hashtable<XAResource, Object>)RESOURCES_FIELD.get(tx);

            for (ResourceTxInfoPair pair : saved) {
                if (pair.txInfo != null)
                    resources.put(pair.res, pair.txInfo);

                try {
                    pair.res.start(pair.xid, XAResource.TMRESUME);
                }
                catch (Exception ignored) {
                    // No-op.
                }
            }
        }
        catch (Exception ignored) {
            // No-op.
        }
    }

    /** Simple pair of XAResource, TxInfo and Xid. */
    private record ResourceTxInfoPair(XAResource res, Object txInfo, Xid xid) { }
}
