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

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.lang.reflect.Field;
import jakarta.transaction.RollbackException;
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;
import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.NotSupportedException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * Narayana TransactionManager wrapper that attempts to simulate JOTM-style suspend/resume.
 * <p>
 * <b>Important limitation:</b> Narayana 7.x does NOT support proper suspend/resume like JOTM did.
 * Narayana's thread-local management (Arjuna ThreadAction) does not restore the transaction context
 * after resume() in a way that Ignite's cache transaction can detect. The suspend/resume tests
 * in {@link org.apache.ignite.internal.processors.cache.GridJtaTransactionManagerSelfTest}
 * are therefore ignored when using Narayana.
 * <p>
 * This wrapper still provides basic pass-through functionality for non-suspend/resume operations.
 */
public class NarayanaTransactionManagerWrapper implements TransactionManager {
    /** Narayana TransactionManager. */
    private final com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple delegate;

    /** Tracks whether current thread has suspended transaction. */
    private static final ThreadLocal<Boolean> SUSPENDED = ThreadLocal.withInitial(() -> false);

    /** Saved (XAResource, TxInfo, Xid) pairs from last suspend. */
    private static final ThreadLocal<List<ResourceTxInfoPair>> SUSPENDED_RESOURCES =
        ThreadLocal.withInitial(ArrayList::new);

    /** Narayana TransactionImple._resources field (Hashtable<XAResource, TxInfo>). */
    private static final Field RESOURCES_FIELD;

    /** Narayana TxInfo._xid field. */
    private static final Field TXINFO_XID_FIELD;

    static {
        Field f;
        try {
            f = com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionImple.class
                .getDeclaredField("_resources");
            f.setAccessible(true);
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException("Failed to access Narayana TransactionImple._resources", e);
        }
        RESOURCES_FIELD = f;

        try {
            Class<?> txInfoClass = Class.forName("com.arjuna.ats.internal.jta.xa.TxInfo");
            f = txInfoClass.getDeclaredField("_xid");
            f.setAccessible(true);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to access Narayana TxInfo._xid", e);
        }
        TXINFO_XID_FIELD = f;
    }

    /** Simple pair of XAResource, TxInfo and Xid. */
    private static class ResourceTxInfoPair {
        final XAResource res;
        final Object txInfo;
        final Xid xid;

        ResourceTxInfoPair(XAResource res, Object txInfo, Xid xid) {
            this.res = res;
            this.txInfo = txInfo;
            this.xid = xid;
        }
    }

    /**
     * @param delegate Narayana TransactionManager to wrap.
     */
    public NarayanaTransactionManagerWrapper(
        com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple delegate) {
        this.delegate = delegate;
    }

    /**
     * Calls end(TMSUSPEND) and removes resources from Narayana's internal map.
     */
    @SuppressWarnings("unchecked")
    private void saveAndRemoveResources(com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionImple tx) {
        List<ResourceTxInfoPair> saved = SUSPENDED_RESOURCES.get();
        saved.clear();
        try {
            Hashtable<XAResource, Object> resources = (Hashtable<XAResource, Object>) RESOURCES_FIELD.get(tx);
            if (resources != null && !resources.isEmpty()) {
                Enumeration<XAResource> resEnum = resources.keys();
                List<XAResource> toRemove = new ArrayList<>();
                while (resEnum.hasMoreElements()) {
                    XAResource res = resEnum.nextElement();
                    Object txInfo = resources.get(res);
                    Xid xid = null;
                    if (txInfo != null) {
                        try {
                            xid = (Xid) TXINFO_XID_FIELD.get(txInfo);
                        } catch (Exception ignored) {}
                    }
                    saved.add(new ResourceTxInfoPair(res, txInfo, xid));
                    toRemove.add(res);

                    try {
                        res.end(xid, XAResource.TMSUSPEND);
                    }
                    catch (Exception ignored) {}
                }
                for (XAResource res : toRemove) {
                    resources.remove(res);
                }
            }
        }
        catch (Exception ignored) {}
    }

    /**
     * Restores resources and calls start(TMRESUME).
     */
    @SuppressWarnings("unchecked")
    private void restoreResourcesAndResume(com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionImple tx) {
        List<ResourceTxInfoPair> saved = SUSPENDED_RESOURCES.get();
        try {
            Hashtable<XAResource, Object> resources = (Hashtable<XAResource, Object>) RESOURCES_FIELD.get(tx);
            for (ResourceTxInfoPair pair : saved) {
                if (pair.txInfo != null) {
                    resources.put(pair.res, pair.txInfo);
                }
                try {
                    pair.res.start(pair.xid, XAResource.TMRESUME);
                }
                catch (Exception ignored) {}
            }
        }
        catch (Exception ignored) {}
        saved.clear();
    }

    @Override public void setRollbackOnly() throws IllegalStateException, SecurityException, SystemException {
        delegate.setRollbackOnly();
    }

    @Override public void setTransactionTimeout(int seconds) throws SystemException {
        delegate.setTransactionTimeout(seconds);
    }

    @Override public int getStatus() throws SystemException {
        return delegate.getStatus();
    }

    @Override public Transaction getTransaction() throws SystemException {
        if (Boolean.TRUE.equals(SUSPENDED.get())) {
            return null;
        }
        return delegate.getTransaction();
    }

    @Override public void begin() throws NotSupportedException, SystemException {
        delegate.begin();
        SUSPENDED.remove();
        SUSPENDED_RESOURCES.remove();
    }

    @Override public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
        SecurityException, IllegalStateException, SystemException {
        delegate.commit();
        SUSPENDED.remove();
        SUSPENDED_RESOURCES.remove();
    }

    @Override public void rollback() throws IllegalStateException, SecurityException, SystemException {
        delegate.rollback();
        SUSPENDED.remove();
        SUSPENDED_RESOURCES.remove();
    }

    @Override public Transaction suspend() throws SystemException {
        Transaction tx = delegate.getTransaction();
        if (tx instanceof com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionImple) {
            saveAndRemoveResources((com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionImple) tx);
        }

        Transaction suspended = delegate.suspend();
        if (suspended != null) {
            SUSPENDED.set(true);
        }
        return suspended;
    }

    @Override public void resume(Transaction tobj) throws jakarta.transaction.InvalidTransactionException,
        IllegalStateException, SystemException {
        delegate.resume(tobj);

        Transaction currentTx = delegate.getTransaction();
        if (currentTx instanceof com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionImple) {
            restoreResourcesAndResume((com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionImple) currentTx);
        }

        SUSPENDED.remove();
        SUSPENDED_RESOURCES.remove();
    }
}
