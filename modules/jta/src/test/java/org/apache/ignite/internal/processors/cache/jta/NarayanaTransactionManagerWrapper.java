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
import java.lang.reflect.Method;
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
 * Narayana TransactionManager wrapper that properly suspends transaction from thread-local context.
 * <p>
 * Narayana's {@code TransactionManagerImple.suspend()} does NOT call
 * {@code delistResource(XAResource, TMSUSPEND)} on enlisted XAResources.
 * This is different from JOTM which did call delistResource on suspend.
 * <p>
 * This wrapper bridges the gap by:
 * <ol>
 *   <li>Saving enrolled (XAResource, Xid) pairs</li>
 *   <li>Calling delistResource(TMSUSPEND) before clearing thread-local</li>
 *   <li>On resume: calling start(xid, TMRESUME) on each saved XAResource</li>
 * </ol>
 */
public class NarayanaTransactionManagerWrapper implements TransactionManager {
    /** Narayana TransactionManager. */
    private final com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple delegate;

    /** Tracks whether current thread has suspended transaction. */
    private static final ThreadLocal<Boolean> SUSPENDED = ThreadLocal.withInitial(() -> false);

    /** Saved (XAResource, Xid) pairs from last suspend. */
    private static final ThreadLocal<List<ResourceXidPair>> SUSPENDED_RESOURCES =
        ThreadLocal.withInitial(ArrayList::new);

    /** Narayana TransactionImple._resources field (Hashtable<XAResource, TxInfo>). */
    private static final Field RESOURCES_FIELD;

    /** Narayana TransactionImple.delistResource(XAResource, int) method. */
    private static final Method DELIST_RESOURCE_METHOD;

    /** Narayana TxInfo.xid field. */
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

        Method m;
        try {
            m = com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionImple.class
                .getDeclaredMethod("delistResource", XAResource.class, int.class);
            m.setAccessible(true);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException("Failed to access Narayana TransactionImple.delistResource", e);
        }
        DELIST_RESOURCE_METHOD = m;

        // TxInfo.xid field to get Xid from Narayana's internal TxInfo
        try {
            Class<?> txInfoClass = Class.forName("com.arjuna.ats.internal.jta.xa.TxInfo");
            f = txInfoClass.getDeclaredField("_xid");
            f.setAccessible(true);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to access Narayana TxInfo.xid", e);
        }
        TXINFO_XID_FIELD = f;
    }

    /** Simple pair of XAResource and Xid. */
    private static class ResourceXidPair {
        final XAResource res;
        final Xid xid;

        ResourceXidPair(XAResource res, Xid xid) {
            this.res = res;
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
     * Saves (XAResource, Xid) pairs and calls delistResource(SUSPEND) on each.
     */
    @SuppressWarnings("unchecked")
    private void saveResourcesAndSuspend(com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionImple tx) {
        List<ResourceXidPair> saved = SUSPENDED_RESOURCES.get();
        saved.clear();
        try {
            Hashtable<XAResource, ?> resources = (Hashtable<XAResource, ?>) RESOURCES_FIELD.get(tx);
            if (resources != null && !resources.isEmpty()) {
                Enumeration<XAResource> resEnum = resources.keys();
                while (resEnum.hasMoreElements()) {
                    XAResource res = resEnum.nextElement();
                    // Extract Xid from TxInfo
                    Object txInfo = resources.get(res);
                    Xid xid = null;
                    if (txInfo != null) {
                        try {
                            xid = (Xid) TXINFO_XID_FIELD.get(txInfo);
                        } catch (Exception ignored) {}
                    }
                    saved.add(new ResourceXidPair(res, xid));

                    try {
                        DELIST_RESOURCE_METHOD.invoke(tx, res, XAResource.TMSUSPEND);
                    }
                    catch (Exception ignored) {
                        // Ignore delistResource failures during suspend
                    }
                }
            }
        }
        catch (Exception ignored) {
            // Should not happen after static init
        }
    }

    /**
     * Calls start(xid, TMRESUME) on previously saved XAResources.
     */
    private void resumeSavedResources() {
        List<ResourceXidPair> saved = SUSPENDED_RESOURCES.get();
        for (ResourceXidPair pair : saved) {
            try {
                pair.res.start(pair.xid, XAResource.TMRESUME);
            }
            catch (Exception ignored) {
                // Ignore start(TMRESUME) failures
            }
        }
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
            // Save (XAResource, Xid) pairs and call delistResource(SUSPEND)
            saveResourcesAndSuspend((com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionImple) tx);
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

        // Call start(xid, TMRESUME) on saved resources to re-associate cache tx with thread
        resumeSavedResources();

        SUSPENDED.remove();
        SUSPENDED_RESOURCES.remove();
    }
}
