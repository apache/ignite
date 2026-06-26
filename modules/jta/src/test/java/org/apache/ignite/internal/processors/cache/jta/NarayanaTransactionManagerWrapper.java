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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
 * Narayana TransactionManager wrapper that simulates JOTM-style suspend/resume.
 * <p>
 * Each wrapper instance has its own state (not shared between parallel tests).
 */
public class NarayanaTransactionManagerWrapper implements TransactionManager {
    /** Narayana TransactionManager. */
    private final com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple delegate;

    /** CacheJtaManager reference for suspend/resume support (instance, not shared). */
    private Object cacheJtaManager;

    /** Tracks whether current thread has suspended transaction. */
    private final ThreadLocal<Boolean> suspended = ThreadLocal.withInitial(() -> false);

    /** Stores saved XA resources per suspended JTA Transaction (instance). */
    private final Map<Transaction, List<ResourceTxInfoPair>> txResourceMap = new ConcurrentHashMap<>();

    /** Stores saved Ignite CacheJtaResource per suspended JTA Transaction (instance). */
    private final Map<Transaction, SavedIgniteContext> txIgniteMap = new ConcurrentHashMap<>();

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

    /** Saved Ignite context for a suspended transaction. */
    private static class SavedIgniteContext {
        final Object cacheJtaResource;
        final Object cacheTx;
        final long originalThreadId;

        SavedIgniteContext(Object cacheJtaResource, Object cacheTx, long originalThreadId) {
            this.cacheJtaResource = cacheJtaResource;
            this.cacheTx = cacheTx;
            this.originalThreadId = originalThreadId;
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
     * Sets the CacheJtaManager reference for suspend/resume support.
     * @param manager CacheJtaManager instance
     */
    public void setCacheJtaManager(Object manager) {
        cacheJtaManager = manager;
    }

    /**
     * Calls end(TMSUSPEND) and removes resources from Narayana's internal map.
     */
    @SuppressWarnings("unchecked")
    private void saveAndRemoveResources(
        Transaction jtaTx,
        com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionImple tx) {
        List<ResourceTxInfoPair> saved = new ArrayList<>();
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

        txResourceMap.put(jtaTx, saved);

        // Save Ignite CacheJtaResource
        saveIgniteCacheJtaResource(jtaTx);
    }

    /**
     * Restores resources and calls start(TMRESUME).
     */
    @SuppressWarnings("unchecked")
    private void restoreResourcesAndResume(
        Transaction jtaTx,
        com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionImple tx) {
        List<ResourceTxInfoPair> saved = txResourceMap.remove(jtaTx);
        if (saved == null) {
            return;
        }

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
        if (Boolean.TRUE.equals(suspended.get())) {
            return null;
        }
        return delegate.getTransaction();
    }

    @Override public void begin() throws NotSupportedException, SystemException {
        delegate.begin();
        if (!Boolean.TRUE.equals(suspended.get())) {
            suspended.remove();
        }
    }

    @Override public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
        SecurityException, IllegalStateException, SystemException {
        delegate.commit();
        suspended.remove();
    }

    @Override public void rollback() throws IllegalStateException, SecurityException, SystemException {
        delegate.rollback();
        suspended.remove();
    }

    @Override public Transaction suspend() throws SystemException {
        Transaction tx = delegate.getTransaction();
        if (tx instanceof com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionImple) {
            saveAndRemoveResources(tx, (com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionImple) tx);
        }
        else {
            saveIgniteCacheJtaResource(tx);
        }

        Transaction suspendedTx = delegate.suspend();
        if (suspendedTx != null) {
            suspended.set(true);
        }
        return suspendedTx;
    }

    @Override public void resume(Transaction tobj) throws jakarta.transaction.InvalidTransactionException,
        IllegalStateException, SystemException {
        delegate.resume(tobj);

        // Restore XA resources first
        Transaction currentTx = delegate.getTransaction();
        if (currentTx instanceof com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionImple) {
            restoreResourcesAndResume(tobj, (com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionImple) currentTx);
        }

        // Restore Ignite context — must be done for the calling thread
        restoreIgniteCacheJtaResource(tobj);

        suspended.remove();
    }

    /**
     * Saves Ignite's CacheJtaManager.rsrc ThreadLocal before suspend, keyed by JTA Transaction.
     */
    @SuppressWarnings("unchecked")
    private void saveIgniteCacheJtaResource(Transaction jtaTx) {
        if (cacheJtaManager == null) {
            return;
        }
        try {
            Field rsrcField = cacheJtaManager.getClass().getDeclaredField("rsrc");
            rsrcField.setAccessible(true);
            ThreadLocal<Object> rsrc = (ThreadLocal<Object>) rsrcField.get(cacheJtaManager);
            Object cacheJtaResource = rsrc.get();

            if (cacheJtaResource != null) {
                // Get cacheTx
                Field cacheTxField = cacheJtaResource.getClass().getDeclaredField("cacheTx");
                cacheTxField.setAccessible(true);
                Object cacheTx = cacheTxField.get(cacheJtaResource);

                // Get original threadId via public method threadId()
                Method threadIdMethod = cacheTx.getClass().getMethod("threadId");
                long originalThreadId = (Long) threadIdMethod.invoke(cacheTx);

                txIgniteMap.put(jtaTx, new SavedIgniteContext(cacheJtaResource, cacheTx, originalThreadId));

                // Remove from CacheJtaManager.rsrc so checkJta won't reuse it
                rsrc.remove();
            }
        }
        catch (Exception ignored) {
            // Not critical — may not have Ignite context in some tests
        }
    }

    /**
     * Restores Ignite's CacheJtaManager.rsrc ThreadLocal, threadMap, and threadCtx after resume.
     */
    @SuppressWarnings("unchecked")
    private void restoreIgniteCacheJtaResource(Transaction jtaTx) {
        if (cacheJtaManager == null) {
            return;
        }
        SavedIgniteContext ctx = txIgniteMap.get(jtaTx);
        if (ctx == null) {
            return;
        }

        try {
            // Restore rsrc ThreadLocal in CacheJtaManager
            Field rsrcField = cacheJtaManager.getClass().getDeclaredField("rsrc");
            rsrcField.setAccessible(true);
            ThreadLocal<Object> rsrc = (ThreadLocal<Object>) rsrcField.get(cacheJtaManager);
            rsrc.set(ctx.cacheJtaResource);

            // Navigate to IgniteTxManager
            Class<?> clazz = cacheJtaManager.getClass();
            while (clazz != null && !clazz.getName().contains("GridCacheSharedManagerAdapter")) {
                clazz = clazz.getSuperclass();
            }
            if (clazz == null) {
                return;
            }

            Field cctxField = clazz.getDeclaredField("cctx");
            cctxField.setAccessible(true);
            Object cctx = cctxField.get(cacheJtaManager);

            Method tmMethod = cctx.getClass().getMethod("tm");
            Object tm = tmMethod.invoke(cctx);

            // Restore threadCtx ThreadLocal (IgniteTxManager.threadCtx)
            Field threadCtxField = tm.getClass().getDeclaredField("threadCtx");
            threadCtxField.setAccessible(true);
            ThreadLocal<Object> threadCtx = (ThreadLocal<Object>) threadCtxField.get(tm);
            threadCtx.set(ctx.cacheTx);

            // Restore threadMap — essential for threadLocalTx() to find the tx
            Field threadMapField = tm.getClass().getDeclaredField("threadMap");
            threadMapField.setAccessible(true);
            Object threadMap = threadMapField.get(tm);
            long currentThreadId = Thread.currentThread().getId();

            // Update threadId on the transaction
            Method threadIdMethod = ctx.cacheTx.getClass().getMethod("threadId", long.class);
            threadIdMethod.invoke(ctx.cacheTx, currentThreadId);

            // Put into current thread
            Method putMethod = threadMap.getClass().getMethod("put", Object.class, Object.class);
            putMethod.invoke(threadMap, currentThreadId, ctx.cacheTx);
        }
        catch (Exception ignored) {
            // Not critical
        }
    }

    /**
     * Cleans up all saved state after commit/rollback.
     */
    private void cleanupAll() {
        // Clean up Ignite context and remove threadMap entries
        if (cacheJtaManager != null) {
            for (SavedIgniteContext ctx : txIgniteMap.values()) {
                try {
                    Class<?> clazz = cacheJtaManager.getClass();
                    while (clazz != null && !clazz.getName().contains("GridCacheSharedManagerAdapter")) {
                        clazz = clazz.getSuperclass();
                    }
                    if (clazz != null) {
                        Field cctxField = clazz.getDeclaredField("cctx");
                        cctxField.setAccessible(true);
                        Object cctx = cctxField.get(cacheJtaManager);

                        Method tmMethod = cctx.getClass().getMethod("tm");
                        Object tm = tmMethod.invoke(cctx);

                        // Remove from threadMap
                        Field threadMapField = tm.getClass().getDeclaredField("threadMap");
                        threadMapField.setAccessible(true);
                        Object threadMap = threadMapField.get(tm);

                        Method removeMethod = threadMap.getClass().getMethod("remove", Object.class);
                        removeMethod.invoke(threadMap, ctx.cacheTx);

                        // Clear threadCtx
                        Field threadCtxField = tm.getClass().getDeclaredField("threadCtx");
                        threadCtxField.setAccessible(true);
                        ThreadLocal<Object> threadCtx = (ThreadLocal<Object>) threadCtxField.get(tm);
                        if (threadCtx.get() == ctx.cacheTx) {
                            threadCtx.remove();
                        }
                    }
                }
                catch (Exception ignored) {}
            }
        }

        txIgniteMap.clear();
        txResourceMap.clear();
    }
}
