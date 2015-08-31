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

package org.apache.ignite.cache.hibernate;

import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.GridLeanSet;
import org.apache.ignite.transactions.Transaction;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.SoftLock;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Implementation of {@link AccessType#READ_WRITE} cache access strategy.
 * <p>
 * Configuration of L2 cache and per-entity cache access strategy can be set in the
 * Hibernate configuration file:
 * <pre name="code" class="xml">
 * &lt;hibernate-configuration&gt;
 *     &lt;!-- Enable L2 cache. --&gt;
 *     &lt;property name="cache.use_second_level_cache"&gt;true&lt;/property&gt;
 *
 *     &lt;!-- Use Ignite as L2 cache provider. --&gt;
 *     &lt;property name="cache.region.factory_class"&gt;org.apache.ignite.cache.hibernate.HibernateRegionFactory&lt;/property&gt;
 *
 *     &lt;!-- Specify entity. --&gt;
 *     &lt;mapping class="com.example.Entity"/&gt;
 *
 *     &lt;!-- Enable L2 cache with read-write access strategy for entity. --&gt;
 *     &lt;class-cache class="com.example.Entity" usage="read-write"/&gt;
 * &lt;/hibernate-configuration&gt;
 * </pre>
 * Also cache access strategy can be set using annotations:
 * <pre name="code" class="java">
 * &#064;javax.persistence.Entity
 * &#064;javax.persistence.Cacheable
 * &#064;org.hibernate.annotations.Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
 * public class Entity { ... }
 * </pre>
 */
public class HibernateReadWriteAccessStrategy extends HibernateAccessStrategyAdapter {
    /** */
    private final ThreadLocal<TxContext> txCtx;

    /**
     * @param ignite Grid.
     * @param cache Cache.
     * @param txCtx Thread local instance used to track updates done during one Hibernate transaction.
     */
    protected HibernateReadWriteAccessStrategy(Ignite ignite, IgniteInternalCache<Object, Object> cache, ThreadLocal txCtx) {
        super(ignite, cache);

        this.txCtx = (ThreadLocal<TxContext>)txCtx;
    }

    /** {@inheritDoc} */
    @Override protected Object get(Object key) throws CacheException {
        boolean success = false;

        try {
            Object o = cache.get(key);

            success = true;

            return o;
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
        finally {
            if (!success)
                rollbackCurrentTx();
        }
    }

    /** {@inheritDoc} */
    @Override protected void putFromLoad(Object key, Object val) throws CacheException {
        boolean success = false;

        try {
            cache.put(key, val);

            success = true;
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
        finally {
            if (!success)
                rollbackCurrentTx();
        }
    }

    /** {@inheritDoc} */
    @Override protected SoftLock lock(Object key) throws CacheException {
        boolean success = false;

        try {
            TxContext ctx = txCtx.get();

            if (ctx == null)
                txCtx.set(ctx = new TxContext());

            lockKey(key);

            ctx.locked(key);

            success = true;

            return null;
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
        finally {
            if (!success)
                rollbackCurrentTx();
        }
    }

    /** {@inheritDoc} */
    @Override protected void unlock(Object key, SoftLock lock) throws CacheException {
        boolean success = false;

        try {
            TxContext ctx = txCtx.get();

            if (ctx != null)
                unlock(ctx, key);

            success = true;
        }
        catch (Exception e) {
            throw new CacheException(e);
        }
        finally {
            if (!success)
                rollbackCurrentTx();
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean update(Object key, Object val) throws CacheException {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean afterUpdate(Object key, Object val, SoftLock lock) throws CacheException {
        boolean success = false;
        boolean res = false;

        try {
            TxContext ctx = txCtx.get();

            if (ctx != null) {
                cache.put(key, val);

                unlock(ctx, key);

                res = true;
            }

            success = true;

            return res;
        }
        catch (Exception e) {
            throw new CacheException(e);
        }
        finally {
            if (!success)
                rollbackCurrentTx();
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean insert(Object key, Object val) throws CacheException {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean afterInsert(Object key, Object val) throws CacheException {
        boolean success = false;

        try {
            cache.put(key, val);

            success = true;

            return true;
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
        finally {
            if (!success)
                rollbackCurrentTx();
        }
    }

    /** {@inheritDoc} */
    @Override protected void remove(Object key) throws CacheException {
        boolean success = false;

        try {
            TxContext ctx = txCtx.get();

            if (ctx != null)
                cache.remove(key);

            success = true;
        }
        catch (IgniteCheckedException e) {
            throw new CacheException(e);
        }
        finally {
            if (!success)
                rollbackCurrentTx();
        }
    }

    /**
     *
     * @param ctx Transaction context.
     * @param key Key.
     * @throws CacheException If failed.
     */
    private void unlock(TxContext ctx, Object key) throws CacheException {
        if (ctx.unlocked(key)) { // Finish transaction if last key is unlocked.
            txCtx.remove();

            Transaction tx = cache.tx();

            assert tx != null;

            try {
                tx.commit();
            }
            finally {
                tx.close();
            }

            assert cache.tx() == null;
        }
    }

    /**
     * Roll backs current transaction.
     */
    private void rollbackCurrentTx() {
        try {
            TxContext ctx = txCtx.get();

            if (ctx != null) {
                txCtx.remove();

                Transaction tx = cache.tx();

                if (tx != null)
                    tx.rollback();
            }
        }
        catch (IgniteException e) {
            log.error("Failed to rollback cache transaction.", e);
        }
    }

    /**
     * @param key Key.
     * @throws IgniteCheckedException If failed.
     */
    private void lockKey(Object key) throws IgniteCheckedException {
        if (cache.tx() == null)
            cache.txStart(PESSIMISTIC, REPEATABLE_READ);

        cache.get(key); // Acquire distributed lock.
    }

    /**
     * Information about updates done during single database transaction.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private static class TxContext {
        /** */
        private Set<Object> locked = new GridLeanSet<>();

        /**
         * Marks key as locked.
         *
         * @param key Key.
         */
        void locked(Object key) {
            locked.add(key);
        }

        /**
         * Marks key as unlocked.
         *
         * @param key Key.
         * @return {@code True} if last locked key was unlocked.
         */
        boolean unlocked(Object key) {
            locked.remove(key);

            return locked.isEmpty();
        }
    }
}