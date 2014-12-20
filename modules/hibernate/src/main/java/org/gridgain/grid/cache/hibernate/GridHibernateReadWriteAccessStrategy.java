/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.hibernate;

import org.apache.ignite.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.*;
import org.hibernate.cache.*;
import org.hibernate.cache.spi.access.*;

import java.util.*;

import static org.apache.ignite.transactions.GridCacheTxConcurrency.*;
import static org.apache.ignite.transactions.GridCacheTxIsolation.*;

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
 *     &lt;!-- Use GridGain as L2 cache provider. --&gt;
 *     &lt;property name="cache.region.factory_class"&gt;org.gridgain.grid.cache.hibernate.GridHibernateRegionFactory&lt;/property&gt;
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
public class GridHibernateReadWriteAccessStrategy extends GridHibernateAccessStrategyAdapter {
    /** */
    private final ThreadLocal<TxContext> txCtx;

    /**
     * @param ignite Grid.
     * @param cache Cache.
     * @param txCtx Thread local instance used to track updates done during one Hibernate transaction.
     */
    protected GridHibernateReadWriteAccessStrategy(Ignite ignite, GridCache<Object, Object> cache, ThreadLocal txCtx) {
        super(ignite, cache);

        this.txCtx = (ThreadLocal<TxContext>)txCtx;
    }

    /** {@inheritDoc} */
    @Override protected Object get(Object key) throws CacheException {
        try {
            return cache.get(key);
        }
        catch (IgniteCheckedException e) {
            rollbackCurrentTx();

            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void putFromLoad(Object key, Object val) throws CacheException {
        try {
            cache.putx(key, val);
        }
        catch (IgniteCheckedException e) {
            rollbackCurrentTx();

            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected SoftLock lock(Object key) throws CacheException {
        try {
            TxContext ctx = txCtx.get();

            if (ctx == null)
                txCtx.set(ctx = new TxContext());

            lockKey(key);

            ctx.locked(key);

            return null;
        }
        catch (IgniteCheckedException e) {
            rollbackCurrentTx();

            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void unlock(Object key, SoftLock lock) throws CacheException {
        try {
            TxContext ctx = txCtx.get();

            if (ctx != null)
                unlock(ctx, key);
        }
        catch (IgniteCheckedException e) {
            rollbackCurrentTx();

            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean update(Object key, Object val) throws CacheException {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean afterUpdate(Object key, Object val, SoftLock lock) throws CacheException {
        try {
            TxContext ctx = txCtx.get();

            if (ctx != null) {
                cache.putx(key, val);

                unlock(ctx, key);

                return true;
            }

            return false;
        }
        catch (IgniteCheckedException e) {
            rollbackCurrentTx();

            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean insert(Object key, Object val) throws CacheException {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean afterInsert(Object key, Object val) throws CacheException {
        try {
            cache.putx(key, val);

            return true;
        }
        catch (IgniteCheckedException e) {
            rollbackCurrentTx();

            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void remove(Object key) throws CacheException {
        try {
            TxContext ctx = txCtx.get();

            if (ctx != null)
                cache.removex(key);
        }
        catch (IgniteCheckedException e) {
            rollbackCurrentTx();

            throw new CacheException(e);
        }
    }

    /**
     *
     * @param ctx Transaction context.
     * @param key Key.
     * @throws IgniteCheckedException If failed.
     */
    private void unlock(TxContext ctx, Object key) throws IgniteCheckedException {
        if (ctx.unlocked(key)) { // Finish transaction if last key is unlocked.
            txCtx.remove();

            IgniteTx tx = cache.tx();

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

                IgniteTx tx = cache.tx();

                if (tx != null)
                    tx.rollback();
            }
        }
        catch (IgniteCheckedException e) {
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
