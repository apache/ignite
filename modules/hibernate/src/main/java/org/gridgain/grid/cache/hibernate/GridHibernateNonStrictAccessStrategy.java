/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.hibernate;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.hibernate.cache.*;
import org.hibernate.cache.spi.access.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Implementation of {@link AccessType#NONSTRICT_READ_WRITE} cache access strategy.
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
 *     &lt;!-- Enable L2 cache with nonstrict-read-write access strategy for entity. --&gt;
 *     &lt;class-cache class="com.example.Entity" usage="nonstrict-read-write"/&gt;
 * &lt;/hibernate-configuration&gt;
 * </pre>
 * Also cache access strategy can be set using annotations:
 * <pre name="code" class="java">
 * &#064;javax.persistence.Entity
 * &#064;javax.persistence.Cacheable
 * &#064;org.hibernate.annotations.Cache(usage = CacheConcurrencyStrategy.NONSTRICT_READ_WRITE)
 * public class Entity { ... }
 * </pre>
 */
public class GridHibernateNonStrictAccessStrategy extends GridHibernateAccessStrategyAdapter {
    /** */
    private final ThreadLocal<WriteContext> writeCtx;

    /**
     * @param grid Grid.
     * @param cache Cache.
     * @param writeCtx Thread local instance used to track updates done during one Hibernate transaction.
     */
    protected GridHibernateNonStrictAccessStrategy(Grid grid, GridCache<Object, Object> cache, ThreadLocal writeCtx) {
        super(grid, cache);

        this.writeCtx = (ThreadLocal<WriteContext>)writeCtx;
    }

    /** {@inheritDoc} */
    @Nullable @Override protected SoftLock lock(Object key) throws CacheException {
        WriteContext ctx = writeCtx.get();

        if (ctx == null)
            writeCtx.set(ctx = new WriteContext());

        ctx.locked(key);

        return null;
    }

    /** {@inheritDoc} */
    @Override protected void unlock(Object key, SoftLock lock) throws CacheException {
        try {
            WriteContext ctx = writeCtx.get();

            if (ctx != null && ctx.unlocked(key)) {
                writeCtx.remove();

                ctx.updateCache(cache);
            }
        }
        catch (GridException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean update(Object key, Object val) throws CacheException {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean afterUpdate(Object key, Object val, SoftLock lock) throws CacheException {
        WriteContext ctx = writeCtx.get();

        if (ctx != null) {
            ctx.updated(key, val);

            unlock(key, lock);

            return true;
        }

        return false;
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
        catch (GridException e) {
            throw new CacheException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void remove(Object key) throws CacheException {
        WriteContext ctx = writeCtx.get();

        if (ctx != null)
            ctx.removed(key);
    }

    /**
     * Information about updates done during single database transaction.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private static class WriteContext {
        /** */
        private Map<Object, Object> updates;

        /** */
        private Set<Object> rmvs;

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

        /**
         * Marks key as updated.
         *
         * @param key Key.
         * @param val Value.
         */
        void updated(Object key, Object val) {
            if (updates == null)
                updates = new GridLeanMap<>();

            updates.put(key, val);
        }

        /**
         * Marks key as removed.
         *
         * @param key Key.
         */
        void removed(Object key) {
            if (rmvs == null)
                rmvs = new GridLeanSet<>();

            rmvs.add(key);
        }

        /**
         * Updates cache.
         *
         * @param cache Cache.
         * @throws GridException If failed.
         */
        void updateCache(GridCache<Object, Object> cache) throws GridException {
            if (!F.isEmpty(rmvs))
                cache.removeAll(rmvs);

            if (!F.isEmpty(updates))
                cache.putAll(updates);
        }
    }
}
