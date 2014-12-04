/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.hibernate;

import org.apache.ignite.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.hibernate.cache.*;
import org.hibernate.cache.spi.access.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.concurrent.*;

/**
 * Common interface used to implement Hibernate L2 cache access strategies ({@link RegionAccessStrategy},
 * {@link EntityRegionAccessStrategy} and {@link CollectionRegionAccessStrategy}).
 * <p>
 * The expected sequences of steps related to various CRUD operations executed by Hibernate are:
 * <p>
 * Insert:
 * <ul>
 *     <li>Start DB transaction.</li>
 *     <li>Execute database insert.</li>
 *     <li>Call {@link GridHibernateAccessStrategyAdapter#insert}.</li>
 *     <li>Commit DB transaction.</li>
 *     <li>Call {@link GridHibernateAccessStrategyAdapter#afterInsert}.</li>
 * </ul>
 * In case if some step fails and DB transaction is rolled back then
 * {@link GridHibernateAccessStrategyAdapter#afterInsert} is not called.
 * <p>
 * Update:
 * <ul>
 *     <li>Start DB transaction.</li>
 *     <li>Call {@link GridHibernateAccessStrategyAdapter#lock}.</li>
 *     <li>Execute database update.</li>
 *     <li>Call {@link GridHibernateAccessStrategyAdapter#update}.</li>
 *     <li>Commit DB transaction.</li>
 *     <li>Call {@link GridHibernateAccessStrategyAdapter#afterUpdate}.</li>
 * </ul>
 * In case if {@link GridHibernateAccessStrategyAdapter#lock} was called, but some other step fails and DB
 * transaction is rolled back then {@link GridHibernateAccessStrategyAdapter#unlock} is called for all locked keys.
 * <p>
 * Delete:
 * <ul>
 *     <li>Start DB transaction.</li>
 *     <li>Call {@link GridHibernateAccessStrategyAdapter#lock} for removing key.</li>
 *     <li>Execute database delete.</li>
 *     <li>Call {@link GridHibernateAccessStrategyAdapter#remove}.</li>
 *     <li>Commit DB transaction.</li>
 *     <li>Call {@link GridHibernateAccessStrategyAdapter#unlock}.</li>
 * </ul>
 * In case if {@link GridHibernateAccessStrategyAdapter#lock} was called, but some other step fails and DB
 * transaction is rolled back then {@link GridHibernateAccessStrategyAdapter#unlock} is called for all locked keys.
 * <p>
 * In case if custom SQL update query is executed Hibernate clears entire cache region,
 * for this case operations sequence is:
 * <ul>
 *     <li>Start DB transaction.</li>
 *     <li>Call {@link GridHibernateAccessStrategyAdapter#lockRegion}.</li>
 *     <li>Execute database query.</li>
 *     <li>Call {@link GridHibernateAccessStrategyAdapter#removeAll}.</li>
 *     <li>Commit DB transaction.</li>
 *     <li>Call {@link GridHibernateAccessStrategyAdapter#unlockRegion}.</li>
 * </ul>
 */
public abstract class GridHibernateAccessStrategyAdapter {
    /** */
    protected final GridCache<Object, Object> cache;

    /** Grid. */
    protected final Ignite ignite;

    /** */
    protected final GridLogger log;

    /**
     * @param ignite Grid.
     * @param cache Cache.
     */
    protected GridHibernateAccessStrategyAdapter(Ignite ignite, GridCache<Object, Object> cache) {
        this.cache = cache;
        this.ignite = ignite;

        log = ignite.log();
    }

    /**
     * Gets value from cache. Used by {@link RegionAccessStrategy#get}.
     *
     * @param key Key.
     * @return Cached value.
     * @throws CacheException If failed.
     */
    @Nullable protected Object get(Object key) throws CacheException {
        try {
            return cache.get(key);
        }
        catch (GridException e) {
            throw new CacheException(e);
        }
    }

    /**
     * Puts in cache value loaded from the database. Used by {@link RegionAccessStrategy#putFromLoad}.
     *
     * @param key Key.
     * @param val Value.
     * @param minimalPutOverride MinimalPut flag
     * @throws CacheException If failed.
     */
    protected void putFromLoad(Object key, Object val, boolean minimalPutOverride) throws CacheException {
        putFromLoad(key, val);
    }

    /**
     * Puts in cache value loaded from the database. Used by {@link RegionAccessStrategy#putFromLoad}.
     *
     * @param key Key.
     * @param val Value.
     * @throws CacheException If failed.
     */
    protected void putFromLoad(Object key, Object val) throws CacheException {
        try {
            cache.putx(key, val);
        }
        catch (GridException e) {
            throw new CacheException(e);
        }
    }

    /**
     * Called during database transaction execution before Hibernate attempts to update or remove given key.
     * Used by {@link RegionAccessStrategy#lockItem}.
     *
     * @param key Key.
     * @return Lock representation or {@code null}.
     * @throws CacheException If failed.
     */
    @Nullable protected abstract SoftLock lock(Object key) throws CacheException;

    /**
     * Called after Hibernate failed to update or successfully removed given key.
     * Used by {@link RegionAccessStrategy#unlockItem}.
     *
     * @param key Key.
     * @param lock The lock previously obtained from {@link #lock}
     * @throws CacheException If failed.
     */
    protected abstract void unlock(Object key, SoftLock lock) throws CacheException;

    /**
     * Called after Hibernate updated object in the database but before transaction completed.
     * Used by {@link EntityRegionAccessStrategy#update} and {@link NaturalIdRegionAccessStrategy#update}.
     *
     * @param key Key.
     * @param val Value.
     * @return {@code True} if operation updated cache.
     * @throws CacheException If failed.
     */
    protected abstract boolean update(Object key, Object val) throws CacheException;

    /**
     * Called after Hibernate updated object in the database and transaction successfully completed.
     * Used by {@link EntityRegionAccessStrategy#afterUpdate} and {@link NaturalIdRegionAccessStrategy#afterUpdate}.
     *
     * @param key Key.
     * @param val Value.
     * @param lock The lock previously obtained from {@link #lock}
     * @return {@code True} if operation updated cache.
     * @throws CacheException If failed.
     */
    protected abstract boolean afterUpdate(Object key, Object val, SoftLock lock) throws CacheException;

    /**
     * Called after Hibernate inserted object in the database but before transaction completed.
     * Used by {@link EntityRegionAccessStrategy#insert} and {@link NaturalIdRegionAccessStrategy#insert}.
     *
     * @param key Key.
     * @param val Value.
     * @return {@code True} if operation updated cache.
     * @throws CacheException If failed.
     */
    protected abstract boolean insert(Object key, Object val) throws CacheException;

    /**
     * Called after Hibernate inserted object in the database and transaction successfully completed.
     * Used by {@link EntityRegionAccessStrategy#afterInsert} and {@link NaturalIdRegionAccessStrategy#afterInsert}.
     *
     * @param key Key.
     * @param val Value.
     * @return {@code True} if operation updated cache.
     * @throws CacheException If failed.
     */
    protected abstract boolean afterInsert(Object key, Object val) throws CacheException;

    /**
     * Called after Hibernate removed object from database but before transaction completed.
     * Used by {@link RegionAccessStrategy#remove}.
     *
     * @param key Key,
     * @throws CacheException If failed.
     */
    protected abstract void remove(Object key) throws CacheException;

    /**
     * Called to remove object from cache without regard to transaction.
     * Used by {@link RegionAccessStrategy#evict}.
     *
     * @param key Key.
     * @throws CacheException If failed.
     */
    protected void evict(Object key) throws CacheException {
        evict(ignite, cache, key);
    }

    /**
     * Called to remove all data from cache without regard to transaction.
     * Used by {@link RegionAccessStrategy#evictAll}.
     *
     * @throws CacheException If failed.
     */
    protected void evictAll() throws CacheException {
        evictAll(cache);
    }

    /**
     * Called during database transaction execution before Hibernate executed
     * update operation which should invalidate entire cache region.
     * Used by {@link RegionAccessStrategy#lockRegion}.
     *
     * @throws CacheException If failed.
     * @return Lock representation or {@code null}.
     */
    @Nullable protected SoftLock lockRegion() throws CacheException {
        return null;
    }

    /**
     * Called after transaction clearing entire cache region completed.
     * Used by {@link RegionAccessStrategy#unlockRegion}.
     *
     * @param lock The lock previously obtained from {@link #lockRegion}
     * @throws CacheException If failed.
     */
    protected void unlockRegion(SoftLock lock) throws CacheException {
        // No-op.
    }

    /**
     * Called during database transaction execution to clear entire cache region after
     * Hibernate executed database update, but before transaction completed.
     * Used by {@link RegionAccessStrategy#removeAll}.
     *
     * @throws CacheException If failed.
     */
    protected final void removeAll() throws CacheException {
        evictAll();
    }

    /**
     * Called to remove object from cache without regard to transaction.
     *
     * @param ignite Grid.
     * @param cache Cache.
     * @param key Key.
     * @throws CacheException If failed.
     */
    static void evict(Ignite ignite, GridCacheProjection<Object,Object> cache, Object key) throws CacheException {
        try {
            ignite.compute(cache.gridProjection()).call(new ClearKeyCallable(key, cache.name()));
        }
        catch (GridException e) {
            throw new CacheException(e);
        }
    }

    /**
     * Called to remove all data from cache without regard to transaction.
     *
     * @param cache Cache.
     * @throws CacheException If failed.
     */
    static void evictAll(GridCacheProjection<Object,Object> cache) throws CacheException {
        try {
            cache.globalClearAll();
        }
        catch (GridException e) {
            throw new CacheException(e);
        }
    }

    /**
     * Callable invalidates given key.
     */
    private static class ClearKeyCallable implements Callable<Void>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @GridInstanceResource
        private Ignite ignite;

        /** */
        private Object key;

        /** */
        private String cacheName;

        /**
         * Empty constructor required by {@link Externalizable}.
         */
        public ClearKeyCallable() {
            // No-op.
        }

        /**
         * @param key Key to clear.
         * @param cacheName Cache name.
         */
        private ClearKeyCallable(Object key, String cacheName) {
            this.key = key;
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public Void call() throws GridException {
            GridCache<Object, Object> cache = ignite.cache(cacheName);

            assert cache != null;

            cache.clear(key);

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(key);

            U.writeString(out, cacheName);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            key = in.readObject();

            cacheName = U.readString(in);
        }
    }
}
