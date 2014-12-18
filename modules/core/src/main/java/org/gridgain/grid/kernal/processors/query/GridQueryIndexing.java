/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.indexing.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.query.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Abstraction for internal indexing implementation.
 */
public interface GridQueryIndexing {
    /**
     * Starts indexing.
     *
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void start(GridKernalContext ctx) throws IgniteCheckedException;

    /**
     * Stops indexing.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void stop() throws IgniteCheckedException;


    /**
     * Runs two step query.
     *
     * @param qry Query.
     * @return Future.
     */
    public IgniteFuture<GridCacheSqlResult> queryTwoStep(GridCacheTwoStepQuery qry);

    /**
     * Queries individual fields (generally used by JDBC drivers).
     *
     * @param spaceName Space name.
     * @param qry Query.
     * @param params Query parameters.
     * @param filters Space name and key filters.
     * @return Query result.
     * @throws IgniteCheckedException If failed.
     */
    public <K, V> GridQueryFieldsResult queryFields(@Nullable String spaceName, String qry,
        Collection<Object> params, GridIndexingQueryFilter filters) throws IgniteCheckedException;

    /**
     * Executes regular query.
     *
     * @param spaceName Space name.
     * @param qry Query.
     * @param params Query parameters.
     * @param type Query return type.
     * @param filters Space name and key filters.
     * @return Queried rows.
     * @throws IgniteCheckedException If failed.
     */
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> query(@Nullable String spaceName, String qry,
        Collection<Object> params, GridQueryTypeDescriptor type, GridIndexingQueryFilter filters) throws IgniteCheckedException;

    /**
     * Executes text query.
     *
     * @param spaceName Space name.
     * @param qry Text query.
     * @param type Query return type.
     * @param filters Space name and key filter.
     * @return Queried rows.
     * @throws IgniteCheckedException If failed.
     */
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryText(@Nullable String spaceName, String qry,
        GridQueryTypeDescriptor type, GridIndexingQueryFilter filters) throws IgniteCheckedException;

    /**
     * Gets size of index for given type or -1 if it is a unknown type.
     *
     * @param spaceName Space name.
     * @param desc Type descriptor.
     * @param filters Filters.
     * @return Objects number.
     * @throws IgniteCheckedException If failed.
     */
    public long size(@Nullable String spaceName, GridQueryTypeDescriptor desc, GridIndexingQueryFilter filters)
        throws IgniteCheckedException;

    /**
     * Registers type if it was not known before or updates it otherwise.
     *
     * @param spaceName Space name.
     * @param desc Type descriptor.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if type was registered, {@code false} if for some reason it was rejected.
     */
    public boolean registerType(@Nullable String spaceName, GridQueryTypeDescriptor desc) throws IgniteCheckedException;

    /**
     * Unregisters type and removes all corresponding data.
     *
     * @param spaceName Space name.
     * @param type Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    public void unregisterType(@Nullable String spaceName, GridQueryTypeDescriptor type) throws IgniteCheckedException;

    /**
     * Updates index. Note that key is unique for space, so if space contains multiple indexes
     * the key should be removed from indexes other than one being updated.
     *
     * @param spaceName Space name.
     * @param type Value type.
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     * @param expirationTime Expiration time or 0 if never expires.
     * @throws IgniteCheckedException If failed.
     */
    public void store(@Nullable String spaceName, GridQueryTypeDescriptor type, Object key, Object val, byte[] ver,
        long expirationTime) throws IgniteCheckedException;

    /**
     * Removes index entry by key.
     *
     * @param spaceName Space name.
     * @param key Key.
     * @throws IgniteCheckedException If failed.
     */
    public void remove(@Nullable String spaceName, Object key) throws IgniteCheckedException;

    /**
     * Will be called when entry with given key is swapped.
     *
     * @param spaceName Space name.
     * @param key Key.
     * @throws IgniteCheckedException If failed.
     */
    public void onSwap(@Nullable String spaceName, Object key) throws IgniteCheckedException;

    /**
     * Will be called when entry with given key is unswapped.
     *
     * @param spaceName Space name.
     * @param key Key.
     * @param val Value.
     * @param valBytes Value bytes.
     * @throws IgniteCheckedException If failed.
     */
    public void onUnswap(@Nullable String spaceName, Object key, Object val, byte[] valBytes) throws IgniteCheckedException;

    /**
     * Rebuilds all indexes of given type.
     *
     * @param spaceName Space name.
     * @param type Type descriptor.
     */
    public void rebuildIndexes(@Nullable String spaceName, GridQueryTypeDescriptor type);
}
