/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.spi.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Indexing SPI allows user to index cache content. Using indexing SPI user can index data in cache and run SQL,
 * TEXT or individual field queries against these indexes. Usually indexing SPI is used by caches by name
 * (see {@link GridCacheConfiguration#getIndexingSpiName()}). Logically storage is organized into separate spaces.
 * Usually cache name will be used as space name, so multiple caches can write to single indexing SPI instance.
 * <p>
 * Functionality of this SPI is exposed to {@link GridCacheQueries} interface:
 * <ul>
 *      <li>{@link GridCacheQueries#createSqlQuery(Class, String)}</li>
 *      <li>{@link GridCacheQueries#createSqlFieldsQuery(String)}</li>
 *      <li>{@link GridCacheQueries#createFullTextQuery(Class, String)}</li>
 * </ul>
 * <p>
 * The default indexing SPI implementation is
 * {@gglink org.gridgain.grid.spi.indexing.h2.GridH2IndexingSpi} which uses H2 database engine
 * for data indexing and querying. User can implement his own indexing SPI and use his own data structures
 * and query language instead of SQL. SPI can be configured for grid using {@link org.apache.ignite.configuration.IgniteConfiguration#getIndexingSpi()}.
 * <p>
 * GridGain comes with following built-in indexing SPI implementations:
 * <ul>
 *      <li>{@gglink org.gridgain.grid.spi.indexing.h2.GridH2IndexingSpi}</li>
 * </ul>
 * <p>
 * <b>NOTE:</b> this SPI (i.e. methods in this interface) should never be used directly. SPIs provide
 * internal view on the subsystem and is used internally by GridGain kernal. In rare use cases when
 * access to a specific implementation of this SPI is required - an instance of this SPI can be obtained
 * via {@link org.apache.ignite.Ignite#configuration()} method to check its configuration properties or call other non-SPI
 * methods. Note again that calling methods from this interface on the obtained instance can lead
 * to undefined behavior and explicitly not supported.
 */
public interface GridIndexingSpi extends IgniteSpi {
    /**
     * Queries individual fields (generally used by JDBC drivers).
     *
     * @param spaceName Space name.
     * @param qry Query.
     * @param params Query parameters.
     * @param filters Space name and key filters.
     * @return Query result.
     * @throws org.gridgain.grid.spi.IgniteSpiException If failed.
     */
    public <K, V> GridIndexingFieldsResult queryFields(@Nullable String spaceName, String qry,
        Collection<Object> params, GridIndexingQueryFilter filters) throws IgniteSpiException;

    /**
     * Executes regular query.
     *
     * @param spaceName Space name.
     * @param qry Query.
     * @param params Query parameters.
     * @param type Query return type.
     * @param filters Space name and key filters.
     * @return Queried rows.
     * @throws org.gridgain.grid.spi.IgniteSpiException If failed.
     */
    public <K, V> IgniteSpiCloseableIterator<GridIndexingKeyValueRow<K, V>> query(@Nullable String spaceName, String qry,
        Collection<Object> params, GridIndexingTypeDescriptor type, GridIndexingQueryFilter filters)
        throws IgniteSpiException;

    /**
     * Executes text query.
     *
     * @param spaceName Space name.
     * @param qry Text query.
     * @param type Query return type.
     * @param filters Space name and key filter.
     * @return Queried rows.
     * @throws org.gridgain.grid.spi.IgniteSpiException If failed.
     */
    public <K, V> IgniteSpiCloseableIterator<GridIndexingKeyValueRow<K, V>> queryText(@Nullable String spaceName, String qry,
        GridIndexingTypeDescriptor type, GridIndexingQueryFilter filters) throws IgniteSpiException;

    /**
     * Gets size of index for given type or -1 if it is a unknown type.
     *
     * @param spaceName Space name.
     * @param desc Type descriptor.
     * @return Objects number.
     * @throws org.gridgain.grid.spi.IgniteSpiException If failed.
     */
    public long size(@Nullable String spaceName, GridIndexingTypeDescriptor desc) throws IgniteSpiException;

    /**
     * Registers type if it was not known before or updates it otherwise.
     *
     * @param spaceName Space name.
     * @param desc Type descriptor.
     * @throws org.gridgain.grid.spi.IgniteSpiException If failed.
     * @return {@code True} if type was registered, {@code false} if for some reason it was rejected.
     */
    public boolean registerType(@Nullable String spaceName, GridIndexingTypeDescriptor desc) throws IgniteSpiException;

    /**
     * Unregisters type and removes all corresponding data.
     *
     * @param spaceName Space name.
     * @param type Type descriptor.
     * @throws org.gridgain.grid.spi.IgniteSpiException If failed.
     */
    public void unregisterType(@Nullable String spaceName, GridIndexingTypeDescriptor type) throws IgniteSpiException;

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
     * @throws org.gridgain.grid.spi.IgniteSpiException If failed.
     */
    public <K, V> void store(@Nullable String spaceName, GridIndexingTypeDescriptor type, GridIndexingEntity<K> key,
        GridIndexingEntity<V> val, byte[] ver, long expirationTime) throws IgniteSpiException;

    /**
     * Removes index entry by key.
     *
     * @param spaceName Space name.
     * @param key Key.
     * @return {@code True} if removed by this operation, {@code false} otherwise.
     * @throws org.gridgain.grid.spi.IgniteSpiException If failed.
     */
    public <K> boolean remove(@Nullable String spaceName, GridIndexingEntity<K> key) throws IgniteSpiException;

    /**
     * Will be called when entry with given key is swapped.
     *
     * @param spaceName Space name.
     * @param swapSpaceName Swap space name.
     * @param key Key.
     * @throws org.gridgain.grid.spi.IgniteSpiException If failed.
     */
    public <K> void onSwap(@Nullable String spaceName, String swapSpaceName, K key) throws IgniteSpiException;

    /**
     * Will be called when entry with given key is unswapped.
     *
     * @param spaceName Space name.
     * @param key Key.
     * @param val Value.
     * @param valBytes Value bytes.
     * @throws org.gridgain.grid.spi.IgniteSpiException If failed.
     */
    public <K, V> void onUnswap(@Nullable String spaceName, K key, V val, byte[] valBytes) throws IgniteSpiException;

    /**
     * Marshaller to be used by SPI.
     *
     * @param marshaller Marshaller.
     */
    public void registerMarshaller(GridIndexingMarshaller marshaller);

    /**
     * Registers space in this SPI.
     *
     * @param spaceName Space name.
     * @throws org.gridgain.grid.spi.IgniteSpiException If failed.
     */
    public void registerSpace(String spaceName) throws IgniteSpiException;

    /**
     * Rebuilds all indexes of given type.
     *
     * @param spaceName Space name.
     * @param type Type descriptor.
     */
    public void rebuildIndexes(@Nullable String spaceName, GridIndexingTypeDescriptor type);
}
