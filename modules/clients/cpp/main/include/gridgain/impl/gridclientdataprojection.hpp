/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_DATA_PROJECTION_IMPL_HPP
#define GRID_CLIENT_DATA_PROJECTION_IMPL_HPP

#include <string>
#include <vector>

#include "gridgain/gridclientdata.hpp"
#include "gridgain/impl/gridclientprojection.hpp"
#include "gridgain/impl/utils/gridthreadpool.hpp"

/**
 * GridClientData implementation.
 */
class GridClientDataProjectionImpl: public GridClientProjectionImpl, public GridClientData {
public:
    /**
     * Public constructor.
     *
     * @param sharedData Client shared data.
     * @param prjLsnr Projection listener.
     * @param cacheName Cache name.
     * @param filter Projection filter.
     * @param threadPool Client thread pool.
     * @param flags Cache flags.
     */
    GridClientDataProjectionImpl(
        TGridClientSharedDataPtr sharedData,
        GridClientProjectionListener& prjLsnr,
        const std::string& cacheName,
        TGridClientNodePredicatePtr filter,
        TGridThreadPoolPtr& threadPool,
        const std::set<GridClientCacheFlag>& flags);

    /** Destructor. */
    virtual ~GridClientDataProjectionImpl() {
    }

    /**
     * @return The name of the cache this projection represents.
     */
    virtual std::string cacheName() const;

    /**
     * Gets client data which will only contact specified remote grid node. By default, remote node
     * is determined based on {@link GridClientDataAffinity} provided - this method allows
     * to override default behavior and use only specified server for all cache operations.
     * <p>
     * Use this method when there are other than <tt>key-affinity</tt> reasons why a certain
     * node should be contacted.
     *
     * @param nodes Optional additional nodes.
     * @return Client data which will only contact server with given node ID.
     * @throws GridClientException If resulting projection is empty.
     */
    virtual TGridClientDataPtr pinNodes(const TGridClientNodeList& nodes);

    /**
     * Puts value to default cache.
     *
     * @param key The cache key.
     * @param val The key value.
     *
     * @return Whether value was actually put to cache.
     * @throws GridClientException In case of error.
     * @throws GridClientClosedException If client was closed manually.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     */
    virtual bool put(const GridClientVariant& key, const GridClientVariant& val);

    /**
     * Asynchronously puts value to default cache.
     *
     * @param key The cache key.
     * @param val The key value.
     *
     * @return Operation future.
     */
    virtual TGridBoolFuturePtr putAsync(const GridClientVariant& key, const GridClientVariant& val);

    /**
     * Puts entries to the cache.
     *
     * @param entries Entries.
     * @throws GridClientException In case of error.
     * @throws GridClientClosedException If client was closed manually.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     */
    virtual bool putAll(const TGridClientVariantMap& entries);

    /**
     * Asynchronously puts entries to default cache.
     *
     * @param entries Entries.
     * @return Future.
     */
    virtual TGridBoolFuturePtr putAllAsync(const TGridClientVariantMap& entries);

    /**
     * Gets value from default cache.
     *
     * @param key Key.
     * @return Value.
     * @throws GridClientException In case of error.
     * @throws GridClientClosedException If client was closed manually.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     */
    virtual GridClientVariant get(const GridClientVariant& key);

    /**
     * Asynchronously gets value from default cache.
     *
     * @param key key.
     * @param fut Future.
     */
    virtual TGridClientFutureVariant getAsync(const GridClientVariant& key);

    /**
     * Gets entries from default cache.
     *
     * @param keys Keys.
     * @throws GridClientException In case of error.
     * @throws GridClientClosedException If client was closed manually.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @return Entries.
     */
    virtual TGridClientVariantMap getAll(const TGridClientVariantSet& keys);

    /**
     * Asynchronously gets entries from default cache.
     *
     * @param keys Keys.
     * @return Future.
     */
    virtual TGridClientFutureVariantMap getAllAsync(const TGridClientVariantSet& keys);

    /**
     * Removes value from default cache.
     *
     * @param key Key.
     * @return Whether value was actually removed.
     * @throws GridClientException In case of error.
     * @throws GridClientClosedException If client was closed manually.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     */
    virtual bool remove(const GridClientVariant& key);

    /**
     * Asynchronously removes value from default cache.
     *
     * @param key key.
     * @return Future.
     */
    virtual TGridBoolFuturePtr removeAsync(const GridClientVariant& key);

    /**
     * Removes entries from default cache.
     *
     * @param keys Keys.
     * @throws GridClientException In case of error.
     * @throws GridClientClosedException If client was closed manually.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     */
    virtual bool removeAll(const TGridClientVariantSet& keys);

    /**
     * Asynchronously removes entries from default cache.
     *
     * @param keys Keys.
     * @return Future.
     */
    virtual TGridBoolFuturePtr removeAllAsync(const TGridClientVariantSet& keys);

    /**
     * Replaces value in default cache.
     *
     * @param key Key.
     * @param val Value.
     * @return Whether value was actually replaced.
     * @throws GridClientException In case of error.
     * @throws GridClientClosedException If client was closed manually.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     */
    virtual bool replace(const GridClientVariant& key, const GridClientVariant& val);

    /**
     * Asynchronously replaces value in default cache.
     *
     * @param key key.
     * @param val Value.
     * @return Future.
     */
    virtual TGridBoolFuturePtr replaceAsync(const GridClientVariant& key, const GridClientVariant& val);

    /**
     * Sets entry value to <tt>val1</tt> if current value is <tt>val2</tt>.
     * <p>
     * If <tt>val1</tt> is <tt>null</tt> and <tt>val2</tt> is equal to current value,
     * entry is removed from cache.
     * <p>
     * If <tt>val2</tt> is <tt>null</tt>, entry is created if it doesn't exist.
     * <p>
     * If both <tt>val1</tt> and <tt>val2</tt> are <tt>null</tt>, entry is removed.
     *
     * @param key Key.
     * @param val1 Value to set.
     * @param val2 Check value.
     * @return Whether value of entry was changed.
     * @throws GridClientException In case of error.
     * @throws GridClientClosedException If client was closed manually.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     */
    virtual bool cas(const GridClientVariant& key, const GridClientVariant& val1, const GridClientVariant& val2);

    /**
     * Asynchronously sets entry value to <tt>val1</tt> if current value is <tt>val2</tt>.
     * <p>
     * If <tt>val1</tt> is <tt>null</tt> and <tt>val2</tt> is equal to current value,
     * entry is removed from cache.
     * <p>
     * If <tt>val2</tt> is <tt>null</tt>, entry is created if it doesn't exist.
     * <p>
     * If both <tt>val1</tt> and <tt>val2</tt> are <tt>null</tt>, entry is removed.
     *
     * @param key Key.
     * @param val1 Value to set.
     * @param val2 Check value.
     * @return Future.
     */
    virtual TGridBoolFuturePtr casAsync(const GridClientVariant& key, const GridClientVariant& val1,
            const GridClientVariant& val2);

    /**
     * Gets affinity node ID for provided key. This method will return <tt>null</tt> if no
     * affinity was configured for the given cache or there are no nodes in topology with
     * cache enabled.
     *
     * @param key Key.
     * @return Node ID.
     * @throws GridClientException In case of error.
     */
    virtual GridUuid affinity(const GridClientVariant& key);

    /**
     * Gets metrics for default cache.
     *
     * @return Cache metrics.
     * @throws GridClientException In case of error.
     * @throws GridClientClosedException If client was closed manually.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     */
    virtual GridClientDataMetrics metrics();

    /**
     * Asynchronously gets metrics for default cache.
     *
     * @return Cache metrics.
     */
    virtual TGridClientFutureDataMetrics metricsAsync();

    /**
     * Gets cache flags enabled on this data projection.
     *
     * @return Flags for this data projection (empty set if no flags have been set).
     */
    virtual std::set<GridClientCacheFlag> flags();

    /**
     * Creates new client data object with enabled cache flags.
     *
     * @param flags Optional cache flags to be enabled.
     * @return New client data object.
     * @throws GridClientException In case of error.
     */
    virtual TGridClientDataPtr flagsOn(const std::set<GridClientCacheFlag>& flags);

    /**
     * Creates new client data object with disabled cache flags.
     *
     * @param flags Cache flags to be disabled.
     * @return New client data object.
     * @throws GridClientException In case of error.
     */
    virtual TGridClientDataPtr flagsOff(const std::set<GridClientCacheFlag>& flags);

    /**
     * Invalidates this data instance. This is done by the client to indicate
     * that is has been stopped. After this call, all interface methods
     * will throw GridClientClosedException.
     */
    void invalidate();

private:
    /** Cache name. */
    std::string prjCacheName;

    /** List of subprojections. */
    std::vector<TGridClientDataPtr> subProjections;

    /** Invalidated flag. */
    TGridAtomicBool invalidated;

    /** Thread pool for running async operations. */
    TGridThreadPoolPtr threadPool;

    /** Cache flags for this projection. */
    std::set<GridClientCacheFlag> prjFlags;
};

#endif
