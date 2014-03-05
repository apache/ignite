/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLIENTDATA_HPP_INCLUDED
#define GRIDCLIENTDATA_HPP_INCLUDED

#include <string>

#include <gridgain/gridclientnode.hpp>
#include <gridgain/gridclienttypedef.hpp>
#include <gridgain/gridclientcacheflag.hpp>

/**
 * A data projection of grid client. Contains various methods for cache operations and metrics retrieval.
 */
class GRIDGAIN_API GridClientData {
public:
    /** Destructor. */
    virtual ~GridClientData() {};

    /**
     * Gets name of the remote cache.
     *
     * @return Name of the remote cache.
     */
    virtual std::string cacheName() const = 0;

    /**
     * Gets client data which will only contact specified remote grid node. By default, remote node
     * is determined based on {@link GridClientDataAffinity} provided - this method allows
     * to override default behavior and use only specified server for all cache operations.
     * <p>
     * Use this method when there are other than key-affinity reasons why a certain
     * node should be contacted.
     *
     * @param nodes Nodes to be included in the projection.
     * @return Client data which will only contact server with given node ID.
     * @throws GridClientException If resulting projection is empty.
     */
    virtual TGridClientDataPtr pinNodes(const TGridClientNodeList& nodes) = 0;

    /**
     * Puts value to cache.
     *
     * @param key Key.
     * @param val Value.
     * @return Whether value was actually put to cache.
     * @throws GridClientException In case of error.
     * @throws GridClientClosedException If client was closed manually.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     */
    virtual bool put(const GridClientVariant& key, const GridClientVariant& val) = 0;

    /**
     * Asynchronously puts value to cache.
     *
     * @param key Key
     * @param val Value.
     * @return Future that contains the result of the operation.
     */
    virtual TGridBoolFuturePtr putAsync(const GridClientVariant& key, const GridClientVariant& val) = 0;

    /**
     * Puts entries to cache.
     *
     * @param entries Entries.
     * @throws GridClientException In case of error.
     * @throws GridClientClosedException If client was closed manually.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     */
    virtual bool putAll(const TGridClientVariantMap& entries) = 0;

    /**
     * Asynchronously puts entries to cache.
     *
     * @param entries Entries.
     * @return Future.
     */
     virtual TGridBoolFuturePtr putAllAsync(const TGridClientVariantMap& entries) = 0;

    /**
     * Gets value from cache.
     *
     * @param key Key.
     * @return Value.
     * @throws GridClientException In case of error.
     * @throws GridClientClosedException If client was closed manually.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     */
     virtual GridClientVariant get(const GridClientVariant& key) = 0;

    /**
     * Asynchronously gets value from cache.
     *
     * @param key key.
     * @return Future.
     */
    virtual TGridClientFutureVariant getAsync(const GridClientVariant& key) = 0;

    /**
     * Gets entries from cache.
     *
     * @param keys Keys.
     * @throws GridClientException In case of error.
     * @throws GridClientClosedException If client was closed manually.
     * @return Entries.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     */
     virtual TGridClientVariantMap getAll(const TGridClientVariantSet& keys) = 0;

    /**
     * Asynchronously gets entries from cache.
     *
     * @param keys Keys to retrieve.
     * @return Future.
     */
    virtual TGridClientFutureVariantMap getAllAsync(const TGridClientVariantSet& keys) = 0;

    /**
     * Removes value from cache.
     *
     * @param key Key.
     * @return Whether value was actually removed.
     * @throws GridClientException In case of error.
     * @throws GridClientClosedException If client was closed manually.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     */
    virtual bool remove(const GridClientVariant& key) = 0;

    /**
     * Asynchronously removes value from cache.
     *
     * @param key key.
     * @return Future.
     */
    virtual TGridBoolFuturePtr removeAsync(const GridClientVariant& key) = 0;

    /**
     * Removes entries from cache.
     *
     * @param keys Keys.
     * @throws GridClientException In case of error.
     * @throws GridClientClosedException If client was closed manually.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     */
    virtual bool removeAll(const TGridClientVariantSet& keys) = 0;

    /**
     * Asynchronously removes entries from cache.
     *
     * @param keys Keys.
     * @return Future.
     */
    virtual TGridBoolFuturePtr removeAllAsync(const TGridClientVariantSet& keys) = 0;

    /**
     * Replaces value in cache.
     *
     * @param key Key.
     * @param val Value.
     * @return Whether value was actually replaced.
     * @throws GridClientException In case of error.
     * @throws GridClientClosedException If client was closed manually.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     */
    virtual bool replace(const GridClientVariant& key, const GridClientVariant& val) = 0;

    /**
     * Asynchronously replaces value in cache.
     *
     * @param key key.
     * @param val Value.
     * @return Future.
     */
    virtual TGridBoolFuturePtr replaceAsync(const GridClientVariant& key, const GridClientVariant& val) = 0;

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
    virtual bool cas(const GridClientVariant& key, const GridClientVariant& val1, 
        const GridClientVariant& val2) = 0;

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
            const GridClientVariant& val2) = 0;

    /**
     * Gets affinity node ID for the given key. This method will return <tt>null</tt> if no
     * affinity was configured for the given cache or there are no nodes in topology with
     * cache enabled.
     *
     * @param key Key.
     * @return Node ID.
     * @throws GridClientException In case of error.
     */
    virtual GridUuid affinity(const GridClientVariant& key) = 0;

    /**
     * Gets metrics for this cache.
     *
     * @return Cache metrics.
     * @throws GridClientException In case of error.
     * @throws GridClientClosedException If client was closed manually.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     */
    virtual GridClientDataMetrics metrics() = 0;

    /**
     * Asynchronously gets metrics for this cache.
     *
     * @return Cache metrics.
     */
    virtual TGridClientFutureDataMetrics metricsAsync() = 0;

    /**
     * Gets cache flags enabled on this data projection.
     *
     * @return Flags for this data projection (empty set if no flags have been set).
     */
    virtual std::set<GridClientCacheFlag> flags() = 0;

    /**
     * Creates new client data object with enabled cache flags.
     *
     * @param flags Optional cache flags to be enabled.
     * @return New client data object.
     * @throws GridClientException In case of error.
     */
    virtual TGridClientDataPtr flagsOn(const std::set<GridClientCacheFlag>& flags) = 0;

    /**
     * Creates new client data object with disabled cache flags.
     *
     * @param flags Cache flags to be disabled.
     * @return New client data object.
     * @throws GridClientException In case of error.
     */
    virtual TGridClientDataPtr flagsOff(const std::set<GridClientCacheFlag>& flags) = 0;
};

#endif // GRIDCLIENTDATA_HPP_INCLUDED
