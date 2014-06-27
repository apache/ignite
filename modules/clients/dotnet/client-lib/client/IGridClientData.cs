/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;
    using System.Collections.ObjectModel;
    using System.Collections.Generic;

    /**
     * <summary>
     * A data projection of grid client. Contains various methods for cache operations
     * and metrics retrieval.</summary>
     */
    public interface IGridClientData {
        /** <summary>Gets name of the remote cache.</summary> */
        String CacheName {
            get;
        }

        /** <summary>Gets cache flags of the remote cache.</summary> */
        ISet<GridClientCacheFlag> CacheFlags {
            get;
        }

        /**
         * <summary>
         * Creates new client data object with enabled cache flags.</summary>
         *
         * <param name="cacheFlags">Cache flags to be disabled.</param>
         * <returns>New client data object.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        IGridClientData CacheFlagsOn(ICollection<GridClientCacheFlag> cacheFlags);

        /**
         * <summary>
         * Creates new client data object with disabled cache flags.</summary>
         *
         * <param name="cacheFlags">Cache flags to be disabled.</param>
         * <returns>New client data object.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        IGridClientData CacheFlagsOff(ICollection<GridClientCacheFlag> cacheFlags);

        /**
         * <summary>
         * Gets client data which will only contact specified remote grid node. By default, remote node
         * is determined based on <see cref="IGridClientDataAffinity"/> provided - this method allows
         * to override default behavior and use only specified server for all cache operations.
         * <para/>
         * Use this method when there are other than <c>key-affinity</c> reasons why a certain
         * node should be contacted.</summary>
         *
         * <param name="node">Node to be contacted.</param>
         * <param name="nodes">Optional additional nodes.</param>
         * <returns>Client data which will only contact server with given node ID.</returns>
         * <exception cref="GridClientException">If resulting projection is empty.</exception>
         */
        IGridClientData PinNodes(IGridClientNode node, params IGridClientNode[] nodes);

        /**
         * <summary>
         * Gets pinned node or <c>null</c> if no node was pinned.</summary>
         *
         * <returns>Pinned node.</returns>
         */
        ICollection<IGridClientNode> PinnedNodes();

        /**
         * <summary>
         * Puts value to default cache.</summary>
         *
         * <param name="key">Key.</param>
         * <param name="val">Value.</param>
         * <returns>Whether value was actually put to cache.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        bool Put<TKey, TVal>(TKey key, TVal val);

        /**
         * <summary>
         * Asynchronously puts value to default cache.</summary>
         *
         * <param name="key">key.</param>
         * <param name="val">Value.</param>
         * <returns>Future whether value was actually put to cache.</returns>
         */
        IGridClientFuture<Boolean> PutAsync<TKey, TVal>(TKey key, TVal val);

        /**
         * <summary>
         * Puts entries to default cache.</summary>
         *
         * <param name="entries">Entries.</param>
         * <returns><c>True</c> if map contained more then one entry or if put succeeded in case of one entry,</returns>
         *      <c>false</c> otherwise
         * <exception cref="GridClientException">In case of error.</exception>
         */
        Boolean PutAll<TKey, TVal>(IDictionary<TKey, TVal> entries);

        /**
         * <summary>
         * Asynchronously puts entries to default cache.</summary>
         *
         * <param name="entries">Entries.</param>
         * <returns>Future whether this operation completes.</returns>
         */
        IGridClientFuture<Boolean> PutAllAsync<TKey, TVal>(IDictionary<TKey, TVal> entries);

        /**
         * <summary>
         * Gets value from default cache.</summary>
         *
         * <param name="key">Key.</param>
         * <returns>Value.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        TVal GetItem<TKey, TVal>(TKey key);

        /**
         * <summary>
         * Asynchronously gets value from default cache.</summary>
         *
         * <param name="key">key.</param>
         * <returns>Future with value for given key or with <c>null</c> if no value was cached.</returns>
         */
        IGridClientFuture<TVal> GetAsync<TKey, TVal>(TKey key);

        /**
         * <summary>
         * Gets entries from default cache.</summary>
         *
         * <param name="keys">Keys.</param>
         * <exception cref="GridClientException">In case of error.</exception>
         * <returns>Entries.</returns>
         */
        IDictionary<TKey, TVal> GetAll<TKey, TVal>(ICollection<TKey> keys);

        /**
         * <summary>
         * Asynchronously gets entries from default cache.</summary>
         *
         * <param name="keys">Keys.</param>
         * <returns>Future with entries retrieved from remote cache nodes.</returns>
         */
        IGridClientFuture<IDictionary<TKey, TVal>> GetAllAsync<TKey, TVal>(ICollection<TKey> keys);

        /**
         * <summary>
         * Removes value from default cache.</summary>
         *
         * <param name="key">Key.</param>
         * <returns>Whether value was actually removed.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        bool Remove<TKey>(TKey key);

        /**
         * <summary>
         * Asynchronously removes value from default cache.</summary>
         *
         * <param name="key">key.</param>
         * <returns>Future whether entry was actually removed.</returns>
         */
        IGridClientFuture<Boolean> RemoveAsync<TKey>(TKey key);

        /**
         * <summary>
         * Removes entries from default cache.</summary>
         *
         * <param name="keys">Keys.</param>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        void RemoveAll<TKey>(ICollection<TKey> keys);

        /**
         * <summary>
         * Asynchronously removes entries from default cache.</summary>
         *
         * <param name="keys">Keys.</param>
         * <returns>Future whether operation finishes.</returns>
         */
        IGridClientFuture RemoveAllAsync<TKey>(ICollection<TKey> keys);

        /**
         * <summary>
         * Replaces value in default cache.</summary>
         *
         * <param name="key">Key.</param>
         * <param name="val">Value.</param>
         * <returns>Whether value was actually replaced.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        bool Replace<TKey, TVal>(TKey key, TVal val);

        /**
         * <summary>
         * Asynchronously replaces value in default cache.</summary>
         *
         * <param name="key">key.</param>
         * <param name="val">Value.</param>
         * <returns>Future whether value was actually replaced.</returns>
         */
        IGridClientFuture<Boolean> ReplaceAsync<TKey, TVal>(TKey key, TVal val);

        /**
         * <summary>
         * Append requested value to already cached one. This method supports work with strings, lists and maps.
         * <para/>
         * Note that this operation is affinity-aware and will immediately contact
         * exactly the remote node on which this key is supposed to be cached (unless
         * some nodes were <c>pinned</c>).</summary>
         *
         * <param name="key">Key to manipulate cache value for.</param>
         * <param name="val">Value to append to the cached one.</param>
         * <returns>Whether value of entry was changed.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        Boolean Append<TKey, TVal>(TKey key, TVal val);

        /**
         * <summary>
         * Append requested value to already cached one. This method supports work with strings, lists and maps.
         * <para/>
         * Note that this operation is affinity-aware and will immediately contact
         * exactly the remote node on which this key is supposed to be cached (unless
         * some nodes were <c>pinned</c>).</summary>
         *
         * <param name="key">Key to manipulate cache value for.</param>
         * <param name="val">Value to append to the cached one.</param>
         * <returns>Future whether value of entry was changed.</returns>
         */
        IGridClientFuture<Boolean> AppendAsync<TKey, TVal>(TKey key, TVal val);

        /**
         * <summary>
         * Prepend requested value to already cached one. This method supports work with strings, lists and maps.
         * <para/>
         * Note that this operation is affinity-aware and will immediately contact
         * exactly the remote node on which this key is supposed to be cached (unless
         * some nodes were <c>pinned</c>).</summary>
         *
         * <param name="key">Key to manipulate cache value for.</param>
         * <param name="val">Value to prepend to the cached one.</param>
         * <returns>Whether value of entry was changed.</returns>
         * <exception>GridClientException In case of error.</exception>
         */
        Boolean Prepend<TKey, TVal>(TKey key, TVal val);

        /**
         * <summary>
         * Prepend requested value to already cached one. This method supports work with strings, lists and maps.
         * <para/>
         * Note that this operation is affinity-aware and will immediately contact
         * exactly the remote node on which this key is supposed to be cached (unless
         * some nodes were <c>pinned</c>).</summary>
         *
         * <param name="key">Key to manipulate cache value for.</param>
         * <param name="val">Value to prepend to the cached one.</param>
         * <returns>Future whether value of entry was changed.</returns>
         */
        IGridClientFuture<Boolean> PrependAsync<TKey, TVal>(TKey key, TVal val);

        /**
         * <summary>
         * Sets entry value to <c>val1</c> if current value is <c>val1</c>.
         * <para/>
         * If <c>val1</c> is <c>val2</c> and <c>val2</c> is equal to current value,
         * entry is removed from cache.
         * <para/>
         * If <c>val2</c> is <c>val1</c>, entry is created if it doesn't exist.
         * <para/>
         * If both <c>val1</c> and <c>val1</c> are <c>val1</c>, entry is removed.</summary>
         *
         * <param name="key">Key.</param>
         * <param name="val1">Value to set.</param>
         * <param name="val2">Check value.</param>
         * <returns>Whether value of entry was changed.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        bool Cas(String key, String val1, String val2);

        /**
         * <summary>
         * Asynchronously sets entry value to <c>val1</c> if current value is <c>val1</c>.
         * <para/>
         * If <c>val1</c> is <c>val2</c> and <c>val2</c> is equal to current value,
         * entry is removed from cache.
         * <para/>
         * If <c>val2</c> is <c>val1</c>, entry is created if it doesn't exist.
         * <para/>
         * If both <c>val1</c> and <c>null</c> are <c>null</c>, entry is removed.</summary>
         *
         * <param name="key">Key.</param>
         * <param name="val1">Value to set.</param>
         * <param name="val2">Check value.</param>
         * <returns>Future whether value of entry was changed.</returns>
         */
        IGridClientFuture<Boolean> CasAsync(String key, String val1, String val2);

        /**
         * <summary>
         * Gets affinity node ID for provided key. This method will return <c>null</c> if no
         * affinity was configured for the given cache or there are no nodes in topology with
         * cache enabled.</summary>
         *
         * <param name="key">Key.</param>
         * <returns>Node ID.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        Guid Affinity<TKey>(TKey key);

        /**
         * <summary>
         * Gets queries facade for this data projection. User can issue different queries against data 
         * porjection with returned facade.</summary>
         * 
         * <returns>Queries facade</returns>
         */
        IGridClientDataQueries Queries();

        /**
         * <summary>
         * Gets metrics for default cache.</summary>
         *
         * <returns>Cache metrics.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        IGridClientDataMetrics Metrics();

        /**
         * <summary>
         * Asynchronously gets metrics for default cache.</summary>
         *
         * <returns>Future with cache metrics.</returns>
         */
        IGridClientFuture<IGridClientDataMetrics> MetricsAsync();
    }
}
