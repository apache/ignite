/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl {
    using System;
    using System.Net;
    using System.Collections.Generic;
    using GridGain.Client.Util;
    using GridGain.Client.Impl.Query;
    using GridGain.Client.Impl.Message;

    /**
     * <summary>
     * Facade for all possible network communications between client and server. Introduced to hide
     * protocol implementation (TCP, HTTP) from client code.</summary>
     */
    internal interface IGridClientConnection {
        /**
         * <summary>
         * Closes connection facade.</summary>
         *
         * <param name="waitCompletion">If <c>true</c> this method will wait until all pending requests are handled.</param>
         */
        void Close(bool waitCompletion);

        /**
         * <summary>
         * Closes connection facade if no requests are in progress.</summary>
         *
         * <returns>Idle timeout.</returns>
         * <returns><c>True</c> if no requests were in progress and client was closed, <c>True</c> otherwise.</returns>
         */
        bool CloseIfIdle(TimeSpan timeout);

        /** <summary>Server address this connection linked to.</summary> */
        IPEndPoint ServerAddress {
            get;
        }

        /**
         * <summary>
         * Puts key-value pair into cache.</summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="cacheFlags">Cache flags.</param>
         * <param name="key">Key.</param>
         * <param name="val">Value.</param>
         * <param name="destNodeId">Node ID to route request to.</param>
         * <returns>If value was actually put.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<Boolean> CachePut<K, V>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, K key, V val, Guid destNodeId);

        /**
         * <summary>
         * Gets entry from the cache for specified key.</summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="cacheFlags">Cache flags.</param>
         * <param name="key">Key.</param>
         * <param name="destNodeId">Node ID to route request to.</param>
         * <returns>Value.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<V> CacheGet<K, V>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, K key, Guid destNodeId);

        /**
         * <summary>
         * Removes entry from the cache for specified key.</summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="cacheFlags">Cache flags.</param>
         * <param name="key">Key.</param>
         * <param name="destNodeId">Node ID to route request to.</param>
         * <returns>Whether entry was actually removed.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<Boolean> CacheRemove<K>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, K key, Guid destNodeId);

        /**
         * <summary>
         * Puts bundle of entries into cache.</summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="cacheFlags">Cache flags.</param>
         * <param name="entries">Entries.</param>
         * <param name="destNodeId">Node ID to route request to.</param>
         * <returns><c>True</c> if map contained more then one entry or if put succeeded in case of one entry,</returns>
         *      <c>false</c> otherwise
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<Boolean> CachePutAll<K, V>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, IDictionary<K, V> entries, Guid destNodeId);

        /**
         * <summary>
         * Gets bundle of entries for specified keys from the cache.</summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="cacheFlags">Cache flags.</param>
         * <param name="keys">Keys.</param>
         * <param name="destNodeId">Node ID to route request to.</param>
         * <returns>Entries.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<IDictionary<K, V>> CacheGetAll<K, V>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, IEnumerable<K> keys, Guid destNodeId);

        /**
         * <summary>
         * Removes bundle of entries for specified keys from the cache.</summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="cacheFlags">Cache flags.</param>
         * <param name="keys">Keys.</param>
         * <param name="destNodeId">Node ID to route request to.</param>
         * <returns>Whether operation finishes.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<Boolean> CacheRemoveAll<K>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, IEnumerable<K> keys, Guid destNodeId);

        /**
         * <summary>
         * Replace key-value pair in cache if already exist.</summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="cacheFlags">Cache flags.</param>
         * <param name="key">Key.</param>
         * <param name="val">Value.</param>
         * <param name="destNodeId">Node ID to route request to.</param>
         * <returns>Whether value was actually replaced.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<Boolean> CacheReplace<K, V>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, K key, V val, Guid destNodeId);

        /**
         * <summary>
         * Append value to already cached for specified key.</summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="cacheFlags">Cache flags.</param>
         * <param name="key">Key of cached value.</param>
         * <param name="val">Value to append.</param>
         * <param name="destNodeId">Node ID to route request to.</param>
         * <returns>Whether value was actually appended.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<Boolean> CacheAppend<K, V>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, K key, V val, Guid destNodeId);

        /**
         * <summary>
         * Prepend value to already cached for specified key.</summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="cacheFlags">Cache flags.</param>
         * <param name="key">Key of cached value.</param>
         * <param name="val">Value to prepend.</param>
         * <param name="destNodeId">Node ID to route request to.</param>
         * <returns>Whether value was actually appended.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<Boolean> CachePrepend<K, V>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, K key, V val, Guid destNodeId);

        /**
         * <summary>
         * <table>
         *     <tr><th>New value</th><th>Actual/old value</th><th>Behaviour</th></tr>
         *     <tr><td>null     </td><td>null   </td><td>Remove entry for key.</td></tr>
         *     <tr><td>newVal   </td><td>null   </td><td>Put newVal into cache if such key doesn't exist.</td></tr>
         *     <tr><td>null     </td><td>oldVal </td><td>Remove if actual value oldVal is equals to value in cache.</td></tr>
         *     <tr><td>newVal   </td><td>oldVal </td><td>Replace if actual value oldVal is equals to value in cache.</td></tr>
         * </table>
         * </summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="cacheFlags">Cache flags.</param>
         * <param name="key">Key.</param>
         * <param name="newVal">Value 1.</param>
         * <param name="oldVal">Value 2.</param>
         * <param name="destNodeId">Node ID to route request to.</param>
         * <returns>Whether new value was actually set.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<Boolean> CacheCompareAndSet<K, V>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, K key, V newVal, V oldVal, Guid destNodeId);

        /**
         * <summary>
         * Gets cache metrics for the key.</summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="cacheFlags">Cache flags.</param>
         * <param name="destNodeId">Node ID to route request to.</param>
         * <returns>Metrics.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<IGridClientDataMetrics> CacheMetrics(String cacheName, ISet<GridClientCacheFlag> cacheFlags, Guid destNodeId);

        /**
         * <summary>
         * Requests task execution with specified name.</summary>
         *
         * <param name="taskName">Task name.</param>
         * <param name="taskArg">Task argument.</param>
         * <param name="destNodeId">Node ID to route request to.</param>
         * <returns>Task execution result.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<T> Execute<T>(String taskName, Object taskArg, Guid destNodeId);

        /**
         * <summary>
         * Requests node by its ID.</summary>
         *
         * <param name="id">Node ID.</param>
         * <param name="includeAttrs">Whether to include attributes.</param>
         * <param name="includeMetrics">Whether to include metrics.</param>
         * <param name="destNodeId">Node ID to route request to.</param>
         * <returns>Node.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<IGridClientNode> Node(Guid id, bool includeAttrs, bool includeMetrics, Guid destNodeId);

        /**
         * <summary>
         * Requests node by its IP address.</summary>
         *
         * <param name="ipAddr">IP address.</param>
         * <param name="includeAttrs">Whether to include attributes.</param>
         * <param name="includeMetrics">Whether to include metrics.</param>
         * <param name="destNodeId">Node ID to route request to.</param>
         * <returns>Node.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<IGridClientNode> Node(String ipAddr, bool includeAttrs, bool includeMetrics, Guid destNodeId);

        /**
         * <summary>
         * Requests actual grid topology.</summary>
         *
         * <param name="includeAttrs">Whether to include attributes.</param>
         * <param name="includeMetrics">Whether to include metrics.</param>
         * <param name="destNodeId">Node ID to route request to.</param>
         * <returns>Nodes.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<IList<IGridClientNode>> Topology(bool includeAttrs, bool includeMetrics, Guid destNodeId);

        /**
         * <summary>
         * Requests server log entries in specified scope.</summary>
         *
         * <param name="path">Log file path.</param>
         * <param name="fromLine">Index of start line that should be retrieved.</param>
         * <param name="toLine">Index of end line that should be retrieved.</param>
         * <param name="destNodeId">Node ID to route request to.</param>
         * <returns>Log file contents.</returns>
         * <exception cref="GridClientConnectionResetException">In case of error.</exception>
         * <exception cref="GridClientClosedException">If client was manually closed before request was sent over network.</exception>
         */
        IGridClientFuture<IList<String>> Log(String path, int fromLine, int toLine, Guid destNodeId);

        /**
         * <summary>
         * Starts query execution on given node.</summary>
         * 
         * <param name="qry">Query bean to create request from.</param>
         * <param name="args">Query execution arguments.</param>
         * <param name="destNodeId">Destination node ID to execute query on.</param>
         */
        IGridClientFuture<GridClientDataQueryResult> ExecuteQuery<T>(GridClientDataQueryBean<T> qry, Object[] args, Guid destNodeId);

        /**
         * <summary>
         * Fetches next query results page from destination node.</summary>
         * 
         * <param name="qryId">Query ID to fetch data for.</param>
         * <param name="pageSize">Page size to fetch</param>
         * <param name="destNodeId">Destination node ID to fetch data from.</param>
         */
        IGridClientFuture<GridClientDataQueryResult> FetchNextPage(long qryId, int pageSize, Guid destNodeId);

        /**
         * <summary>
         * Requests index rebuild.</summary>
         * 
         * <param name="clsName">Optional class name to rebuild indexes for.</param>
         */
        IGridClientFuture<GridClientDataQueryResult> RebuildIndexes(String clsName);
    }
}
