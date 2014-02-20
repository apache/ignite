// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl {
    using System;
    using System.Linq;
    using GridGain.Client;
    using GridGain.Client.Balancer;
    using GridGain.Client.Util;
    using System.Collections.Generic;

    using N = GridGain.Client.IGridClientNode;

    /** <summary>Data projection that serves one cache instance and handles communication errors.</summary> */
    internal class GridClientDataImpl : GridClientAbstractProjection<GridClientDataImpl>, IGridClientData {
        /**
         * <summary>
         * Creates a data projection.</summary>
         *
         * <param name="cacheName">Cache name for projection.</param>
         * <param name="cfg">Projection configuration.</param>
         * <param name="nodes">Pinned nodes.</param>
         * <param name="filter">Node filter.</param>
         * <param name="balancer">Pinned node balancer.</param>
         */
        internal GridClientDataImpl(String cacheName, IGridClientProjectionConfig cfg, IEnumerable<N> nodes,
            Predicate<N> filter, IGridClientLoadBalancer balancer)
            : base(cfg, nodes, filter, balancer) {
            CacheName = cacheName;
            CacheFlags = new HashSet<GridClientCacheFlag>();
        }

        /** <inheritdoc /> */
        public String CacheName {
            get;
            private set;
        }

        /** <inheritdoc /> */
        public ISet<GridClientCacheFlag> CacheFlags {
            get;
            private set;
        }

        /** <inheritdoc /> */
        public IGridClientData CacheFlagsOn(ICollection<GridClientCacheFlag> cacheFlags) {
            GridClientDataImpl dataPrj = CreateProjection(_nodes, _filter, _balancer);

            dataPrj.CacheFlags = new HashSet<GridClientCacheFlag>(CacheFlags);
            dataPrj.CacheFlags.AddAll(cacheFlags);

            return dataPrj;
        }


        /** <inheritdoc /> */
        public IGridClientData CacheFlagsOff(ICollection<GridClientCacheFlag> cacheFlags) {
            GridClientDataImpl dataPrj = CreateProjection(_nodes, _filter, _balancer);

            dataPrj.CacheFlags = new HashSet<GridClientCacheFlag>(CacheFlags);

            foreach (GridClientCacheFlag flag in cacheFlags)
                dataPrj.CacheFlags.Remove(flag);

            return dataPrj;
        }

        /** <inheritdoc /> */
        public IGridClientData PinNodes(N node, params N[] nodes) {
            LinkedList<N> n = new LinkedList<N>();

            n.AddFirst(node);

            if (nodes != null)
                n.AddAll(nodes);

            return CreateProjection(n, null, null);
        }

        /** <inheritdoc /> */
        public ICollection<N> PinnedNodes() {
            return _nodes;
        }

        /** <inheritdoc /> */
        public bool Put<TKey, TVal>(TKey key, TVal val) {
            return PutAsync(key, val).Result;
        }

        /** <inheritdoc /> */
        public IGridClientFuture<Boolean> PutAsync<TKey, TVal>(TKey key, TVal val) {
            return WithReconnectHandling((conn, nodeId) => conn.CachePut<TKey, TVal>(CacheName, CacheFlags, key, val, nodeId), CacheName, key);
        }

        /** <inheritdoc /> */
        public Boolean PutAll<TKey, TVal>(IDictionary<TKey, TVal> entries) {
            return PutAllAsync(entries).Result;
        }

        /** <inheritdoc /> */
        public IGridClientFuture<Boolean> PutAllAsync<TKey, TVal>(IDictionary<TKey, TVal> entries) {
            if (entries.Count == 0)
                return new GridClientFinishedFuture<Boolean>(false);

            TKey key = entries.Keys.First();

            return WithReconnectHandling((conn, nodeId) => conn.CachePutAll<TKey, TVal>(CacheName, CacheFlags, entries, nodeId), CacheName, key);
        }

        /** <inheritdoc /> */
        public TVal GetItem<TKey, TVal>(TKey key) {
            return this.GetAsync<TKey, TVal>(key).Result;
        }

        /** <inheritdoc /> */
        public IGridClientFuture<TVal> GetAsync<TKey, TVal>(TKey key) {
            return WithReconnectHandling((conn, nodeId) => conn.CacheGet<TKey, TVal>(CacheName, CacheFlags, key, nodeId), CacheName, key);
        }

        /** <inheritdoc /> */
        public IDictionary<TKey, TVal> GetAll<TKey, TVal>(ICollection<TKey> keys) {
            return this.GetAllAsync<TKey, TVal>(keys).Result;
        }

        /** <inheritdoc /> */
        public IGridClientFuture<IDictionary<TKey, TVal>> GetAllAsync<TKey, TVal>(ICollection<TKey> keys) {
            if (keys.Count == 0)
                return new GridClientFinishedFuture<IDictionary<TKey, TVal>>(new Dictionary<TKey, TVal>());

            TKey key = keys.First();

            return WithReconnectHandling((conn, nodeId) => conn.CacheGetAll<TKey, TVal>(CacheName, CacheFlags, keys, nodeId), CacheName, key);
        }

        /** <inheritdoc /> */
        public bool Remove<TKey>(TKey key) {
            return RemoveAsync(key).Result;
        }

        /** <inheritdoc /> */
        public IGridClientFuture<Boolean> RemoveAsync<TKey>(TKey key) {
            return WithReconnectHandling((conn, nodeId) => conn.CacheRemove<TKey>(CacheName, CacheFlags, key, nodeId), CacheName, key);
        }

        /** <inheritdoc /> */
        public void RemoveAll<TKey>(ICollection<TKey> keys) {
            RemoveAllAsync(keys).WaitDone();
        }

        /** <inheritdoc /> */
        public IGridClientFuture RemoveAllAsync<TKey>(ICollection<TKey> keys) {
            if (keys.Count == 0)
                return new GridClientFinishedFuture<Boolean>(false);

            TKey key = keys.First();

            return WithReconnectHandling<Boolean>((conn, nodeId) => conn.CacheRemoveAll<TKey>(CacheName, CacheFlags, keys, nodeId), CacheName, key);
        }

        /** <inheritdoc /> */
        public bool Replace<TKey, TVal>(TKey key, TVal val) {
            return ReplaceAsync(key, val).Result;
        }

        /** <inheritdoc /> */
        public IGridClientFuture<Boolean> ReplaceAsync<TKey, TVal>(TKey key, TVal val) {
            return WithReconnectHandling((conn, nodeId) => conn.CacheReplace<TKey, TVal>(CacheName, CacheFlags, key, val, nodeId), CacheName, key);
        }
        
        /** <inheritdoc /> */
        public Boolean Append<TKey, TVal>(TKey key, TVal val) {
            return AppendAsync(key, val).Result;
        }

        /** <inheritdoc /> */
        public IGridClientFuture<Boolean> AppendAsync<TKey, TVal>(TKey key, TVal val) {
            return WithReconnectHandling((conn, nodeId)=> conn.CacheAppend<TKey, TVal>(CacheName, CacheFlags, key, val, nodeId), CacheName, key);
        }

        /** <inheritdoc /> */
        public Boolean Prepend<TKey, TVal>(TKey key, TVal val) {
            return PrependAsync(key, val).Result;
        }

        /** <inheritdoc /> */
        public IGridClientFuture<Boolean> PrependAsync<TKey, TVal>(TKey key, TVal val) {
            return WithReconnectHandling((conn, nodeId) => conn.CachePrepend<TKey, TVal>(CacheName, CacheFlags, key, val, nodeId), CacheName, key);
        }

        /** <inheritdoc /> */
        public bool Cas(String key, String val1, String val2) {
            return CasAsync(key, val1, val2).Result;
        }

        /** <inheritdoc /> */
        public IGridClientFuture<Boolean> CasAsync(String key, String val1, String val2) {
            return WithReconnectHandling((conn, nodeId) => conn.CacheCompareAndSet<String, String>(CacheName, CacheFlags, key, val1, val2, nodeId), CacheName, key);
        }

        /** <inheritdoc /> */
        public Guid Affinity<TKey>(TKey key) {
            IGridClientDataAffinity affinity = cfg.Affinity(CacheName);

            if (affinity == null)
                return Guid.Empty;

            return affinity.Node(key, ProjectionNodes()).Id;
        }

        /** <inheritdoc /> */
        public IGridClientDataMetrics Metrics() {
            return MetricsAsync().Result;
        }

        /** <inheritdoc /> */
        public IGridClientFuture<IGridClientDataMetrics> MetricsAsync() {
            return WithReconnectHandling((conn, nodeId) => conn.CacheMetrics(CacheName, CacheFlags, nodeId));
        }

        /** <inheritdoc /> */
        override protected GridClientDataImpl CreateProjectionImpl(IEnumerable<N> nodes,
            Predicate<N> filter, IGridClientLoadBalancer balancer) {
                return new GridClientDataImpl(CacheName, cfg, nodes, filter, balancer);
        }
    }
}
