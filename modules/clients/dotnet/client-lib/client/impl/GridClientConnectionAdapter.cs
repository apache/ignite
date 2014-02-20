// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl {
    using System;
    using System.Net;
    using System.Globalization;
    using System.Collections.Generic;
    using GridGain.Client.Util;
    using GridGain.Client.Ssl;

    using U = GridGain.Client.Util.GridClientUtils;
    using Dbg = System.Diagnostics.Debug;

    /**
     * <summary>
     * Facade for all possible network communications between client and server. Introduced to hide
     * protocol implementation (TCP, HTTP) from client code.</summary>
     */
    internal abstract class GridClientConnectionAdapter : IGridClientConnection {
        /** <summary>Topology</summary> */
        protected readonly GridClientTopology Top;

        /** <summary>Client id.</summary> */
        protected readonly Guid ClientId;

        /** <summary>Client credentials.</summary> */
        protected readonly Object Credentials;

        /** <summary>SSL context to use if ssl is enabled.</summary> */
        protected readonly IGridClientSslContext SslCtx;

        /**
         * <summary>
         * Creates a facade.</summary>
         *
         * <param name="clientId">Client identifier.</param>
         * <param name="srvAddr">Server address this connection connected to.</param>
         * <param name="sslCtx">SSL context to use if SSL is enabled, <c>null</c> otherwise.</param>
         * <param name="credentials">Authentication credentials</param>
         * <param name="top">Topology.</param>
         */
        protected GridClientConnectionAdapter(Guid clientId, IPEndPoint srvAddr, IGridClientSslContext sslCtx,
            Object credentials, GridClientTopology top) {
            this.ClientId = clientId;
            this.ServerAddress = srvAddr;
            this.Top = top;
            this.Credentials = credentials;
            this.SslCtx = sslCtx;
        }

        /** <summary>Server address this connection connected to</summary> */
        public IPEndPoint ServerAddress {
            get;
            private set;
        }

        /**
         * <summary>
         * Closes the connection.</summary>
         *
         * <param name="waitCompletion">If <c>true</c> will wait for all pending requests to be proceeded.</param>
         */
        public abstract void Close(bool waitCompletion);

        /**
         * <summary>
         * Closes connection facade if no requests are in progress.</summary>
         *
         * <returns>Idle timeout.</returns>
         * <returns><c>True</c> if no requests were in progress and client was closed, <c>True</c> otherwise.</returns>
         */
        public abstract bool CloseIfIdle(TimeSpan timeout);

        /** <inheritdoc /> */
        public virtual IGridClientFuture<Boolean> CachePut<TKey, TVal>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, TKey key, TVal val, Guid destNodeId) {
            IDictionary<TKey, TVal> map = new Dictionary<TKey, TVal>();

            map.Add(key, val);

            return CachePutAll<TKey, TVal>(cacheName, cacheFlags, map, destNodeId);
        }

        /** <inheritdoc /> */
        public virtual IGridClientFuture<TVal> CacheGet<TKey, TVal>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, TKey key, Guid destNodeId) {
            IGridClientFuture<IDictionary<TKey, TVal>> res = CacheGetAll<TKey, TVal>(cacheName, cacheFlags, new TKey[] { key }, destNodeId);

            return new GridClientFinishedFuture<TVal>(() => {
                TVal val;

                res.Result.TryGetValue(key, out val);

                return val;
            });
        }

        /** <inheritdoc /> */
        public abstract IGridClientFuture<Boolean> CacheRemove<T>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, T key, Guid destNodeId);

        /** <inheritdoc /> */
        public abstract IGridClientFuture<Boolean> CachePutAll<TKey, TVal>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, IDictionary<TKey, TVal> entries, Guid destNodeId);

        /** <inheritdoc /> */
        public abstract IGridClientFuture<IDictionary<TKey, TVal>> CacheGetAll<TKey, TVal>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, IEnumerable<TKey> keys, Guid destNodeId);

        /** <inheritdoc /> */
        public abstract IGridClientFuture<Boolean> CacheRemoveAll<TKey>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, IEnumerable<TKey> keys, Guid destNodeId);

        /** <inheritdoc /> */
        public abstract IGridClientFuture<Boolean> CacheReplace<TKey, TVal>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, TKey key, TVal val, Guid destNodeId);

        /** <inheritdoc /> */
        public abstract IGridClientFuture<Boolean> CacheAppend<K, V>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, K key, V val, Guid destNodeId);

        /** <inheritdoc /> */
        public abstract IGridClientFuture<Boolean> CachePrepend<K, V>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, K key, V val, Guid destNodeId);
        
        /** <inheritdoc /> */
        public abstract IGridClientFuture<Boolean> CacheCompareAndSet<TKey, TVal>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, TKey key, TVal val1, TVal val2, Guid destNodeId);

        /** <inheritdoc /> */
        public abstract IGridClientFuture<IGridClientDataMetrics> CacheMetrics(String cacheName, ISet<GridClientCacheFlag> cacheFlags, Guid destNodeId);

        /** <inheritdoc /> */
        public abstract IGridClientFuture<T> Execute<T>(String taskName, Object taskArg, Guid destNodeId);

        /** <inheritdoc /> */
        public abstract IGridClientFuture<IGridClientNode> Node(Guid id, bool includeAttrs, bool includeMetrics, Guid destNodeId);

        /** <inheritdoc /> */
        public abstract IGridClientFuture<IGridClientNode> Node(String ipAddr, bool includeAttrs, bool includeMetrics, Guid destNodeId);

        /** <inheritdoc /> */
        public abstract IGridClientFuture<IList<IGridClientNode>> Topology(bool includeAttrs, bool includeMetrics, Guid destNodeId);

        /** <inheritdoc /> */
        public abstract IGridClientFuture<IList<String>> Log(String path, int fromLine, int toLine, Guid destNodeId);

        /**
         * <summary>
         * Encodes cache flags to bit map.</summary>
         *
         * <param name="flagSet">Set of flags to be encoded.</param>
         * <returns>Bit map.</returns>
         */
        protected int encodeCacheFlags(ICollection<GridClientCacheFlag> flagSet) {
            int bits = 0;

            foreach (GridClientCacheFlag flag in flagSet)
                bits |= (int) flag;

            return bits;
        }
        
        /**
         * <summary>
         * Convert the map representation of the cache metrics to an equivalent objects.</summary>
         *
         * <param name="data">Map of raw cache metrics.</param>
         * <returns>Converted cache metrics.</returns>
         */
        protected IGridClientDataMetrics parseCacheMetrics(IDictionary<String, Object> data) {
            var m = new GridClientDataMetrics();
            
            m.CreateTime = U.Timestamp(asLong(data["createTime"]));
            m.WriteTime = U.Timestamp(asLong(data["writeTime"]));
            m.ReadTime = U.Timestamp(asLong(data["readTime"]));
            m.Reads = asLong(data["reads"]);
            m.Writes = asLong(data["writes"]);
            m.Hits = asLong(data["hits"]);
            m.Misses = asLong(data["misses"]);

            return m;
        }

        /**
         * <summary>
         * Convert the map representation of the node metrics to an equivalent objects.</summary>
         *
         * <param name="data">Map of raw node metrics.</param>
         * <returns>Converted node metrics.</returns>
         */
        protected IGridClientNodeMetrics parseNodeMetrics(IDictionary<String, Object> data) {
            GridClientNodeMetrics m = new GridClientNodeMetrics();

            m.StartTime = U.Timestamp(safeLong(data, "startTime"));

            m.WaitingJobs.Average = safeDouble(data, "averageWaitingJobs");
            m.WaitingJobs.Current = safeLong(data, "currentWaitingJobs");
            m.WaitingJobs.Maximum = safeLong(data, "maximumWaitingJobs");
            m.WaitingJobs.Total = m.WaitingJobs.Maximum;

            m.ExecutedJobs.Average = safeDouble(data, "averageActiveJobs");
            m.ExecutedJobs.Current = safeLong(data, "currentActiveJobs");
            m.ExecutedJobs.Maximum = safeLong(data, "maximumActiveJobs");
            m.ExecutedJobs.Total = safeLong(data, "totalExecutedJobs");

            m.RejectedJobs.Average = safeDouble(data, "averageRejectedJobs");
            m.RejectedJobs.Current = safeLong(data, "currentRejectedJobs");
            m.RejectedJobs.Maximum = safeLong(data, "maximumRejectedJobs");
            m.RejectedJobs.Total = safeLong(data, "totalRejectedJobs");

            m.CancelledJobs.Average = safeDouble(data, "averageCancelledJobs");
            m.CancelledJobs.Current = safeLong(data, "currentCancelledJobs");
            m.CancelledJobs.Maximum = safeLong(data, "maximumCancelledJobs");
            m.CancelledJobs.Total = safeLong(data, "totalCancelledJobs");

            m.JobWaitTime.Average = TimeSpan.FromMilliseconds(safeDouble(data, "averageJobWaitTime"));
            m.JobWaitTime.Current = TimeSpan.FromMilliseconds(safeDouble(data, "currentJobWaitTime"));
            m.JobWaitTime.Maximum = TimeSpan.FromMilliseconds(safeDouble(data, "maximumJobWaitTime"));
            m.JobWaitTime.Total = TimeSpan.FromMilliseconds(m.JobWaitTime.Average.TotalMilliseconds * m.ExecutedJobs.Total);

            m.JobExecuteTime.Average = TimeSpan.FromMilliseconds(safeDouble(data, "averageJobExecuteTime"));
            m.JobExecuteTime.Current = TimeSpan.FromMilliseconds(safeDouble(data, "currentJobExecuteTime"));
            m.JobExecuteTime.Maximum = TimeSpan.FromMilliseconds(safeDouble(data, "maximumJobExecuteTime"));
            m.JobExecuteTime.Total = TimeSpan.FromMilliseconds(m.JobExecuteTime.Average.TotalMilliseconds * m.ExecutedJobs.Total);

            m.StartTime = U.Timestamp(safeLong(data, "startTime"));
            m.NodeStartTime = U.Timestamp(safeLong(data, "nodeStartTime"));
            m.UpTime = TimeSpan.FromMilliseconds(safeLong(data, "upTime"));
            m.LastUpdateTime = U.Timestamp(safeLong(data, "lastUpdateTime"));
            m.IdleTimeTotal = TimeSpan.FromMilliseconds(safeLong(data, "totalIdleTime"));
            //m.IdleTimeCurrent = (safeLong(data, "currentIdleTime"));

            m.CpuCount = (int)safeLong(data, "totalCpus");
            m.CpuAverageLoad = safeDouble(data, "averageCpuLoad");
            m.CpuCurrentLoad = safeLong(data, "currentCpuLoad");

            m.FileSystemFreeSpace = safeLong(data, "fileSystemFreeSpace");
            m.FileSystemTotalSpace = safeLong(data, "fileSystemTotalSpace");
            m.FileSystemUsableSpace = safeLong(data, "fileSystemUsableSpace");

            m.HeapMemoryInitialized = safeLong(data, "heapMemoryInitialized");
            m.HeapMemoryUsed = safeLong(data, "heapMemoryUsed");
            m.HeapMemoryCommitted = safeLong(data, "heapMemoryCommitted");
            m.HeapMemoryMaximum = safeLong(data, "heapMemoryMaximum");

            m.NonHeapMemoryInitialized = safeLong(data, "nonHeapMemoryInitialized");
            m.NonHeapMemoryUsed = safeLong(data, "nonHeapMemoryUsed");
            m.NonHeapMemoryCommitted = safeLong(data, "nonHeapMemoryCommitted");
            m.NonHeapMemoryMaximum = safeLong(data, "nonHeapMemoryMaximum");

            m.ThreadCount.Current = safeLong(data, "currentThreadCount");
            m.ThreadCount.Maximum = safeLong(data, "maximumThreadCount");
            m.ThreadCount.Total = safeLong(data, "totalStartedThreadCount");

            m.DaemonThreadCount = safeLong(data, "currentDaemonThreadCount");

            m.LastDataVersion = safeLong(data, "lastDataVersion");

            return m;
        }

        /**
         * <summary>
         * Convert the string representation of the cache mode names to an equivalent enumerated objects.</summary>
         *
         * <param name="rawCaches">Map of raw cache modes.</param>
         * <returns>Converted cache modes.</returns>
         */
        protected IDictionary<String, GridClientCacheMode> parseCacheModes(IDictionary<String, String> rawCaches) {
            var caches = new GridClientNullDictionary<String, GridClientCacheMode>();

            foreach (KeyValuePair<String, String> e in rawCaches)
                try {
                    caches.Add(e.Key, parseCacheMode(e.Value));
                }
                catch (ArgumentException x) {
                    Dbg.WriteLine("Invalid cache mode received from remote node (will ignore)" +
                        " [srv={0}, cacheName={1}, cacheMode={2}, e={3}]", ServerAddress, e.Key, e.Value, x);
                }

            return caches;
        }

        /**
         * <summary>
         * Converts the string representation of the cache mode name to an equivalent enumerated object.</summary>
         *
         * <param name="val">A string containing the name to convert.</param>
         * <returns>Parsed cache mode.</returns>
         */
        private static GridClientCacheMode parseCacheMode(String val) {
            switch (val) {
                case "LOCAL":
                    return GridClientCacheMode.Local;
                case "REPLICATED":
                    return GridClientCacheMode.Replicated;
                case "PARTITIONED":
                    return GridClientCacheMode.Partitioned;
                default:
                    throw new ArgumentException("Unsupported cache mode: " + val);
            }
        }

        /**
         * <summary>
         * Convert any object to double value.</summary>
         *
         * <param name="val">Json object to convert.</param>
         * <returns>Double value.</returns>
         */
        protected static Double AsDouble(Object val) {
            return val == null ? 0 : Double.Parse(val + "", CultureInfo.InvariantCulture);
        }

        /**
         * <summary>
         * Convert any object to long value.</summary>
         *
         * <param name="val">Json object to convert.</param>
         * <returns>Long value.</returns>
         */
        protected static long asLong(Object val) {
            return val == null ? 0 : long.Parse(val + "", CultureInfo.InvariantCulture);
        }

        /**
         * <summary>
         * Safely get long value from the dictionary by the key.</summary>
         * 
         * <param name="map">Map to get value from.</param>
         * <param name="key">Key in the map to get value for.</param>
         * <returns>Parsed value if map contains specified key and corresponding value 
         * can be parsed to expected type, <c>-1</c> - otherwise.</returns>
         */
        private static long safeLong(IDictionary<String, Object> map, String key) {
            return safe<long>(map, key, -1, long.TryParse);
        }

        /**
         * <summary>
         * Safely get double value from the dictionary by the key.</summary>
         * 
         * <param name="map">Map to get value from.</param>
         * <param name="key">Key in the map to get value for.</param>
         * <returns>Parsed value if map contains specified key and corresponding value 
         * can be parsed to expected type, <c>-1</c> - otherwise.</returns>
         */
        private static double safeDouble(IDictionary<String, Object> map, String key) {
            return safe<double>(map, key, -1, double.TryParse);
        }

        /**
         * <summary>
         * Safely get value from the dictionary or return failure value.</summary>
         * 
         * <param name="map">Map to get value from.</param>
         * <param name="key">Key in the map to get value for.</param>
         * <param name="def">Default value to return if get operation fails.</param>
         * <param name="parser">Parser callback.</param>
         * <returns>Parsed value if map contains specified key and corresponding value can be parsed to expected type.</returns>
         */
        private static T safe<T>(IDictionary<String, Object> map, String key, T def, Parser<T> parser) {
            Object obj;
            T val;

            return map.TryGetValue(key, out obj) && parser(obj + "", out val) ? val : def;
        }

        /**
         * <summary>
         * Parser safely converts string value to expected type.</summary>
         * 
         * <param name="s">String value to parse.</param>
         * <param name="res">Variable to save parsed result to on success</param>
         * <returns><c>True</c> if operation succeed, <c>false</c> - otherwize.</returns>
         */
        private delegate bool Parser<T>(string s, out T res);
    }
}
