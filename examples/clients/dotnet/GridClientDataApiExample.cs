/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;
    using System.Text;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;

    using GridGain.Client.Balancer;

    using X = System.Console;

    /**
     * <summary>
     * This example demonstrates use of C# remote Data Client API. To execute
     * this example you should start an instance of <c>GridClientCacheExampleNodeStartup</c>
     * Java class which will start up a GridGain node with proper configuration.
     * <para/>
     * You can also start a stand-alone GridGain instance by passing the path
     * to configuration file to <c>ggstart.{sh|bat}</c> script, like so:
     * <c>ggstart.sh examples/config/example-cache.xml'</c>.
     * <para/>
     * After node has been started this example creates a client and performs several cache
     * operations.
     * <para/>
     * Note that different nodes cannot share the same port for rest services. If you want
     * to start more than one node on the same physical machine you must provide different
     * configurations for each node. Otherwise, this example would not work.</summary>
     */
    public class GridClientDataApiExample {
        /** <summary>Grid node address to connect to.</summary> */
        private static readonly String ServerAddress = "127.0.0.1";

        /** <summary>Count of keys to be stored in this example.</summary> */
        public static readonly int KeysCount = 10;

        /**
         * <summary>
         * Starts up an empty node with specified cache configuration, then runs client cache example and client
         * compute example.</summary>
         *
         * <exception cref="GridClientException">If failed.</exception>
         */
        [STAThread]
        public static void Main() {
            /* Enable debug messages. */
            //Debug.Listeners.Add(new TextWriterTraceListener(System.Console.Out));

            IGridClient client = CreateClient();

            try {
                // Show grid topology.
                X.WriteLine(">>> Client created, current grid topology: " + ToString(client.Compute().Nodes()));

                // Random node ID.
                Guid randNodeId = client.Compute().Nodes()[0].Id;

                // Get client projection of grid partitioned cache.
                IGridClientData rmtCache = client.Data("partitioned");

                IList<String> keys = new List<String>(KeysCount);

                // Put some values to the cache.
                for (int i = 0; i < KeysCount; i++) {
                    String iKey = i + "";

                    // Put request will go exactly to the primary node for this key.
                    rmtCache.Put(iKey, "val-" + i);

                    Guid nodeId = rmtCache.Affinity(iKey);

                    X.WriteLine(">>> Storing key " + iKey + " on node " + nodeId);

                    keys.Add(iKey);
                }

                // Pin a remote node for communication. All further communication
                // on returned projection will happen through this pinned node.
                IGridClientData prj = rmtCache.PinNodes(client.Compute().Node(randNodeId));

                // Request batch from our local node in pinned mode.
                IDictionary<String, Object> vals = prj.GetAll<String, Object>(keys);

                foreach (KeyValuePair<String, Object> entry in vals)
                    X.WriteLine(">>> Loaded cache entry [key=" + entry.Key + ", val=" + entry.Value + ']');

                // After nodes are pinned the list of pinned nodes may be retrieved.
                X.WriteLine(">>> Pinned nodes: " + ToString(prj.PinnedNodes()));

                // Keys may be stored asynchronously.
                IGridClientFuture<Boolean> futPut = rmtCache.PutAsync("0", "new value for 0");

                X.WriteLine(">>> Result of asynchronous put: " + (futPut.Result ? "success" : "failure"));

                IDictionary<Guid, IDictionary<String, String>> keyVals = new Dictionary<Guid, IDictionary<String, String>>();

                // Batch puts are also supported.
                // Here we group key-value pairs by their affinity node ID to ensure
                // the least amount of network trips possible.
                for (int i = 0; i < KeysCount; i++) {
                    String iKey = i + "";

                    Guid nodeId = rmtCache.Affinity(iKey);

                    IDictionary<String, String> m;

                    if (!keyVals.TryGetValue(nodeId, out m))
                        keyVals.Add(nodeId, m = new Dictionary<String, String>());

                    m.Add(iKey, "val-" + i);
                }

                foreach (IDictionary<String, String> kvMap in keyVals.Values)
                    // Affinity-aware bulk put operation - it will connect to the
                    // affinity node for provided keys.
                    rmtCache.PutAll(kvMap);

                // Asynchronous batch put is available as well.
                ICollection<IGridClientFuture> futs = new LinkedList<IGridClientFuture>();

                foreach (IDictionary<String, String> kvMap in keyVals.Values) {
                    IGridClientFuture futPutAll = rmtCache.PutAllAsync(kvMap);

                    futs.Add(futPutAll);
                }

                // Wait for all futures to complete.
                foreach (IGridClientFuture fut in futs)
                    fut.WaitDone();

                // Of course there's getting value by key functionality.
                String key = 0 + "";

                X.WriteLine(">>> Value for key " + key + " is " + rmtCache.GetItem<String, Object>(key));

                // Asynchronous gets, too.
                IGridClientFuture<String> futVal = rmtCache.GetAsync<String, String>(key);

                X.WriteLine(">>> Asynchronous value for key " + key + " is " + futVal.Result);

                // Multiple values can be fetched at once. Here we batch our get
                // requests by affinity nodes to ensure least amount of network trips.
                foreach (KeyValuePair<Guid, IDictionary<String, String>> nodeEntry in keyVals) {
                    Guid nodeId = nodeEntry.Key;
                    ICollection<String> keyCol = nodeEntry.Value.Keys;

                    // Since all keys in our getAll(...) call are mapped to the same primary node,
                    // grid cache client will pick this node for the request, so we only have one
                    // network trip here.
                    X.WriteLine(">>> Values from node [nodeId=" + nodeId + ", values=" + ToString(rmtCache.GetAll<String, Object>(keyCol)) + ']');
                }

                // Multiple values may be retrieved asynchronously, too.
                // Here we retrieve all keys at ones. Since this request
                // will be sent to some grid node, this node may not be
                // the primary node for all keys and additional network
                // trips will have to be made within grid.
                IGridClientFuture<IDictionary<String, Object>> futVals = rmtCache.GetAllAsync<String, Object>(keys);

                X.WriteLine(">>> Asynchronous values for keys are " + ToString(futVals.Result));

                // Contents of cache may be removed one by one synchronously.
                // Again, this operation is affinity aware and only the primary
                // node for the key is contacted.
                bool res = rmtCache.Remove(0 + "");

                X.WriteLine(">>> Result of removal: " + (res ? "success" : "failure"));

                // ... and asynchronously.
                IGridClientFuture<Boolean> futRes = rmtCache.RemoveAsync(1 + "");

                X.WriteLine(">>> Result of asynchronous removal is: " + (futRes.Result ? "success" : "failure"));

                // Multiple entries may be removed at once synchronously...
                rmtCache.RemoveAll(new String[] { 2 + "", 3 + "" });

                // ... and asynchronously.
                IGridClientFuture futResAll = rmtCache.RemoveAllAsync(new String[] { 3 + "", 4 + "" });

                futResAll.WaitDone();

                // Values may also be replaced.
                res = rmtCache.Replace(0 + "", "new value for 0");

                X.WriteLine(">>> Result for replace for nonexistent key is " + (res ? "success" : "failure"));

                // Asynchronous replace is supported, too.
                futRes = rmtCache.ReplaceAsync("" + 0, "newest value for 0");

                X.WriteLine(">>> Result for asynchronous replace for nonexistent key is " +
                    (futRes.Result ? "success" : "failure"));

                // Compare and set are implemented, too.
                res = rmtCache.Cas("" + 0, "new value for 0", null);

                X.WriteLine(">>> Result for put using cas for key that didn't have value yet is " +
                    (res ? "success" : "failure"));

                // CAS can be asynchronous.
                futRes = rmtCache.CasAsync("" + 0, "newest value for 0", "new value for 0");

                X.WriteLine(">>> Result for put using asynchronous cas is " + (futRes.Result ? "success" : "failure"));

                // It's possible to obtain cache metrics using data client API.
                X.WriteLine(">>> Cache metrics : " + rmtCache.Metrics());

                // Global and per key metrics retrieval can be asynchronous, too.
                IGridClientFuture<IGridClientDataMetrics> futMetrics = rmtCache.MetricsAsync();

                X.WriteLine(">>> Cache asynchronous metrics : " + futMetrics.Result);
            }
            catch (GridClientException e) {
                Console.WriteLine("Unexpected grid client exception happens: {0}", e);
            }
            finally {
                GridClientFactory.StopAll();
            }
        }

        /**
         * <summary>
         * This method will create a client with default configuration. Note that this method expects that
         * first node will bind rest binary protocol on default port. It also expects that partitioned cache is
         * configured in grid.</summary>
         *
         * <returns>Client instance.</returns>
         * <exception cref="GridClientException">If client could not be created.</exception>
         */
        private static IGridClient CreateClient() {
            var cacheCfg = new GridClientDataConfiguration();

            // Set remote cache name.
            cacheCfg.Name = "partitioned";

            // Set client partitioned affinity for this cache.
            cacheCfg.Affinity = new GridClientPartitionAffinity();

            var cfg = new GridClientConfiguration();

            cfg.DataConfigurations.Add(cacheCfg);

            // Point client to a local node. Note that this server is only used
            // for initial connection. After having established initial connection
            // client will make decisions which grid node to use based on collocation
            // with key affinity or load balancing.
            cfg.Servers.Add(ServerAddress + ':' + GridClientConfiguration.DefaultTcpPort);

            return GridClientFactory.Start(cfg);
        }

        /**
         * <summary>
         * Concatenates the members of a collection, using the specified separator between each member.</summary>
         *
         * <param name="list">A collection that contains the objects to concatenate.</param>
         * <param name="separator">The string to use as a separator.</param>
         * <returns>A string that consists of the members of values delimited by the separator string.</return>
         */
        public static String ToString<T>(IEnumerable<T> list, String separator = ",") {
            return list == null ? "null" : String.Join(separator, list);
        }

        /**
         * <summary>
         * Concatenates the members of a map, using the specified separator between each member.</summary>
         *
         * <param name="map">A map that contains the objects to concatenate.</param>
         * <param name="separator">The string to use as a separator.</param>
         * <returns>A string that consists of the members of values delimited by the separator string.</return>
         */
        public static String ToString<TKey, TVal>(IDictionary<TKey, TVal> map, String separator = ",") {
            if (map == null)
                return "null";

            StringBuilder sb = new StringBuilder("[");
            String prefix = "";

            foreach(var pair in map) {
                sb.Append(prefix).Append(pair.Key).Append("=").Append(pair.Value);
                prefix = separator;
            }

            return sb.Append("]").ToString();
        }
    }
}
