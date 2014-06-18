/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl {
    using System;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using System.Threading;
    using GridGain.Client;
    using GridGain.Client.Balancer;
    using GridGain.Client.Util;

    using U = GridGain.Client.Util.GridClientUtils;
    using Dbg = System.Diagnostics.Debug;

    /** <summary>Client implementation.</summary> */
    internal sealed class GridClientImpl : IGridClient, IGridClientProjectionConfig {
        /* Delay between two idle connections check. */
        private static readonly TimeSpan IDLE_CHECK_DELAY = TimeSpan.FromSeconds(1);

        /** <summary>Client configuration.</summary> */
        private readonly GridClientConfiguration cfg;

        /** <summary>Connections manager.</summary> */
        private readonly GridClientConnectionManager connMgr;

        /** <summary>Topology.</summary> */
        private readonly GridClientTopology top;

        /** <summary>Main compute projection.</summary> */
        private readonly GridClientComputeImpl _compute;

        /** <summary>Data projections.</summary> */
        private readonly IDictionary<String, GridClientDataImpl> dataMap = new GridClientNullDictionary<String, GridClientDataImpl>();

        /** <summary>Connection idle checker thread.</summary> */
        private readonly Thread idleCheckThread;

        /** <summary>Topology updater thread.</summary> */
        private readonly Thread topUpdateThread;

        /** <summary>Topology updater balancer.</summary> */
        private readonly GridClientRoundRobinBalancer topUpdateBalancer = new GridClientRoundRobinBalancer();

        /** <summary>State of the client.</summary> */
        private volatile bool closed = false;

        /**
         * <summary>
         * Creates a new client based on a given configuration.</summary>
         *
         * <param name="id">Client identifier.</param>
         * <param name="cfg0">Client configuration.</param>
         * <exception cref="GridClientException">If client configuration is incorrect.</exception>
         * <exception cref="GridClientServerUnreachableException">If none of the servers specified in configuration can be reached.</exception>
         */
        public GridClientImpl(Guid id, GridClientConfiguration cfg0) {
            Id = id;

            cfg = new GridClientConfiguration(cfg0);

            ICollection<IPEndPoint> srvs = ParseServers(cfg.Servers);
            ICollection<IPEndPoint> routers = ParseServers(cfg.Routers);

            top = new GridClientTopology(id, cfg.IsTopologyCacheEnabled);

            Action<Object> addTopLsnr = o => {
                var lsnr = o as IGridClientTopologyListener;

                if (lsnr != null)
                    top.AddTopologyListener(lsnr);
            };

            // Add to topology as listeners.
            foreach (GridClientDataConfiguration dataCfg in cfg.DataConfigurations)
                addTopLsnr(dataCfg.Affinity);

            addTopLsnr(cfg.Balancer);
            addTopLsnr(topUpdateBalancer);

            if (srvs.Count == 0)
                // Use routers for initial connection.
                srvs = routers; 
            else
                // Disable routers for connection manager.
                routers = new HashSet<IPEndPoint>(); 

            connMgr = new GridClientConnectionManager(Id, top, routers, cfg.Credentials, cfg.SslContext, cfg.ConnectTimeout);

            int retries = 3;

            while (true) {
                IGridClientConnection conn = null;

                try {
                    // Create connection to a server from the list of endpoints.
                    conn = connMgr.connection(srvs);

                    // Request topology at start to determine which node we connected to.
                    // Also this request validates TCP connection is alive.
                    conn.Topology(false, false, Guid.Empty).WaitDone();

                    break;
                }
                catch (GridClientAuthenticationException) {
                    if (conn != null)
                        conn.Close(false);

                    top.Dispose();

                    throw;
                }
                catch (GridClientException e) {
                    if (conn != null)
                        conn.Close(false);

                    if (retries-- <= 0) {
                        top.Dispose();

                        throw new GridClientException("Failed to update grid topology.", e);
                    }
                }
            }

            IDictionary<String, GridClientCacheMode> overallCaches = new GridClientNullDictionary<String, GridClientCacheMode>();

            // Topology is now updated, so we can identify current connection.
            foreach (GridClientNodeImpl node in top.Nodes())
                foreach (KeyValuePair<String, GridClientCacheMode> pair in node.Caches)
                    overallCaches[pair.Key] = pair.Value;

            foreach (KeyValuePair<String, GridClientCacheMode> entry in overallCaches)
                if (Affinity(entry.Key) is GridClientPartitionAffinity && entry.Value != GridClientCacheMode.Partitioned)
                    Dbg.WriteLine(typeof(GridClientPartitionAffinity) + " is used for a cache configured " +
                        "for non-partitioned mode [cacheName=" + entry.Key + ", cacheMode=" + entry.Value + ']');

            idleCheckThread = new Thread(checkIdle);

            idleCheckThread.Name = "grid-check-idle-worker--client#" + id;

            idleCheckThread.Start();

            topUpdateThread = new Thread(updateTopology);

            topUpdateThread.Name = "grid-topology-update-worker--client#" + id;

            topUpdateThread.Start();

            _compute = new GridClientComputeImpl(this, null, null, cfg.Balancer);

            Dbg.WriteLine("Client started. Id: " + Id);
        }

        /** <inheritdoc /> */
        public Guid Id {
            get;
            private set;
        }

        /**
         * <summary>
         * Closes client.</summary>
         *
         * <param name="waitCompletion">If <c>true</c> will wait for all pending requests to be proceeded.</param>
         */
        public void stop(bool waitCompletion) {
            lock (this) {
                if (closed)
                    return;

                closed = true;
            }

            // Shutdown the topology refresh thread and connections clean-up thread.
            topUpdateThread.Interrupt();
            idleCheckThread.Interrupt();

            topUpdateThread.Join();
            idleCheckThread.Join();

            // Close connections.
            connMgr.closeAll(waitCompletion);

            // Shutdown listener notification.
            top.Dispose();

            foreach (GridClientDataConfiguration dataCfg in cfg.DataConfigurations) {
                var lsnr = dataCfg.Affinity as IGridClientTopologyListener;

                if (lsnr != null)
                    RemoveTopologyListener(lsnr);
            }

            Dbg.WriteLine("Client stopped. Id: " + Id);
        }

        /** <inheritdoc /> */
        public IGridClientData Data() {
            return Data(null);
        }

        /** <inheritdoc /> */
        public IGridClientData Data(String cacheName) {
            lock (dataMap) {
                GridClientDataImpl data;

                if (!dataMap.TryGetValue(cacheName, out data)) {
                    GridClientDataConfiguration dataCfg = cfg.DataConfiguration(cacheName);

                    if (dataCfg == null && cacheName != null)
                        throw new GridClientException("Data configuration for given cache name was not provided: " +
                            cacheName);

                    IGridClientLoadBalancer balancer = dataCfg != null ? dataCfg.PinnedBalancer :
                        new GridClientRandomBalancer();

                    Predicate<IGridClientNode> filter = delegate(IGridClientNode e) {
                        return e.Caches.ContainsKey(cacheName);
                    };

                    data = new GridClientDataImpl(cacheName, this, null, filter, balancer);

                    dataMap.Add(cacheName, data);
                }

                return data;
            }
        }

        /** <inheritdoc /> */
        public IGridClientCompute Compute() {
            return _compute;
        }

        /** <inheritdoc /> */
        public void AddTopologyListener(IGridClientTopologyListener lsnr) {
            top.AddTopologyListener(lsnr);
        }

        /** <inheritdoc /> */
        public void RemoveTopologyListener(IGridClientTopologyListener lsnr) {
            top.RemoveTopologyListener(lsnr);
        }

        /** <inheritdoc /> */
        public ICollection<IGridClientTopologyListener> TopologyListeners() {
            return top.TopologyListeners();
        }

        /** <summary>Connections manager.</summary> */
        public GridClientConnectionManager ConnectionManager {
            get {
                return connMgr;
            }
        }

        /** <summary>Topology instance.</summary> */
        public GridClientTopology Topology {
            get {
                return top;
            }
        }

        /**
         * <summary>
         * Gets data affinity for a given cache name.</summary>
         *
         * <param name="cacheName">Name of cache for which affinity is obtained. Data configuration with this name</param>
         *      must be configured at client startup.
         * <returns>Data affinity object.</returns>
         * <exception cref="ArgumentException">If client data with given name was not configured.</exception>
         */
        public IGridClientDataAffinity Affinity(String cacheName) {
            GridClientDataConfiguration dataCfg = cfg.DataConfiguration(cacheName);

            return dataCfg == null ? null : dataCfg.Affinity;
        }

        /**
         * <summary>
         * Thread that checks opened client connections for idle
         * and closes connections that idle for a long time.</summary>
         */
        private void checkIdle() {
            while (!closed && U.Sleep(IDLE_CHECK_DELAY)) {
                try {
                    connMgr.closeIdle(cfg.ConnectionIdleTimeout);
                }
                catch (ThreadInterruptedException) {
                    break;
                }
            }
        }

        /**
         * <summary>
         * Thread that updates topology according to refresh interval specified
         * in configuration.</summary>
         */
        private void updateTopology() {
            IGridClientCompute topPrj = new GridClientComputeImpl(this, null, null, topUpdateBalancer);

            while (!closed && U.Sleep(cfg.TopologyRefreshFrequency)) {
                try {
                    topPrj.RefreshTopology(false, false);
                }
                catch (GridClientException e) {
                    Dbg.WriteLine("Failed to update topology: " + e.Message);
                }
                catch (ThreadInterruptedException) {
                    break;
                }
            }
        }

        /**
         * <summary>
         * Parse servers configuration into collection of endpoints.</summary>
         * 
         * <param name="srvStrs">Collection of server endpoint definitions.</param>
         * <returns>Connection endpoints.</returns>
         */
        private static ICollection<IPEndPoint> ParseServers(ICollection<String> srvStrs) {
            ICollection<IPEndPoint> srvs = new List<IPEndPoint>(srvStrs.Count);

            foreach (String srvStr in srvStrs) {
                String[] split = srvStr.Split(":".ToCharArray());

                if (split.Length != 2)
                    throw new GridClientException("Failed to create client (invalid endpoint format, expected 'IPv4:Port'): " + srvStr);

                IPAddress addr;

                if (!IPAddress.TryParse(split[0], out addr))
                    throw new GridClientException("Failed to create client (invalid address format): " + srvStr);

                int port;

                if (!int.TryParse(split[1], out port))
                    throw new GridClientException("Failed to create client (invalid port format): " + srvStr);

                srvs.Add(new IPEndPoint(addr, port));
            }

            return srvs;
        }

        /** <inheritdoc /> */
        override public String ToString() {
            return new System.Text.StringBuilder()
                .Append(typeof(GridClientImpl).FullName)
                .Append(" [Id=").Append(Id)
                .Append(", closed=").Append(closed)
                .Append(']').ToString();
        }
    }
}
