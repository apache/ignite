/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl {
    using System;
    using System.Threading;
    using System.Collections.Generic;
    using scc = System.Collections.Concurrent;
    using GridGain.Client;
    using GridGain.Client.Util;

    using A = GridGain.Client.Util.GridClientArgumentCheck;
    using N = GridGain.Client.IGridClientNode;
    using Dbg = System.Diagnostics.Debug;

    /**
     * <summary>
     * Client topology cache.</summary>
     */
    internal sealed class GridClientTopology : IDisposable {
        /** <summary>Topology cache</summary> */
        private IDictionary<Guid, N> _nodes = new Dictionary<Guid, N>();

        /** <summary>Flag indicating whether node attributes and metrics should be cached.</summary> */
        private bool topCache;

        /** <summary>Lock for topology changing.</summary> */
        private ReaderWriterLock busyLock = new ReaderWriterLock();

        /** <summary>Topology listeners.</summary> */
        private IList<IGridClientTopologyListener> topLsnrs = new List<IGridClientTopologyListener>();

        /** <summary>The queue of topology events awaiting for delivering.</summary> */
        private scc::BlockingCollection<TopologyEvent> evtQ = new scc::BlockingCollection<TopologyEvent>();

        /** <summary>Thread for topology events delivering.</summary> */
        private readonly Thread evtSndr;

        /** <summary>Closed state flag.</summary> */
        private volatile bool closed;

        /**
         * <summary>
         * Creates topology instance.</summary>
         *
         * <param name="clientId">Client id.</param>
         * <param name="topCache">If <c>true</c>, then topology will cache node attributes and metrics.</param>
         */
        public GridClientTopology(Guid clientId, bool topCache) {
            this.topCache = topCache;

            evtSndr = new Thread(this.SendEvents);

            evtSndr.Name = "grid-topology-send-event-worker--client#" + clientId;

            //Dbg.WriteLine("Start thread: " + evtSndr.Name);

            evtSndr.Start();
        }

        /**
         * <summary>
         * Adds topology listener.</summary>
         *
         * <param name="lsnr">Topology listener.</param>
         */
        public void AddTopologyListener(IGridClientTopologyListener lsnr) {
            A.NotNull(lsnr, "lsnr");

            busyLock.AcquireWriterLock(Timeout.Infinite);

            try {
                topLsnrs.Add(lsnr);
            }
            finally {
                busyLock.ReleaseWriterLock();
            }
        }

        /**
         * <summary>
         * Removes topology listener.</summary>
         *
         * <param name="lsnr">Topology listener.</param>
         */
        public void RemoveTopologyListener(IGridClientTopologyListener lsnr) {
            busyLock.AcquireWriterLock(Timeout.Infinite);

            try {
                topLsnrs.Remove(lsnr);
            }
            finally {
                busyLock.ReleaseWriterLock();
            }
        }

        /**
         * <summary>
         * Returns all added topology listeners.</summary>
         *
         * <returns>Unmodifiable view of topology listeners.</returns>
         */
        public IList<IGridClientTopologyListener> TopologyListeners() {
            busyLock.AcquireReaderLock(Timeout.Infinite);

            try {
                return new List<IGridClientTopologyListener>(topLsnrs);
            }
            finally {
                busyLock.ReleaseReaderLock();
            }
        }

        /**
         * <summary>
         * Updates topology if cache enabled. If cache is disabled, returns original node.</summary>
         *
         * <param name="node">Converted rest server response.</param>
         * <returns>Node in topology.</returns>
         */
        public N UpdateNode(N node) {
            A.NotNull(node, "node");

            busyLock.AcquireWriterLock(Timeout.Infinite);

            try {
                bool nodeAdded = !_nodes.ContainsKey(node.Id);

                // We update the whole topology if node was not in topology or we cache metrics.
                if (nodeAdded || topCache) {
                    node = ClearAttributes(node);

                    _nodes[node.Id] = node;
                }

                if (nodeAdded)
                    FireEvents(new TopologyEvent[] { new TopologyEvent(true, node) });

                return node;
            }
            finally {
                busyLock.ReleaseWriterLock();
            }
        }

        /**
         * <summary>
         * Updates (if cache is enabled) the whole topology. If cache is disabled,
         * original collection is returned.</summary>
         *
         * <param name="nodeList">Converted rest server response.</param>
         * <returns>Topology nodes.</returns>
         */
        public void UpdateTopology(IEnumerable<N> nodeList) {
            ICollection<TopologyEvent> evts = new LinkedList<TopologyEvent>();

            busyLock.AcquireWriterLock(Timeout.Infinite);

            try {
                IDictionary<Guid, N> updated = new Dictionary<Guid, N>();

                foreach (N node in nodeList) {
                    updated.Add(node.Id, ClearAttributes(node));

                    // Generate add events.
                    if (!_nodes.ContainsKey(node.Id))
                        evts.Add(new TopologyEvent(true, node));
                }

                foreach (KeyValuePair<Guid, N> e in _nodes) {
                    // Generate leave events.
                    if (!updated.ContainsKey(e.Key))
                        evts.Add(new TopologyEvent(false, e.Value));
                }

                _nodes = updated;

                FireEvents(evts);
            }
            finally {
                busyLock.ReleaseWriterLock();
            }
        }

        /**
         * <summary>
         * Updates topology when node that is expected to be in topology fails.</summary>
         *
         * <param name="nodeId">Node id for which node failed to be obtained.</param>
         */
        public void NodeFailed(Guid nodeId) {
            busyLock.AcquireWriterLock(Timeout.Infinite);

            try {
                N deleted;

                _nodes.TryGetValue(nodeId, out deleted);

                if (_nodes.Remove(nodeId) && deleted != null)
                    FireEvents(new TopologyEvent[] { new TopologyEvent(false, deleted) });
            }
            finally {
                busyLock.ReleaseWriterLock();
            }
        }

        /**
         * <summary>
         * Gets node from last saved topology snapshot by it's id.</summary>
         *
         * <param name="id">Node id.</param>
         * <returns>Node or <c>null</c> if node was not found.</returns>
         */
        public N Node(Guid id) {
            busyLock.AcquireReaderLock(Timeout.Infinite);

            try {
                N node;

                _nodes.TryGetValue(id, out node);

                return node;
            }
            finally {
                busyLock.ReleaseReaderLock();
            }
        }

        /**
         * <summary>
         * Gets a collection of nodes from last saved topology snapshot by their ids.</summary>
         *
         * <param name="ids">Collection of ids for which nodes should be retrieved..</param>
         * <returns>Collection of nodes that are in topology.</returns>
         */
        public IList<N> Nodes(IEnumerable<Guid> ids) {
            IList<N> res = new List<N>();

            busyLock.AcquireReaderLock(Timeout.Infinite);

            try {
                foreach (Guid id in ids) {
                    N node;

                    if (_nodes.TryGetValue(id, out node))
                        res.Add(node);
                }

                return res;
            }
            finally {
                busyLock.ReleaseReaderLock();
            }
        }

        /**
         * <summary>
         * Gets full topology snapshot.</summary>
         *
         * <returns>Collection of nodes that were in last captured topology snapshot.</returns>
         */
        public IList<N> Nodes() {
            busyLock.AcquireReaderLock(Timeout.Infinite);

            try {
                return new List<N>(_nodes.Values);
            }
            finally {
                busyLock.ReleaseReaderLock();
            }
        }

        /**
         * <summary>
         * Shutdowns executor service that performs listener notification.</summary>
         */
        public void Dispose() {
            lock (this) {
                if (closed)
                    return;

                // Stop notifications delivering thread.
                closed = true;
            }

            // Clean up listeners.
            busyLock.AcquireWriterLock(Timeout.Infinite);

            try {
                topLsnrs.Clear();
            }
            finally {
                busyLock.ReleaseWriterLock();
            }

            //Dbg.WriteLine("Join thread: " + evtSndr.Name);

            // Stop events sender.
            evtSndr.Interrupt();
            evtSndr.Join();

            //Dbg.WriteLine("Thread stopped: " + evtSndr.Name);

            evtQ.Dispose();

            GC.SuppressFinalize(this);
        }

        /**
         * <summary>
         * Clears attributes and metrics map in case if node cache is disabled.</summary>
         *
         * <param name="node">Node to be cleared.</param>
         * <returns>The same node if cache is enabled or node contains no attributes and metrics,</returns>
         *      otherwise will return new node without attributes and metrics.
         */
        private N ClearAttributes(N node) {
            if (topCache || (node.Attributes.Count == 0 && node.Metrics == null))
                return node;

            // Fill all fields but attributes and metrics since we do not cache them.
            GridClientNodeImpl updated = new GridClientNodeImpl(node.Id);

            updated.TcpAddresses.AddAll<String>(node.TcpAddresses);
            updated.TcpHostNames.AddAll<String>(node.TcpHostNames);
            updated.JettyAddresses.AddAll<String>(node.JettyAddresses);
            updated.JettyHostNames.AddAll<String>(node.JettyHostNames);
            updated.TcpPort = node.TcpPort;
            updated.HttpPort = node.HttpPort;
            updated.ConsistentId= node.ConsistentId;
            updated.Metrics = null;
            updated.Caches.AddAll<KeyValuePair<String, GridClientCacheMode>>(node.Caches);

            return updated;
        }

        /**
         * <summary>
         * Runs listener notification is separate thread.</summary>
         *
         * <param name="evts">Event list.</param>
         */
        private void FireEvents(IEnumerable<TopologyEvent> evts) {
            foreach (TopologyEvent e in evts)
                evtQ.Add(e);
        }

        /**
         * <summary>
         * Deliver notification events from the queue to the topology listeners.</summary>
         */
        private void SendEvents() {
            while (!closed) {
                TopologyEvent evt;

                try {
                    evt = evtQ.Take();
                }
                catch (ThreadInterruptedException) {
                    break;
                }

                foreach (IGridClientTopologyListener lsnr in topLsnrs)
                    try {
                        if (evt.Added)
                            lsnr.OnNodeAdded(evt.Node);
                        else
                            lsnr.OnNodeRemoved(evt.Node);
                    }
                    catch (Exception e) {
                        Dbg.WriteLine("Ignore uncaught listener exception [lsnr={0}, node={1}, e={2}].", lsnr, evt.Node, e);
                    }
            }
        }

        /** <summary>Event for node adding and removal.</summary> */
        private class TopologyEvent {
            /**
             * <summary>
             * Creates a new event.</summary>
             *
             * <param name="added">If <c>true</c>, indicates that node was added to topology.</param>
             *      If <c>false</c>, indicates that node was removed.
             * <param name="node">Added or removed node.</param>
             */
            public TopologyEvent(bool added, N node) {
                this.Added = added;
                this.Node = node;
            }

            /** <summary>Added or removed flag</summary> */
            public readonly bool Added;

            /** <summary>Node that triggered event.</summary> */
            public readonly N Node;
        }
    }
}
