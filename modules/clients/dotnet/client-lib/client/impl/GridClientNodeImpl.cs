// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl {
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Text;
    using System.Net;
    using System.Net.Sockets;
    using GridGain.Client;
    using GridGain.Client.Util;
    using GridGain.Client.Hasher;

    using A = GridGain.Client.Util.GridClientArgumentCheck;
    using Dbg = System.Diagnostics.Debug;

    /** <summary>Client node implementation.</summary> */
    internal class GridClientNodeImpl : IGridClientNode, IGridClientConsistentHashObject {
        /** <summary>Reference to a list of addresses.</summary> */
        private readonly HashSet<IPEndPoint> restAddresses = new HashSet<IPEndPoint>();

        /**
         * <summary>
         * Constructs grid client node.</summary>
         *
         * <param name="nodeId">Node ID.</param>
         */
        public GridClientNodeImpl(Guid nodeId) {
            Id = nodeId;
            TcpAddresses = new List<String>();
            TcpHostNames = new List<String>();
            JettyAddresses = new List<String>();
            JettyHostNames = new List<String>();
            Attributes = new Dictionary<String, Object>();
            Metrics = null;
            Caches = new GridClientNullDictionary<String, GridClientCacheMode>();
        }

        /** <summary>Node id.</summary> */
        public Guid Id {
            get;
            private set;
        }

        /** <summary>TCP addresses.</summary> */
        public IList<String> TcpAddresses {
            get;
            private set;
        }

        /** <summary>TCP host names.</summary> */
        public IList<String> TcpHostNames {
            get;
            private set;
        }

        /** <summary>Jetty addresses.</summary> */
        public IList<String> JettyAddresses {
            get;
            private set;
        }

        /** <summary>Jetty host names.</summary> */
        public IList<String> JettyHostNames {
            get;
            private set;
        }

        /** <summary>Tcp remote port value.</summary> */
        public int TcpPort {
            get;
            set;
        }

        /** <summary>Http(s) port value.</summary> */
        public int HttpPort {
            get;
            set;
        }

        /** <summary>Node attributes.</summary> */
        public IDictionary<String, Object> Attributes {
            get;
            private set;
        }

        /** <inheritdoc /> */
        public T Attribute<T>(String name) {
            return Attribute<T>(name, default(T));
        }

        /** <inheritdoc /> */
        public T Attribute<T>(String name, T def) {
            Object result;

            return Attributes.TryGetValue(name, out result) && (result is T) ? (T)result : def;
        }

        /** <inheritdoc /> */
        public int ReplicaCount {
            get;
            set;
        }

        /** <summary>Node metrics.</summary> */
        public IGridClientNodeMetrics Metrics {
            get;
            set;
        }

        /** Caches available on remote node.*/
        public IDictionary<String, GridClientCacheMode> Caches {
            get;
            private set;
        }

        /**
         * <summary>
         * Gets list of all addresses available for connection.</summary>
         *
         * <param name="proto">Protocol type.</param>
         * <returns>List of socket addresses.</returns>
         */
        public IList<IPEndPoint> AvailableAddresses(GridClientProtocol proto) {
            lock (restAddresses) {
                if (restAddresses.Count == 0) {
                    int port = proto == GridClientProtocol.Tcp ? TcpPort : HttpPort;

                    if (port != 0) {
                        if (proto == GridClientProtocol.Tcp)
                            foreach (IPAddress addr in ToHostAddresses(TcpAddresses, TcpHostNames))
                                restAddresses.Add(new IPEndPoint(addr, port));
                        else
                            foreach (IPAddress addr in ToHostAddresses(JettyAddresses, JettyHostNames))
                                restAddresses.Add(new IPEndPoint(addr, port));
                    }
                }
            }

            return new List<IPEndPoint>(restAddresses);
        }

        /**
         * <summary>Returns IP addresses, if host names can not be resolved then ip addresses is used for resolution.</summary>
         */
        private static IList<IPAddress> ToHostAddresses(IList<String> addrs, IList<String> hostNames) {
            IList<IPAddress> res = new List<IPAddress>(addrs.Count);

            IEnumerator<String> hostNamesIt = hostNames.GetEnumerator();

            foreach (String addr in addrs) {
                String hostName = hostNamesIt.MoveNext() ? hostNamesIt.Current : null;

                IPAddress inetAddr = null;
           
                if (hostName != null && !hostName.Equals("")) {
                    try {
                        inetAddr = Dns.GetHostAddresses(hostName)[0];
                    }
                    catch (SocketException) {
                    }
                }

                if (inetAddr == null) {
                    try {
                        inetAddr = Dns.GetHostAddresses(addr)[0];
                    }
                    catch (SocketException) {
                    }
                }

                if (inetAddr != null)
                    res.Add(inetAddr);
            }

            if (res.Count == 0)
                throw new GridClientException("Addresses can not be resolved [addr=" + addrs +
                ", hostNames=" + hostNames + ']');

            return res;
        }

        /**
         * <summary>
         * Consistent globally unique node ID. Unlike the Id property,
         * this property contains a consistent node ID which survives node restarts.</summary>
         */
        public Object ConsistentId {
            get;
            set;
        }

        /** <inheritdoc /> */
        override public bool Equals(Object obj) {
            if (this == obj)
                return true;

            GridClientNodeImpl that = obj as GridClientNodeImpl;

            return that != null && Id.Equals(that.Id);
        }

        /** <inheritdoc /> */
        override public int GetHashCode() {
            return Id.GetHashCode();
        }

        /** <inheritdoc /> */
        override public String ToString() {
            StringBuilder sb = new StringBuilder("GridClientNodeImpl");

            sb.AppendFormat(" [NodeId={0}", Id);
            sb.AppendFormat(", TcpAddresses={0}", String.Join<String>(",", TcpAddresses));
            sb.AppendFormat(", TcpHostNames={0}", String.Join<String>(",", TcpHostNames));
            sb.AppendFormat(", JettyAddresses={0}", String.Join<String>(",", JettyAddresses));
            sb.AppendFormat(", JettyHostNames={0}", String.Join<String>(",", JettyHostNames));
            sb.AppendFormat(", TcpPort={0}", TcpPort);
            sb.AppendFormat(", HttpPort={0}", HttpPort);
            sb.Append(']');

            return sb.ToString();
        }
    }
}
