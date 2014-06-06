/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Message {
    using System;
    using System.Collections.Generic;
    using GridGain.Client.Util;

    /** <summary>Node bean.</summary> */
    internal class GridClientNodeBean : IGridPortableObject {
        public const int PORTABLE_TYPE_ID = -4;
        
        /** <summary>Constructs client node bean.</summary> */
        public GridClientNodeBean() {
            TcpAddresses = new HashSet<String>();
            TcpHostNames = new HashSet<String>();
            JettyAddresses = new HashSet<String>();
            JettyHostNames = new HashSet<String>();
            Attributes = new Dictionary<String, Object>();
            Caches = new GridClientNullDictionary<String, String>();
        }

        /** <summary>Node ID.</summary> */
        public Guid NodeId {
            get;
            set;
        }

        /** <summary>TCP addresses.</summary> */
        public ICollection<String> TcpAddresses {
            get;
            private set;
        }

        /** <summary>TCP host names.</summary> */
        public ICollection<String> TcpHostNames {
            get;
            private set;
        }

        /** <summary>Jetty addresses.</summary> */
        public ICollection<String> JettyAddresses {
            get;
            private set;
        }

        /** <summary>Jetty host names.</summary> */
        public ICollection<String> JettyHostNames {
            get;
            private set;
        }

        /** <summary>Node replica count for consistent hash ring.</summary> */
        public int ReplicaCount {
            get;
            set;
        }

        /** <summary>Gets metrics.</summary> */
        public GridClientNodeMetricsBean Metrics {
            get;
            private set;
        }

        /** <summary>Attributes.</summary> */
        public IDictionary<String, Object> Attributes {
            get;
            private set;
        }

        /** <summary>REST binary protocol port.</summary> */
        public int TcpPort {
            get;
            set;
        }

        /** <summary>REST http protocol port.</summary> */
        public int JettyPort {
            get;
            set;
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

        /** <summary>Mode for cache with null name.</summary> */
        public String DefaultCacheMode {
            get;
            set;
        }

        /**
         * <summary>
         * Configured node caches - the map where key is cache name
         * and value is cache mode ("LOCAL", "REPLICATED", "PARTITIONED").</summary>
         */
        public IDictionary<String, String> Caches {
            get;
            private set;
        }

        public int TypeId {
            get { return PORTABLE_TYPE_ID; }
        }

        public void WritePortable(IGridPortableWriter writer) {
            writer.WriteInt("tcpPort", TcpPort);
            writer.WriteInt("jettyPort", JettyPort);
            writer.WriteInt("replicaCnt", ReplicaCount);

            writer.WriteString("dfltCacheMode", DefaultCacheMode);

            writer.WriteMap("attrs", Attributes);
            writer.WriteMap("caches", Caches);

            writer.WriteCollection("tcpAddrs", TcpAddresses);
            writer.WriteCollection("tcpHostNames", TcpHostNames);
            writer.WriteCollection("jettyAddrs", JettyAddresses);
            writer.WriteCollection("jettyHostNames", JettyHostNames);

            writer.WriteGuid("nodeId", NodeId);

            writer.WriteObject("consistentId", ConsistentId);
            writer.WriteObject("metrics", Metrics);
        }

        public void ReadPortable(IGridPortableReader reader) {
            TcpPort = reader.ReadInt("tcpPort");
            JettyPort = reader.ReadInt("jettyPort");
            ReplicaCount = reader.ReadInt("replicaCnt");

            DefaultCacheMode = reader.ReadString("dfltCacheMode");

            Attributes = reader.ReadMap<String, Object>("attrs");
            Caches = reader.ReadMap<String, String>("caches");

            TcpAddresses = reader.ReadCollection<String>("tcpAddrs");
            TcpHostNames = reader.ReadCollection<String>("tcpHostNames");
            JettyAddresses = reader.ReadCollection<String>("jettyAddrs");
            JettyHostNames = reader.ReadCollection<String>("jettyHostNames");

            NodeId = reader.ReadGuid("nodeId");

            ConsistentId = reader.ReadObject<Object>("consistentId");
            Metrics = reader.ReadObject<GridClientNodeMetricsBean>("metrics");

            if (DefaultCacheMode != null) {
                Caches = Caches == null ? new GridClientNullDictionary<string, string>() : new GridClientNullDictionary<string, string>(Caches);

                Caches.Add(null, DefaultCacheMode);
            }
        }
    }
}
