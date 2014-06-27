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
    using GridGain.Client.Portable;
    using GridGain.Client.Util;

    using PU = GridGain.Client.Impl.Portable.GridClientPortableUilts;

    /** <summary>Node bean.</summary> */
    [GridClientPortableId(PU.TYPE_NODE_BEAN)]
    internal class GridClientNodeBean : IGridClientPortable {
        /** Portable type ID. */
        // TODO: GG-8535: Remove in favor of normal IDs.
        public static readonly int PORTABLE_TYPE_ID = 0;

        /** <summary>Constructs client node bean.</summary> */
        public GridClientNodeBean() {
            TcpAddresses = new HashSet<String>();
            TcpHostNames = new HashSet<String>();
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
        
        /** <inheritdoc /> */
        public void WritePortable(IGridClientPortableWriter writer) {
            IGridClientPortableRawWriter rawWriter = writer.RawWriter();

            rawWriter.WriteInt(TcpPort);
            rawWriter.WriteInt(ReplicaCount);
            rawWriter.WriteString(DefaultCacheMode);
            rawWriter.WriteGenericDictionary(Attributes);
            rawWriter.WriteGenericDictionary(Caches);
            rawWriter.WriteGenericCollection(TcpAddresses);
            rawWriter.WriteGenericCollection(TcpHostNames);
            rawWriter.WriteGuid(NodeId != Guid.Empty ? NodeId : (Guid?)null);
            rawWriter.WriteObject(ConsistentId);
            rawWriter.WriteObject(Metrics);
        }

        /** <inheritdoc /> */
        public void ReadPortable(IGridClientPortableReader reader) {
            IGridClientPortableRawReader rawReader = reader.RawReader();

            TcpPort = rawReader.ReadInt();
            ReplicaCount = rawReader.ReadInt();

            DefaultCacheMode = rawReader.ReadString();

            Attributes = rawReader.ReadGenericDictionary<string, object>();
            Caches = rawReader.ReadGenericDictionary<string, string>();

            TcpAddresses = rawReader.ReadGenericCollection<string>();
            TcpHostNames = rawReader.ReadGenericCollection<string>();

            Guid? nodeId0 = rawReader.ReadGuid();

            NodeId = nodeId0.HasValue ? nodeId0.Value : Guid.Empty;

            ConsistentId = rawReader.ReadObject<Object>();
            Metrics = rawReader.ReadObject<GridClientNodeMetricsBean>();

            if (DefaultCacheMode != null) {
                Caches = Caches == null ? new GridClientNullDictionary<string, string>() : 
                    new GridClientNullDictionary<string, string>(Caches);

                Caches.Add(null, DefaultCacheMode);
            }
        }
    }
}
