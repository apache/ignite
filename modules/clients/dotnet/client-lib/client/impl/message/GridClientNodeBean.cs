/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Message {
    using System;
    using System.Text;
    using System.Collections.Generic;
    using GridGain.Client.Util;

    /** <summary>Node bean.</summary> */
    internal class GridClientNodeBean {
        /** <summary>Constructs client node bean.</summary> */
        public GridClientNodeBean() {
            TcpAddresses = new HashSet<String>();
            TcpHostNames = new HashSet<String>();
            JettyAddresses = new HashSet<String>();
            JettyHostNames = new HashSet<String>();
            Metrics = new Dictionary<String, Object>();
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
        public IDictionary<String, Object> Metrics {
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

        /**
         * <summary>
         * Configured node caches - the map where key is cache name
         * and value is cache mode ("LOCAL", "REPLICATED", "PARTITIONED").</summary>
         */
        public IDictionary<String, String> Caches {
            get;
            private set;
        }
    }
}
