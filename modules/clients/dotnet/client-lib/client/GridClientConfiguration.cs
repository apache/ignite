/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;
    using System.Collections.Generic;
    using GridGain.Client.Balancer;
    using GridGain.Client.Portable;
    using GridGain.Client.Ssl;
    using GridGain.Client.Util;

    /** <summary>Client configuration.</summary> */
    public class GridClientConfiguration {
        /** <summary>Default client protocol.</summary> */
        public const GridClientProtocol DefaultClientProtocol = GridClientProtocol.Tcp;

        /** <summary>Default TCP server port.</summary> */
        public static readonly int DefaultTcpPort = 11211;

        /** <summary>Default topology refresh frequency is 2 sec.</summary> */
        public static readonly TimeSpan DefaultTopologyRefreshFrequency = TimeSpan.FromSeconds(2);

        /** <summary>Default maximum time connection can be idle.</summary> */
        public static readonly TimeSpan DefaultConnectionIdleTimeout = TimeSpan.FromSeconds(30);

        /** <summary>Creates default configuration.</summary> */
        public GridClientConfiguration() {
            /* Setup defaults. */
            Balancer = new GridClientRandomBalancer();
            ConnectionIdleTimeout = DefaultConnectionIdleTimeout;
            ConnectTimeout = 0;
            Credentials = null;
            DataConfigurations = new List<GridClientDataConfiguration>();
            IsTopologyCacheEnabled = false;
            Protocol = DefaultClientProtocol;
            Servers = new HashSet<String>();
            Routers = new HashSet<String>();
            SslContext = null;
            TopologyRefreshFrequency = DefaultTopologyRefreshFrequency;
        }

        /**
         * <summary>
         * Copy constructor.</summary>
         *
         * <param name="cfg">Configuration to be copied.</param>
         */
        public GridClientConfiguration(GridClientConfiguration cfg) {
            // Preserve alphabetical order for maintenance;
            Balancer = cfg.Balancer;
            ConnectionIdleTimeout = cfg.ConnectionIdleTimeout;
            ConnectTimeout = cfg.ConnectTimeout;
            Credentials = cfg.Credentials;
            DataConfigurations = new List<GridClientDataConfiguration>(cfg.DataConfigurations);
            IsTopologyCacheEnabled = cfg.IsTopologyCacheEnabled;
            PortableClassConfigurations = cfg.PortableClassConfigurations;
            Protocol = cfg.Protocol;
            Servers = new HashSet<String>(cfg.Servers);
            Routers = new HashSet<String>(cfg.Routers);
            SslContext = cfg.SslContext;
            TopologyRefreshFrequency = cfg.TopologyRefreshFrequency;
        }

        /**
         * <summary>
         * Default balancer to be used for computational client. It can be overridden
         * for different compute instances.</summary>
         */
        public IGridClientLoadBalancer Balancer {
            get;
            set;
        }

        /** <summary>Maximum amount of time that client connection can be idle before it is closed.</summary> */
        public TimeSpan ConnectionIdleTimeout {
            get;
            set;
        }

        /** <summary>Timeout for socket connect operation.</summary> */
        public int ConnectTimeout {
            get;
            set;
        }

        /** <summary>Client credentials to authenticate with.</summary> */
        public Object Credentials {
            get;
            set;
        }

        /** <summary>Collection of data configurations (possibly empty).</summary> */
        public ICollection<GridClientDataConfiguration> DataConfigurations {
            get;
            set;
        }

        /**
         * <summary>
         * Gets data configuration for a cache with specified name.</summary>
         *
         * <param name="name">Name of grid cache.</param>
         * <returns>Configuration or <c>null</c> if there is not configuration for specified name.</returns>
         */
        public GridClientDataConfiguration DataConfiguration(String name) {
            foreach (var cfg in DataConfigurations)
                if (name == null ? cfg.Name == null : name.Equals(cfg.Name))
                    return cfg;

            return null;
        }

        /**
         * <summary>
         * Enables client to cache topology internally, so it does not have to
         * be always refreshed. Topology cache will be automatically refreshed
         * in the background every <see cref="TopologyRefreshFrequency"/> interval.</summary>
         */
        public bool IsTopologyCacheEnabled {
            get;
            set;
        }

        /** <summary>Protocol for communication between client and remote grid.</summary> */
        public GridClientProtocol Protocol {
            get;
            set;
        }

        /**
         * <summary>
         * Collection of <c>'host:port'</c> pairs representing
         * remote grid servers used to establish initial connection to
         * the grid. Once connection is established, GridGain will get
         * a full view on grid topology and will be able to connect to
         * any available remote node.</summary>
         */
        public ICollection<String> Servers {
            get;
            private set;
        }

        /**
         * <summary>
         * Collection of <c>'host:port'</c> pairs representing
         * grid routers used to establish connection to the grid.
         * <p/>
         * This configuration parameter will not be used and
         * direct grid connection will be established if
         * 'Servers' return non-<c>null</c> value.</summary>
         */
        public ICollection<String> Routers {
            get;
            private set;
        }

        /**
         * <summary>
         * SSL context, indicating whether client should try to connect server with secure
         * socket layer enabled (regardless of protocol used).
         * <para/>
         * SSL context is null to disable secure communication.</summary>
         */
        public IGridClientSslContext SslContext {
            get;
            set;
        }

        /**
         * <summary>
         * Topology refresh frequency. If topology cache is enabled, grid topology
         * will be refreshed every <c>topRefreshFreq</c> milliseconds.</summary>
         */
        public TimeSpan TopologyRefreshFrequency {
            get;
            set;
        }

        /**
         * <summary>Configuration for custom portable classes.</summary>
         */
        public ICollection<GridClientPortableClassConfiguration> PortableClassConfigurations
        {
            get;
            set;
        }
    }
}
