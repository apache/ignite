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
    using GridGain.Client.Ssl;
    using GridGain.Client.Util;

    /** <summary>Client configuration adapter.</summary> */
    public class GridClientConfiguration : IGridClientConfiguration {
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
            DataConfigurations = new List<IGridClientDataConfiguration>();
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
        public GridClientConfiguration(IGridClientConfiguration cfg) {
            // Preserve alphabetical order for maintenance;
            Balancer = cfg.Balancer;
            ConnectionIdleTimeout = cfg.ConnectionIdleTimeout;
            ConnectTimeout = cfg.ConnectTimeout;
            Credentials = cfg.Credentials;
            DataConfigurations = new List<IGridClientDataConfiguration>(cfg.DataConfigurations);
            IsTopologyCacheEnabled = cfg.IsTopologyCacheEnabled;
            Protocol = cfg.Protocol;
            Servers = new HashSet<String>(cfg.Servers);
            Routers = new HashSet<String>(cfg.Routers);
            SslContext = cfg.SslContext;
            TopologyRefreshFrequency = cfg.TopologyRefreshFrequency;
        }

        /** <inheritdoc /> */
        public IGridClientLoadBalancer Balancer {
            get;
            set;
        }

        /** <inheritdoc /> */
        public TimeSpan ConnectionIdleTimeout {
            get;
            set;
        }

        /** <inheritdoc /> */
        public int ConnectTimeout {
            get;
            set;
        }

        /** <inheritdoc /> */
        public Object Credentials {
            get;
            set;
        }

        /** <inheritdoc /> */
        public ICollection<IGridClientDataConfiguration> DataConfigurations {
            get;
            private set;
        }

        /**
         * <summary>
         * Gets data configuration for a cache with specified name.</summary>
         *
         * <param name="name">Name of grid cache.</param>
         * <returns>Configuration or <c>null</c> if there is not configuration for specified name.</returns>
         */
        public IGridClientDataConfiguration DataConfiguration(String name) {
            foreach (var cfg in DataConfigurations)
                if (name == null ? cfg.Name == null : name.Equals(cfg.Name))
                    return cfg;

            return null;
        }

        /** <inheritdoc /> */
        public bool IsTopologyCacheEnabled {
            get;
            set;
        }

        /** <inheritdoc /> */
        public GridClientProtocol Protocol {
            get;
            set;
        }

        /** <inheritdoc /> */
        public ICollection<String> Servers {
            get;
            private set;
        }

        /** <inheritdoc /> */
        public ICollection<String> Routers {
            get;
            private set;
        }

        /** <inheritdoc /> */
        public IGridClientSslContext SslContext {
            get;
            set;
        }

        /** <inheritdoc /> */
        public TimeSpan TopologyRefreshFrequency {
            get;
            set;
        }
    }
}
