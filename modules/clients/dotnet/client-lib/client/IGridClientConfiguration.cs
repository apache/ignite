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

    /** <summary>Client configuration.</summary> */
    public interface IGridClientConfiguration {
        /**
         * <summary>
         * Default balancer to be used for computational client. It can be overridden
         * for different compute instances.</summary>
         */
        IGridClientLoadBalancer Balancer {
            get;
        }

        /** <summary>Maximum amount of time that client connection can be idle before it is closed.</summary> */
        TimeSpan ConnectionIdleTimeout {
            get;
        }

        /** <summary>Timeout for socket connect operation.</summary> */
        int ConnectTimeout {
            get;
        }

        /** <summary>Client credentials to authenticate with.</summary> */
        Object Credentials {
            get;
        }

        /** <summary>Collection of data configurations (possibly empty).</summary> */
        ICollection<IGridClientDataConfiguration> DataConfigurations {
            get;
        }

        /**
         * <summary>
         * Enables client to cache topology internally, so it does not have to
         * be always refreshed. Topology cache will be automatically refreshed
         * in the background every <see cref="TopologyRefreshFrequency"/> interval.</summary>
         */
        bool IsTopologyCacheEnabled {
            get;
        }

        /** <summary>Protocol for communication between client and remote grid.</summary> */
        GridClientProtocol Protocol {
            get;
        }

        /**
         * <summary>
         * Collection of <c>'host:port'</c> pairs representing
         * remote grid servers used to establish initial connection to
         * the grid. Once connection is established, GridGain will get
         * a full view on grid topology and will be able to connect to
         * any available remote node.</summary>
         */
        ICollection<String> Servers {
            get;
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
        ICollection<String> Routers {
            get;
        }

        /**
         * <summary>
         * SSL context, indicating whether client should try to connect server with secure
         * socket layer enabled (regardless of protocol used).
         * <para/>
         * SSL context is null to disable secure communication.</summary>
         */
        IGridClientSslContext SslContext {
            get;
        }

        /**
         * <summary>
         * Topology refresh frequency. If topology cache is enabled, grid topology
         * will be refreshed every <c>topRefreshFreq</c> milliseconds.</summary>
         */
        TimeSpan TopologyRefreshFrequency {
            get;
        }
    }
}
