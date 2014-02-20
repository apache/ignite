// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl {
    using System;

    /** <summary>Interface provides configuration for the client projection(s).</summary> */
    internal interface IGridClientProjectionConfig {
        /** <summary>Connections manager.</summary> */
        GridClientConnectionManager ConnectionManager {
            get;
        }

        /** <summary>Topology instance to be used in this projection.</summary> */
        GridClientTopology Topology {
            get;
        }

        /**
         * <summary>
         * Gets data affinity for a given cache name.</summary>
         *
         * <param name="cacheName">Name of cache for which affinity is obtained. Data configuration with this name</param>
         *     must be configured at client startup.
         * <returns>Data affinity object.</returns>
         * <exception cref="ArgumentException">If client data with given name was not configured.</exception>
         */
        IGridClientDataAffinity Affinity(String cacheName);
    }
}
