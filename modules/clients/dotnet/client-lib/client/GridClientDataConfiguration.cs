// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;
    using GridGain.Client.Balancer;

    /** <summary>Data configuration bean.</summary> */
    public class GridClientDataConfiguration : IGridClientDataConfiguration {
        /** <summary>Creates empty configuration.</summary> */
        public GridClientDataConfiguration() {
            PinnedBalancer = new GridClientRandomBalancer();
        }

        /**
         * <summary>
         * Copy constructor.</summary>
         *
         * <param name="cfg">Configuration to copy.</param>
         */
        public GridClientDataConfiguration(IGridClientDataConfiguration cfg) {
            // Preserve alphabetic order for maintenance.
            Affinity = cfg.Affinity;
            Name = cfg.Name;
            PinnedBalancer = cfg.PinnedBalancer;
        }

        /** <summary>Client data affinity for this configuration.</summary> */
        public IGridClientDataAffinity Affinity {
            get;
            set;
        }

        /** <summary>Grid cache name for this configuration.</summary> */
        public String Name {
            get;
            set;
        }

        /** <summary>Balancer that will be used in pinned mode.</summary> */
        public IGridClientLoadBalancer PinnedBalancer {
            get;
            set;
        }
    }
}
