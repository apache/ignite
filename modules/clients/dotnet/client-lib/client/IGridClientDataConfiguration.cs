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

    /** <summary>Client data projection configuration.</summary> */
    public interface IGridClientDataConfiguration {
        /** <summary>Remote cache name.</summary> */
        String Name {
            get;
        }

        /** <summary>Cache affinity to use.</summary> */
        IGridClientDataAffinity Affinity {
            get;
        }

        /** <summary>Node balancer for pinned mode.</summary> */
        IGridClientLoadBalancer PinnedBalancer {
            get;
        }
    }
}
