// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Balancer {
    using System.Collections.Generic;

    /**
     * <summary>
     * Interface that defines a selection logic of a server node for a particular operation
     * (e.g. task run or cache operation in case of pinned mode).</summary>
     */
    public interface IGridClientLoadBalancer {
        /**
         * <summary>
         * Gets next node for executing client command.</summary>
         * 
         * <param name="nodes">Nodes to pick from.</param>
         * <returns>Next node to pick.</returns>
         * <exception cref="GridGain.Client.GridClientServerUnreachableException">
         *     If none of the nodes given to the balancer can be reached.</exception>
         */
        IGridClientNode BalancedNode<TNode>(ICollection<TNode> nodes) where TNode : IGridClientNode;
    }
}
