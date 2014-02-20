// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Balancer {
    using System;
    using System.Threading;
    using System.Collections.ObjectModel;
    using System.Collections.Generic;

    /**
     * <summary>
     * Simple balancer that relies on random node selection from a given collection.</summary>
     *
     * <remarks>
     * This implementation has no caches and treats each given collection as a new one.
     * More strictly, for any non-empty collection of size <c>n</c> the probability of selection of any
     * node in this collection will be <c>1/n</c>.</remarks>
     */
    public class GridClientRandomBalancer : IGridClientLoadBalancer {
        /** <summary>Random for node selection.</summary> */
        private Random rnd = new Random();

        /**
         * <summary>
         * Picks up a random node from a collection.</summary>
         *
         * <param name="nodes">Nodes to pick from.</param>
         * <returns>Random node from collection.</returns>
         */
        public IGridClientNode BalancedNode<TNode>(ICollection<TNode> nodes) where TNode : IGridClientNode {
            int size = nodes.Count;

            if (size == 0)
                throw new ArgumentException("Failed to pick up balanced node (collection of nodes is empty)");

            int idx = rnd.Next(size);

            var list = nodes as IList<TNode>;

            if (list != null)
                return list[idx];

            foreach (TNode node in nodes)
                if (idx-- <= 0)
                    return node;

            throw new InvalidOperationException("Unreachable code.");
        }
    }
}
