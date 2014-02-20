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

    /** <summary>Simple balancer that implements the round-robin balancing.</summary> */
    public class GridClientRoundRobinBalancer : IGridClientLoadBalancer, IGridClientTopologyListener {
        /** <summary>Nodes to share load.</summary> */
        private readonly LinkedList<Guid> nodeQueue = new LinkedList<Guid>();

        /** <inheritdoc /> */
        public IGridClientNode BalancedNode<TNode>(ICollection<TNode> nodes) where TNode : IGridClientNode {
            IDictionary<Guid, IGridClientNode> lookup = new Dictionary<Guid, IGridClientNode>(nodes.Count);

            foreach (IGridClientNode node in nodes)
                lookup.Add(node.Id, node);

            lock (nodeQueue) {
                IGridClientNode balanced = null;


                foreach (Guid nodeId in nodeQueue) {
                    balanced = lookup[nodeId];

                    if (balanced != null) {
                        nodeQueue.Remove(nodeId);

                        break;
                    }
                }

                if (balanced != null) {
                    nodeQueue.AddLast(balanced.Id);

                    return balanced;
                }

                throw new GridClientServerUnreachableException("Failed to get balanced node (topology does not have alive " +
                    "nodes): " + nodes);
            }
        }

        /** <inheritdoc /> */
        public void OnNodeAdded(IGridClientNode node) {
            lock (nodeQueue) {
                nodeQueue.AddFirst(node.Id);
            }
        }

        /** <inheritdoc /> */
        public void OnNodeRemoved(IGridClientNode node) {
            lock (nodeQueue) {
                nodeQueue.Remove(node.Id);
            }
        }
    }
}
