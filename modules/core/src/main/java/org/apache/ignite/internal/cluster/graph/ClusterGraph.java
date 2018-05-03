package org.apache.ignite.internal.cluster.graph;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CommunicationFailureContext;

public class ClusterGraph {
    /** */
    private final IgniteLogger log;

    /** */
    private final CommunicationFailureContext ctx;

    /** */
    private final Predicate<ClusterNode> nodeFilterOut;

    /** */
    private final int nodeCnt;

    /** */
    private final List<ClusterNode> nodes;

    /** */
    private final BitSet[] connections;

    /** */
    private final FullyConnectedComponentSearcher fccSearcher;

    /**
     * @param log Logger.
     * @param ctx Context.
     */
    public ClusterGraph(IgniteLogger log, CommunicationFailureContext ctx, Predicate<ClusterNode> nodeFilterOut) {
        this.log = log;
        this.ctx = ctx;
        this.nodeFilterOut = nodeFilterOut;

        nodes = ctx.topologySnapshot();

        nodeCnt = nodes.size();

        assert nodeCnt > 0;

        connections = buildConnectivityMatrix(ctx, nodeFilterOut);

        fccSearcher = new FullyConnectedComponentSearcher(connections);
    }

    private BitSet[] buildConnectivityMatrix(CommunicationFailureContext ctx, Predicate<ClusterNode> nodeFilterOut) {
        BitSet[] connections = new BitSet[nodeCnt];

        for (int i = 0; i < nodeCnt; i++) {
            ClusterNode node = nodes.get(i);

            if (nodeFilterOut.test(node)) {
                connections[i] = null;
                continue;
            }

            connections[i] = new BitSet(nodeCnt);
            for (int j = 0; j < nodeCnt; j++) {
                ClusterNode to = nodes.get(j);

                if (i == j || ctx.connectionAvailable(node, to))
                    connections[i].set(j);
            }
        }

        // Remove unidirectional connections (node A can connect to B, but B can't connect to A).
        for (int i = 0; i < nodeCnt; i++)
            for (int j = i + 1; j < nodeCnt; j++) {
                if (connections[i] == null || connections[j] == null)
                    continue;

                if (connections[i].get(j) ^ connections[j].get(i)) {
                    connections[i].set(j, false);
                    connections[j].set(i, false);
                }
            }

        return connections;
    }

    /**
     *
     * @return
     */
    public List<BitSet> findConnectedComponents() {
        List<BitSet> connectedComponets = new ArrayList<>();

        BitSet visitSet = new BitSet(nodeCnt);

        for (int i = 0; i < nodeCnt; i++) {
            if (visitSet.get(i) || connections[i] == null)
                continue;

            BitSet graphComponent = new BitSet(nodeCnt);

            dfs(i, graphComponent, visitSet);

            connectedComponets.add(graphComponent);
        }

        return connectedComponets;
    }

    private void dfs(int nodeIdx, BitSet currentComponent, BitSet allVisitSet) {
        assert !allVisitSet.get(nodeIdx)
            : "Incorrect node visit " + nodeIdx;

        assert connections[nodeIdx] != null
            : "Incorrect node visit. Node has not passed filter " + nodes.get(nodeIdx);

        allVisitSet.set(nodeIdx);

        currentComponent.set(nodeIdx);

        for (int toIdx = 0; toIdx < nodeCnt; toIdx++) {
            if (toIdx == nodeIdx || allVisitSet.get(toIdx) || connections[toIdx] == null)
                continue;

            boolean connected = connections[nodeIdx].get(toIdx) && connections[toIdx].get(nodeIdx);

            if (connected)
                dfs(toIdx, currentComponent, allVisitSet);
        }
    }

    public BitSet findLargestFullyConnectedComponent(BitSet nodesSet) {
        // Check that current set is already fully connected.
        boolean fullyConnected = checkFullyConnected(nodesSet);

        if (fullyConnected)
            return nodesSet;

        return fccSearcher.findLargest(nodesSet);
    }

    /**
     * @param nodesSet Cluster nodes bit set.
     * @return {@code True} if all cluster nodes are able to connect to each other.
     */
    public boolean checkFullyConnected(BitSet nodesSet) {
        int maxIdx = nodesSet.length();

        Iterator<Integer> it = new BitSetIterator(nodesSet);

        while (it.hasNext()) {
            int idx = it.next();

            for (int i = 0; i < maxIdx; i++) {
                if (i == idx)
                    continue;

                if (nodesSet.get(i) && !connections[idx].get(i))
                    return false;
            }
        }

        return true;
    }
}
