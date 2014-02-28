// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include <set>
#include <iostream>

#include "gridgain/gridclientexception.hpp"
#include "gridgain/impl/gridclientprojection.hpp"
#include "gridgain/impl/projectionclosure/gridclientprojectionclosure.hpp"
#include "gridgain/loadbalancer/gridclientloadbalancer.hpp"
#include "gridgain/loadbalancer/gridclientrouterbalancer.hpp"

#include "gridgain/impl/utils/gridclientlog.hpp"

using namespace std;

const static size_t RETRY_CNT = 1;

GridClientProjectionImpl::GridClientProjectionImpl(TGridClientSharedDataPtr pData, GridClientProjectionListener& prjLsnr,
        TGridClientNodePredicatePtr pFilter) : sharedData(pData), prjLsnr(prjLsnr), filter(pFilter),
        dfltAffinity(new GridClientPartitionAffinity()) {

    vector<GridClientDataConfiguration> dataCfg = pData->clientConfiguration().dataConfiguration();

    // Read affinity configuration from vector to affinity map.
    std::transform(dataCfg.begin(), dataCfg.end(), std::inserter(affinityMap, affinityMap.end()),
        [] (GridClientDataConfiguration& c) { return std::make_pair(c.name(), c.affinity()); });
}

/**
 * This method executes request to a communication layer and handles connection error, if it occurs.
 * In case of communication exception client instance is notified and new instance of client is created.
 * If none of the grid servers can be reached, an exception is thrown.
 *
 * @param c Closure to be executed.
 * @return Future returned by closure.
 * @throws GridServerUnreachableException If either none of the server can be reached or each attempt
 *      of communication within {@link #RETRY_CNT} attempts resulted in {@link GridClientConnectionResetException}.
 */
void GridClientProjectionImpl::withReconnectHandling(ClientProjectionClosure& c) const {
    std::set<TGridClientNodePtr> nodesSet;
    set<GridUuid> seenUuids;

    GridOneOfUuid filter(seenUuids);

    // Loop until we succeed or we run out grid nodes to try.
    while (true) {
        TNodesSet nodes;

        try {
            subProjectionNodes(nodes, filter);
        }
        catch (GridClientException&) {
            // No more nodes to try.
            GG_LOG_ERROR0("Failed to process a client request: the projection is empty.");

            throw GridClientException("Failed to process a client request: the projection is empty.");
        }

        TGridClientNodePtr node = balancedNode(nodes);

        seenUuids.insert(node->getNodeId());

        for (size_t i = 0; i < RETRY_CNT; i++) {
            if (processClosure(node, c))
                return;
        }
    }
}

/**
 * This method executes request to a communication layer and handles connection error, if it occurs. Server
 * is picked up according to the projection affinity and key given. Connection will be made with the node
 * on which key is cached. In case of communication exception client instance is notified and new instance
 * of client is created. If none of servers can be reached, an exception is thrown.
 *
 * @param c Closure to be executed.
 * @param cacheName Cache name for which mapped node will be calculated.
 * @param affKey Affinity key.
 * @return Operation future.
 * @throws GridClientException In case of problems.
 */
void GridClientProjectionImpl::withReconnectHandling(ClientProjectionClosure& c, const std::string& cacheName,
        const GridHasheableObject& affKey) {

    // First, we try the affinity node.
    TGridClientNodePtr node = affinityNode(cacheName, affKey);

    set<GridUuid> seenUuids;

    GridClientCompositeFilter<GridClientNode> filter;

    filter.add(TGridClientNodePredicatePtr(new GridCacheNameFilter(cacheName)));
    filter.add(TGridClientNodePredicatePtr(new GridOneOfUuid(seenUuids)));

    // Loop until we succeed or we run out grid nodes to try.
    while (true) {
        for (size_t i = 0; i < RETRY_CNT; i++) {
            if (processClosure(node, c))
                return;
        }

        // Could not execute the closure on the affinity node. Try to process the closure on other nodes
        // that have this cache configured.
        seenUuids.insert(node->getNodeId());

        TNodesSet nodes;

        try {
            subProjectionNodes(nodes, filter);
        }
        catch (GridClientException&) {
            // No more nodes to try.
            throw GridClientException(
                    "Failed to process a client request: the cache projection [" + cacheName + "] has no more nodes.");
        }

        node = balancedNode(nodes);

        seenUuids.insert(node->getNodeId());
    }
}

bool GridClientProjectionImpl::processClosure(TGridClientNodePtr node, ClientProjectionClosure& c) const {
    assert(node.get() != NULL);

    std::vector<GridSocketAddress> addrs = sharedData->clientConfiguration().routers();

    bool routing = !addrs.empty();

    if (!routing) {
        addrs = node->availableAddresses(protocol());
    }

    if (addrs.empty())
        throw GridClientException(string("No available endpoints to connect (is rest enabled for this node?): ") +
                node->toString());

    TGridClientCommandExecutorPtr exec = executor();

    if (routing) {
        TGridClientRouterBalancerPtr balancer = sharedData->clientConfiguration().routerBalancer();

        while (!addrs.empty()) {
            unsigned int idx = balancer->balancedRouter(addrs);

            try {
                c.apply(node, addrs[idx], *exec); //networking here

                return true;
            }
            catch (GridClientConnectionResetException&) {
                addrs.erase(addrs.begin() + idx);
            }
            catch (GridServerUnreachableException&) {
                addrs.erase(addrs.begin() + idx);
            }
        }
    }
    else { //no routing
        int numAddrs = addrs.size();

        for (size_t addrIdx = 0; addrIdx < numAddrs; ++addrIdx) {
            try {
                c.apply(node, addrs[addrIdx], *exec); //networking here

                return true;
            }
            catch (GridClientConnectionResetException&) {
                prjLsnr.onNodeIoFailed(*node);
            }
            catch (GridServerUnreachableException&) {
                prjLsnr.onNodeIoFailed(*node);
            }
        }
    }

    return false;
}

TGridClientNodePtr GridClientProjectionImpl::affinityNode(const string& cacheName,
        const GridHasheableObject& affKey) {
    TNodesSet nodes;

    subProjectionNodes(nodes, GridCacheNameFilter(cacheName));

    std::shared_ptr<GridClientDataAffinity> affinity =
        affinityMap.count(cacheName) > 0 ? affinityMap[cacheName] : dfltAffinity;

    return affinity->getNode(nodes, affKey);
}

TGridClientNodePtr GridClientProjectionImpl::balancedNode() const {
    TNodesSet subPrjNodes;

    subProjectionNodes(subPrjNodes);

    return balancedNode(subPrjNodes);
}

TGridClientNodePtr GridClientProjectionImpl::balancedNode(const string& cacheName) const {
    TNodesSet subPrjNodes;

    subProjectionNodes(subPrjNodes, GridCacheNameFilter(cacheName));

    return balancedNode(subPrjNodes);
}

TGridClientNodePtr GridClientProjectionImpl::balancedNode(const TNodesSet& nodes) const {
    TGridClientNodeList nodeLst;

    for (auto iter = nodes.begin(); iter != nodes.end(); ++iter)
        nodeLst.push_back(TGridClientNodePtr(new GridClientNode(*iter)));

    return loadBalancer()->balancedNode(nodeLst);
}

/**
 * Get the list of the sub-projection nodes.
 *
 * @param subPrjNodes TNodesSet - The list of sub-Projection nodes.
 */
void GridClientProjectionImpl::subProjectionNodes(TNodesSet& subPrjNodes,
        const TGridClientNodePredicate& dynamicFilter) const {
    TNodesSet nodes = topologyNodes();

    for (auto it = nodes.begin(); it != nodes.end(); ++it) {
        if ((!filter || filter->apply(*it)) && dynamicFilter.apply(*it))
            subPrjNodes.insert(*it);
    }

    if (subPrjNodes.empty())
        throw GridClientException("Empty topology, make sure nodes are started and connection parameters are correct.");
}
