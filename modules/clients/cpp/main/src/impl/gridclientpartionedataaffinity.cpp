// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include <map>
#include <vector>

#include "gridgain/impl/gridclientpartitionedaffinity.hpp"
#include "gridgain/gridclientnode.hpp"
#include "gridgain/impl/gridclientprojection.hpp"
#include "gridgain/gridclientvariant.hpp"
#include "gridgain/impl/hash/gridclientsimpletypehasheableobject.hpp"
#include "gridgain/impl/hash/gridclientvarianthasheableobject.hpp"

using namespace std;

const char* GridClientPartitionedAffinity::DFLT_REPLICA_CNT_ATTR_NAME = "gg:affinity:node:replicas";

GridClientPartitionedAffinity::GridClientPartitionedAffinity() {
    parts = DFLT_PARTITION_CNT;
    replicas = DFLT_REPLICA_CNT;
    backups = DFLT_BACKUP_CNT;
    attrName = DFLT_REPLICA_CNT_ATTR_NAME;

    hashIdResolver = [] (const GridClientNode& node) {
        return node.getConsistentId();
    };
}

GridClientPartitionedAffinity::~GridClientPartitionedAffinity() {
}

int GridClientPartitionedAffinity::getDefaultReplicas() const {
    return replicas;
}

void GridClientPartitionedAffinity::setDefaultReplicas(int pReplicas) {
    replicas = pReplicas;
}

int GridClientPartitionedAffinity::getKeyBackups() const {
    return backups;
}

void GridClientPartitionedAffinity::setKeyBackups(int pBackups) {
    backups = pBackups;
}

int GridClientPartitionedAffinity::getPartitions() const {
    return parts;
}

void GridClientPartitionedAffinity::setPartitions(int pParts) {
    parts = pParts;
}

const TGridClientNodePredicatePtr GridClientPartitionedAffinity::getBackupFilter() {
    return backupFilter;
}

void GridClientPartitionedAffinity::setBackupFilter(const TGridClientNodePredicatePtr pBackupFilter) {
    backupFilter = pBackupFilter;
}

std::string GridClientPartitionedAffinity::getReplicaCountAttributeName() const {
    return attrName;
}

void GridClientPartitionedAffinity::setReplicaCountAttributeName(const string& pAttrName) {
    attrName = pAttrName;
}

GridHashIdResolver GridClientPartitionedAffinity::getHashIdResolver() const {
    return hashIdResolver;
}

void GridClientPartitionedAffinity::setHashIdResolver(GridHashIdResolver& pHashIdResolver) {
    hashIdResolver = pHashIdResolver;
}

int GridClientPartitionedAffinity::getReplicas(const GridClientNode& n) {
    if (n.getReplicaCount() > 0)
        return n.getReplicaCount();

    TGridClientVariantMap attrs = n.getAttributes();

    if (attrs.count(attrName) > 0)
        return atoi(attrs[attrName].toString().c_str());

    return replicas;
}

void GridClientPartitionedAffinity::add(const GridClientNode& node, int replicas) {
    nodeHash.addNode(
        NodeInfo(
            node.getNodeId(),
            std::shared_ptr<GridHasheableObject>(new GridClientVariantHasheableObject(hashIdResolver(node)))),
        replicas);

    addedNodes.insert(node.getNodeId());
}

TGridClientNodePtr GridClientPartitionedAffinity::getNode(const TNodesSet& nodes, const GridHasheableObject& key) {
    set<GridUuid> newNodes;

    for (auto iter = nodes.begin(); iter != nodes.end(); ++iter)
        newNodes.insert(iter->getNodeId());

    GridUuid nodeId;

    {
        boost::lock_guard<boost::mutex> lock(mux);

        if (addedNodes != newNodes) {
            // Recreate the consistent hash ring.
            addedNodes.clear();
            nodeHash.clear();

            for (auto iter = nodes.begin(); iter != nodes.end(); ++iter) {
                GridClientNode n = *iter;

                add(n, getReplicas(n));
            }
        }

        int32_t part = abs(key.hashCode() % parts);

        std::set<NodeInfo> nInfos;

        for (TNodesSet::const_iterator i = nodes.begin(); i != nodes.end(); i++)
            nInfos.insert(NodeInfo(i->getNodeId(),
                std::shared_ptr<GridHasheableObject>(new GridClientVariantHasheableObject(hashIdResolver(*i)))));

        nodeId = nodeHash.node(GridInt32Hasheable(part), nInfos).id();
    }

    return findNode(nodeId, nodes);
}

TGridClientNodePtr GridClientPartitionedAffinity::findNode(const GridUuid& id, const TNodesSet& nodes) const {
    TNodesSet::const_iterator iter = find_if(nodes.begin(), nodes.end(), [&id] (const GridClientNode& node) {
        return node.getNodeId() == id;
    });

    assert(iter != nodes.end());

    return TGridClientNodePtr(new GridClientNode(*iter));
}

void GridClientPartitionedAffinity::checkRemoved(const GridClientNode& node) {
    boost::lock_guard<boost::mutex> lock(mux);

    addedNodes.erase(find_if(addedNodes.begin(), addedNodes.end(), [&node] (const GridUuid& id) {
        return id == node.getNodeId();
    }));

    nodeHash.removeNode(NodeInfo(node.getNodeId(),
        std::shared_ptr<GridHasheableObject>(new GridClientVariantHasheableObject(hashIdResolver(node)))));
}
