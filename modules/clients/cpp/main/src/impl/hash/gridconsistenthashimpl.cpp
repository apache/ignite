/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include <iostream>
#include <map>
#include <cstdio>
#include <ctime>
#include <algorithm>
#include <functional>

#include "gridgain/impl/hash/gridclientsimpletypehasheableobject.hpp"
#include "gridgain/impl/gridconsistenhashimpl.hpp"
#include "gridgain/impl/utils/gridclientlog.hpp"

GridClientConsistentHashImpl::GridClientConsistentHashImpl() {
    srand((int)time(NULL));

    affSeed = 15485857;
}

void GridClientConsistentHashImpl::addNode(const NodeInfo& node, int replicas) {
    if (nodes.count(node) == 0) {
        nodes.insert(node);

        int64_t seed = hashCode(affSeed) * 31 + hash(*node.hashId());

        int32_t hashVal = hash(GridInt64Hasheable(seed));

        addNodeToCircle(node, hashVal);

        for (int i = 1; i <= replicas; i++) {
            seed = seed * hashCode(affSeed) + i;

            hashVal = hash(GridInt64Hasheable(seed));

            addNodeToCircle(node, hashVal);
        }
    }
}

void GridClientConsistentHashImpl::addNodeToCircle(const NodeInfo& node, const int32_t hashCode) {
    circle[hashCode].insert(node);
}

void GridClientConsistentHashImpl::removeNode(const NodeInfo& node) {
    if (nodes.erase(node) != 0) {
        for (auto iter = circle.begin(); iter != circle.end(); iter++)
            iter->second.erase(node);

        GG_LOG_DEBUG("Removed node ID: %s", node.id().uuid().c_str());
    }
    else
        GG_LOG_DEBUG("Failed to find node ID: %s", node.id().uuid().c_str());
}

std::set<NodeInfo> GridClientConsistentHashImpl::getNodes() const {
    return nodes; // Copy.
}

template<class K, class V>
typename std::map<K, V>::const_iterator inclusiveUpperBound(const std::map<K, V>& m, const K& k) {
    if (m.empty())
        return m.end();

    typename std::map<K, V>::const_iterator ub = m.upper_bound(k); //exclusive

    if (ub != m.begin()) {
        --ub; //step back one element

        if (ub->first == k)
            return ub;

        //the current key is less then k
        //step forward again
        ub++;
    }

    return ub;
}

NodeInfo GridClientConsistentHashImpl::node(const GridClientHasheableObject& value, const std::set<NodeInfo>& inc) const {
    int32_t hashVal = hash(value); //(matches)

    // Point to the nodeId that compares greater than this key.
    std::map<int32_t, std::set<NodeInfo> >::const_iterator itFoundKey = inclusiveUpperBound(circle, hashVal);

    itFoundKey = itFoundKey != circle.end() ? itFoundKey : circle.begin();

    std::set<NodeInfo> failed;

    while (itFoundKey != circle.end()) {
        for (auto iter = itFoundKey->second.begin(); iter != itFoundKey->second.end(); iter++) {
            if (inc.count(*iter) != 0)
                return *iter;
            else if (failed.insert(*iter).second && failed.size() == nodes.size())
                return NodeInfo(GridClientUuid(), std::shared_ptr<GridClientHasheableObject>());
        }

        itFoundKey++;
    }

    return NodeInfo(GridClientUuid(), std::shared_ptr<GridClientHasheableObject>());
}

int32_t GridClientConsistentHashImpl::hash(const GridClientHasheableObject& key) const {
    int32_t h = key.hashCode();

    // Spread bits to hash code.
    h += (h <<  15) ^ 0xffffcd7d;
    h ^= ((uint32_t)h >> 10);
    h += (h <<   3);
    h ^= ((uint32_t)h >>  6);
    h += (h <<   2) + (h << 14);

    return h ^ ((uint32_t)h >> 16);
}

int32_t GridClientConsistentHashImpl::hashCode(long code) {
    GridInt64Hasheable obj(code);

    return obj.hashCode();
}
