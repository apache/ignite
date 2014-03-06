/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCONSISTENCEHASHIMPL_HPP__INCLUDED
#define GRIDCONSISTENCEHASHIMPL_HPP__INCLUDED

#include <map>
#include <set>
#include <utility>
#include <memory>

#include "gridgain/gridclientuuid.hpp"

/**
 * Node info.
 */
class NodeInfo {
public:
    /**
     * @param id ID.
     * @param hashID Hash ID.
     */
    NodeInfo(const GridUuid& id, const std::shared_ptr<GridHasheableObject>& hashId): mId(id), mHashId(hashId) {}

    /**
     * Copy constructor.
     *
     * @param from Object to copy from.
     */
    NodeInfo(const NodeInfo& from): mId(from.mId), mHashId(from.mHashId) {}

    /**
     * Assignment operator.
     *
     * @param right Right hand side object.
     */
    NodeInfo& operator=(const NodeInfo& right) {
        mId = right.id();
        mHashId = right.hashId();

        return *this;
    }

    /**
     * Equality operator.
     *
     * @param right Right hand side object.
     */
    bool operator==(const NodeInfo& right) const {
        return mId == right.id() && mHashId->hashCode() == right.hashId()->hashCode();
    }

    /**
     * Less than operator.
     *
     * @param right Right hand side object.
     */
    bool operator<(const NodeInfo& right) const {
        if (mId == right.id())
            return mHashId->hashCode() < right.hashId()->hashCode();

        return mId < right.id();
    }

    /**
     * @return ID.
     */
    GridUuid id() const {
        return mId;
    }

    /**
     * @return Hash ID.
     */
    std::shared_ptr<GridHasheableObject> hashId() const {
        return mHashId;
    }

private:
    /** ID. */
    GridUuid mId;

    /** Hash ID. */
    std::shared_ptr<GridHasheableObject> mHashId;
};

/**
 * Implementation of GridClientConsistentHash.
 */
class GridClientConsistentHashImpl {
public:

    /**
     * Default constructor.
     */
    GridClientConsistentHashImpl();

    /**
     * Destructor.
     */
    virtual ~GridClientConsistentHashImpl() {
        // No-op.
    }

    /**
     * Adds a node to consistent hash algorithm.
     *
     * @param node New node (if <tt>null</tt> then no-op).
     * @param replicas Number of replicas for the node.
     * @return <tt>True</tt> if node was added, <tt>false</tt> if it is <tt>null</tt> or
     * is already contained in the hash.
     */
    virtual void addNode(const NodeInfo& node, int replicas);

    /**
     * Removes a node and all of its replicas.
     *
     * @param node Node to remove.
     */
    virtual void removeNode(const NodeInfo& node);

    /**
     * Gets set of all distinct nodes in the consistent hash (in no particular order).
     *
     * @return Set of all distinct nodes in the consistent hash.
     */
    virtual std::set<NodeInfo> getNodes() const;

    /**
     * Gets node for a key.
     *
     * @param key Key.
     * @param inc Node IDs to include.
     * @return Node.
     */
    virtual NodeInfo node(const GridHasheableObject& value, const std::set<NodeInfo>& inc) const;

    /**
     * Gets the total of the unique nodes.
     *
     * @return The count of unique nodes.
     */
    size_t nodesSize() const {
        return nodes.size();
    }

    /**
     * Gets the total elements in the cycle.
     *
     * @return The count of elements in the circle.
     */
    size_t circlesSize() const {
        return circle.size();
    }

    /**
     * Removes all the nodes and their replicas from the ring.
     */
    void clear() {
        circle.clear();
        nodes.clear();
    }

    /**
     * Calculates hash for a hashable object.
     *
     * @param key Object to calculate hash for.
     * @return Hash code.
     */
    int32_t hash(const GridHasheableObject& key) const;

private:
    /**
     * Adds node to circle.
     *
     * @param node Node to add.
     * @param hashCode Hash code.
     * @return If node was really added.
     */
    void addNodeToCircle(const NodeInfo& node, const int32_t hashCode);

    /**
     * Calculates hash code for a long value.
     *
     * @param value What to calculate hash for.
     * @return Hash code.
     */
    static int32_t hashCode(long value);

    /** Consistent hash circle. */
    std::map<int32_t, std::set<NodeInfo> > circle;

    /** List of nodes. */
    std::set<NodeInfo> nodes;

    /** Affinity seed. */
    long affSeed;
};

#endif
