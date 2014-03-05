/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_TOPOLOGY_HPP_INCLUDE
#define GRID_CLIENT_TOPOLOGY_HPP_INCLUDE

#include <set>

#include <boost/thread.hpp>

#include "gridgain/gridclientnode.hpp"

class GridUuid;

/**
 * Topology holder class.
 */
class GridClientTopology {
public:
    /** Updates current topology with new nodes information. */
    void update(const TNodesSet& pNodes);

    /** Removes some nodes from current topology. */
    void remove(const TNodesSet& pNodes);

    /**
     * Returns current list of nodes in the topology.
     *
     * @return A copy of the topology node cache.
     */
    TNodesSet nodes() const;

    /**
     * Retrieves a node from topology by id.
     *
     * @param uuid Node id.
     * @return Shared pointer to a node.
     */
    const TGridClientNodePtr node(const GridUuid& uuid) const;

    /**
     * Empties the topology cache. Next topology event will repopulate the cache with the current grid nodes.
     */
    void reset();

protected:
    mutable boost::shared_mutex mux_;

    /** Nodes. */
    TNodesSet nodes_;
};

#endif
