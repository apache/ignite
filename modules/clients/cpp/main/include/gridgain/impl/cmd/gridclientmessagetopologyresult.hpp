// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLEINT_MESSAGE_TOPOLOGY_RESULT_HPP_INCLUDED
#define GRID_CLEINT_MESSAGE_TOPOLOGY_RESULT_HPP_INCLUDED

#include "gridgain/impl/cmd/gridclientmessageresult.hpp"
#include "gridgain/gridclientnode.hpp"

/** Type definition for nodes list. */
typedef std::vector<GridClientNode> TNodesList;

/**
 * Topology result message.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GridClientMessageTopologyResult : public GridClientMessageResult {
private:
    /** List of nodes in topology. */
    TNodesList nodes_;
public:
    /**
     * Getter method for the list of nodes.
     *
     * @return list of nodes in the topology.
     */
    const TNodesList getNodes() const {
        return nodes_;
    }

    /**
     * Setter method for the list of nodes.
     *
     * @param nodes Collection of nodes - new value for the nodes in topology.
     */
    void setNodes(const TNodesList& nodes) {
        nodes_ = nodes;
    }
};
#endif
