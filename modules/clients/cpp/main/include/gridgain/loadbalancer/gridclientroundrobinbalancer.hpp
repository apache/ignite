// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_ROUNDROUBIN_BALANCER_HPP_INCLUDED
#define GRID_CLIENT_ROUNDROUBIN_BALANCER_HPP_INCLUDED

#include <gridgain/loadbalancer/gridclientloadbalancer.hpp>

/**
 * Round robin balancer.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GRIDGAIN_API GridClientRoundRobinBalancer : public GridClientLoadBalancer {
public:
    /** Default constructor. */
    GridClientRoundRobinBalancer() : nodePos(0) { };

    /**
     * Gets next node for executing client command.
     *
     * @param nodes Nodes to pick from.
     * @return Next node to pick.
     */
    virtual TGridClientNodePtr balancedNode(const TGridClientNodeList& nodes);

private:
    /** Position of last node served in the list. */
    unsigned int nodePos;
};

#endif
