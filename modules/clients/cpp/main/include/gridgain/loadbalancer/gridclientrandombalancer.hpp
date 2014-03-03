/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_RANDOM_BALANCER_HPP_INCLUDED
#define GRID_CLIENT_RANDOM_BALANCER_HPP_INCLUDED

#include <gridgain/loadbalancer/gridclientloadbalancer.hpp>

/**
 * Random balancer.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GRIDGAIN_API GridClientRandomBalancer : public GridClientLoadBalancer {
public:
    /** Default constructor. */
    GridClientRandomBalancer();

    /**
     * Gets next node for executing client command.
     *
     * @param nodes Nodes to pick from.
     * @return Next node to pick.
     */
    virtual TGridClientNodePtr balancedNode(const TGridClientNodeList& nodes);
};

#endif
