// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLIENTLOADBALANCER_HPP_INCLUDED
#define GRIDCLIENTLOADBALANCER_HPP_INCLUDED

#include <gridgain/gridconf.hpp>
#include <gridgain/gridclienttypedef.hpp>

/**
 * Interface that defines a selection logic of a server node for a particular operation
 * (e.g. task run or cache operation in case of pinned mode).
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GRIDGAIN_API GridClientLoadBalancer {
public:
    /** Destructor. */
    virtual ~GridClientLoadBalancer() {}

    /**
     * Gets next node for executing client command.
     *
     * @param nodes Node id to pick from.
     * @return Next node id to pick.
     */
    virtual TGridClientNodePtr balancedNode(const TGridClientNodeList& nodes) = 0;
};

#endif // GRIDCLIENTLOADBALANCER_HPP_INCLUDED
