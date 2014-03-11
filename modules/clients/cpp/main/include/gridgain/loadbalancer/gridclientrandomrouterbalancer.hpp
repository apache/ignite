/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLIENTRANDOMROUTERBALANCER_HPP_
#define GRIDCLIENTRANDOMROUTERBALANCER_HPP_

#include <gridgain/gridconf.hpp>
#include <gridgain/gridclienttypedef.hpp>
#include "gridgain/loadbalancer/gridclientrouterbalancer.hpp"

class GRIDGAIN_API GridClientRandomRouterBalancer: public GridClientRouterBalancer {
public:
    /**
     * Default constructor.
     */
    GridClientRandomRouterBalancer();

    /**
     * Gets next router for executing client command.
     *
     * @param addrs Address list to pick router from.
     * @return Index of the next router address to pick from list.
     */
    virtual unsigned int balancedRouter(const TGridClientSocketAddressList& addrs);
};


#endif /* GRIDCLIENTRANDOMROUTERBALANCER_HPP_ */
