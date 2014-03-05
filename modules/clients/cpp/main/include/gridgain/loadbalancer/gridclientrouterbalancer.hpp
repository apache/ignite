/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLIENTROUTERBALANCER_HPP_
#define GRIDCLIENTROUTERBALANCER_HPP_

#include <gridgain/gridconf.hpp>
#include <gridgain/gridclienttypedef.hpp>
#include <gridgain/gridsocketaddress.hpp>

/**
 * Interface that defines a router selection logic for a particular operation
 * (e.g. task run or cache operation in case of pinned mode).
 */
class GRIDGAIN_API GridClientRouterBalancer {
public:
    /** Destructor. */
    virtual ~GridClientRouterBalancer() {}

    /**
     * Gets next router for executing client command.
     *
     * @param addrs Address list to pick router from.
     * @return Index of the next router address to pick from the list.
     */
    virtual unsigned int balancedRouter(const TGridSocketAddressList& addrs) = 0;
};

#endif /* GRIDCLIENTROUTERBALANCER_HPP_ */
