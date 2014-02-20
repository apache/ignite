// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include <cstdlib>
#include <ctime>

#include "gridgain/loadbalancer/gridclientrandomrouterbalancer.hpp"

GridClientRandomRouterBalancer::GridClientRandomRouterBalancer() {
    srand((int)time(NULL));
}

unsigned int GridClientRandomRouterBalancer::balancedRouter(const TGridSocketAddressList& addrs) {
    size_t n = rand() % addrs.size();

    return n;
}
