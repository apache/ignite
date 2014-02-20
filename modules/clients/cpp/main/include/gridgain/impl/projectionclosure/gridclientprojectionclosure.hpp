// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_PROJECTION_CLOSURE_HPP_INCLUDED
#define GRID_CLIENT_PROJECTION_CLOSURE_HPP_INCLUDED

#include "gridgain/gridclienttypedef.hpp"

class GridSocketAddress;
class GridClientCommandExecutor;

/**
 * Basic class for all closures.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class ClientProjectionClosure {
public:
    /** Virtual destructor. */
    virtual ~ClientProjectionClosure() {}

    /** Apply executor to a certain connection parameters.
     *
     * @param node Node to apply this closure to.
     * @param connParams Host/port pair.
     * @param cmdExecutor Command executor.
     */
    virtual void apply(TGridClientNodePtr node, GridSocketAddress connParams, GridClientCommandExecutor& cmdExecutor) = 0;
};

#endif
