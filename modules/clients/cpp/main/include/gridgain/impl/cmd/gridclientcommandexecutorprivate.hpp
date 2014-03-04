/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLIENTCOMMANDEXECUTORPRIVATE_HPP_
#define GRIDCLIENTCOMMANDEXECUTORPRIVATE_HPP_

#include "gridclientcommandexecutor.hpp"

class GridClientCommandExecutorPrivate: public GridClientCommandExecutor {
public:
    /**
     * Stops the command executor freeing all resources
     * and closing all connections.
     */
    virtual void stop() = 0;
};

#endif /* GRIDCLIENTCOMMANDEXECUTORPRIVATE_HPP_ */
