/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include "gridgain/gridclientvariant.hpp"
#include "gridgain/impl/cmd/gridclientcommandexecutorfactory.hpp"
#include "gridgain/impl/cmd/gridclienttcpcommandexecutor.hpp"

/**
 * Creates command executor based on protocol configuration.
 *
 * @param protoCfg protocol configuration
 * @param connPool connection pool
 * @return Command executor based on that configuration using that connection pool.
 */
boost::shared_ptr<GridClientCommandExecutor> GridClientCommandExecutorFactory::createCommandExecutor
    (const GridClientProtocolConfiguration& protoCfg, boost::shared_ptr<GridClientConnectionPool>& connPool) {
    boost::shared_ptr<GridClientCommandExecutor> cmdExecutor(new GridClientTcpCommandExecutor(connPool));

    return cmdExecutor;
}
