// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include <cstdio>
#include <cstdlib>

#include "gridgain/gridclientvariant.hpp"
#include "gridgain/impl/cmd/gridclienthttpcommandexecutor.hpp"
#include "gridgain/impl/connection/gridclienthttpconnection.hpp"

/**
 * Stops the command executor freeing all resources
 * and closing all connections.
 */
void GridClientHttpCommandExecutor::stop() {
    connPool->close();
}

/**
 * Sends a request to the specified host
 *
 * @param host Host/port pair to connect to.
 * @param msg Message to send.
 * @param respMsg Response message to fill.
 */
void GridClientHttpCommandExecutor::sendPacket(const GridSocketAddress& host,
        const TRequestParams& msg,
        TJson& respMsg) {
    std::shared_ptr<GridClientHttpConnection> conn =
            connPool->rentHttpConnection(host.host(), host.port());

    try {
        conn->send(msg, respMsg);

        connPool->turnBack(conn);
    }
    catch(GridClientException&) {
        connPool->steal(conn); // Throw this connection away.

        conn->close();

        throw;
    }
}

/**
 * Sends a log command to a remote host.
 *
 * @param host Host/port pair to connect to.
 * @param logCmd Log command to send.
 * @param rslt Log response message to fill.
 */
void GridClientHttpCommandExecutor::executeLogCmd(const GridSocketAddress& host,
        GridLogRequestCommand& logCmd, GridClientMessageLogResult& rslt) {
    executeCmd(host, logCmd, rslt);
}

/**
 * Sends a topology command to a remote host.
 *
 * @param host Host/port pair to connect to.
 * @param topCmd Topology command to send.
 * @param rslt Topology response message to fill.
 */
void GridClientHttpCommandExecutor::executeTopologyCmd(
        const GridSocketAddress& host, GridTopologyRequestCommand& topCmd,
        GridClientMessageTopologyResult& rslt) {

    executeCmd(host, topCmd, rslt);
}

/**
 * Sends a cache get command to a remote host.
 *
 * @param host Host/port pair to connect to.
 * @param cacheCmd Cache command to send.
 * @param rslt Cache get response message to fill.
 */
void GridClientHttpCommandExecutor::executeGetCacheCmd(
        const GridSocketAddress& host, GridCacheRequestCommand& cacheCmd,
        GridClientMessageCacheGetResult& rslt) {
    executeCmd(host, cacheCmd, rslt);
}

/**
 * Sends a cache modify command to a remote host.
 *
 * @param host Host/port pair to connect to.
 * @param cacheCmd Cache modify command to send.
 * @param rslt Cache modify response message to fill.
 */
void GridClientHttpCommandExecutor::executeModifyCacheCmd(
        const GridSocketAddress& host, GridCacheRequestCommand& cacheCmd,
        GridClientMessageCacheModifyResult& rslt) {
    executeCmd(host, cacheCmd, rslt);
}

/**
 * Sends a cache metrics command to a remote host.
 *
 * @param host Host/port pair to connect to.
 * @param cacheCmd Cache metrics command to send.
 * @param rslt Cache metrics response message to fill.
 */
void GridClientHttpCommandExecutor::executeGetCacheMetricsCmd(
        const GridSocketAddress& host, GridCacheRequestCommand& cacheCmd,
        GridClientMessageCacheMetricResult& rslt) {
    executeCmd(host, cacheCmd, rslt);
}

/**
 * Sends a task command to a remote host.
 *
 * @param host Host/port pair to connect to.
 * @param taskCmd task command to send.
 * @param rslt Task response message to fill.
 */
void GridClientHttpCommandExecutor::executeTaskCmd(const GridSocketAddress& host,
        GridTaskRequestCommand& taskCmd,
        GridClientMessageTaskResult& rslt) {
    executeCmd(host, taskCmd, rslt);
}

/**
 * Sends a general command to a remote host.
 *
 * @param host Host/port pair to connect to.
 * @param cmd Command to send.
 * @param rslt Response message to fill.
 */
template<class C, class R> void GridClientHttpCommandExecutor::executeCmd(
        const GridSocketAddress& host, C& cmd, R& rslt) {
    TRequestParams protoMsg;
    TJson respMsg;

    GridClientJsonMarshaller::wrap(cmd, protoMsg);

    sendPacket(host, protoMsg, respMsg);

    GridClientJsonMarshaller::unwrap(respMsg, rslt);
}
