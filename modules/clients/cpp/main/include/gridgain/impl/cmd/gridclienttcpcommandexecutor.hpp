/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_TCP_COMMAND_EXECUTOR_HPP_INCLUDED
#define GRID_CLIENT_TCP_COMMAND_EXECUTOR_HPP_INCLUDED

#include "gridgain/impl/cmd/gridclientcommandexecutorprivate.hpp"
#include "gridgain/impl/connection/gridclienttcpconnection.hpp"

/** Forward declaration. */
class GridClientConnectionPool;
class GridClientSocketAddress;

namespace org {
namespace gridgain {
namespace client {
namespace message {
/** Forward declaration. */
class ObjectWrapper;

}}}}

/**
 * TCP command executor. Sends commands over HTTP transport.
 */
class GridClientTcpCommandExecutor : public GridClientCommandExecutorPrivate {
public:
    /**
     * Public constructor.
     *
     * @param connectionPool Connection pool to use.
     */
    GridClientTcpCommandExecutor(boost::shared_ptr<GridClientConnectionPool> connectionPool) :
        connPool(connectionPool) { }

    /**
     * Execute log command.
     *
     * @param nodeHost Host/port to send command to.
     * @param logRequest Log request command.
     * @param result Log request result.
     */
    virtual void executeLogCmd(const GridClientSocketAddress& nodeHost,
            GridLogRequestCommand& logRequest, GridClientMessageLogResult& result);

    /**
     * Execute topology command.
     *
     * @param nodeHost Host/port to send command to.
     * @param topologyRequest Topology request command.
     * @param result Topology request result.
     */
    virtual void executeTopologyCmd(const GridClientSocketAddress& nodeHost,
            GridTopologyRequestCommand& topologyRequest, GridClientMessageTopologyResult& result);

    /**
     * Execute cache get command.
     *
     * @param nodeHost Host/port to send command to.
     * @param cacheCmd Cache get request command.
     * @param result Cache get request result.
     */
    virtual void executeGetCacheCmd(const GridClientSocketAddress& nodeHost,
            GridCacheRequestCommand& cacheCmd, GridClientMessageCacheGetResult&);

    /**
     * Execute cache modify command.
     *
     * @param nodeHost Host/port to send command to.
     * @param cacheCmd Cache modify request command.
     * @param result Cache modify request result.
     */
    virtual void executeModifyCacheCmd(const GridClientSocketAddress& nodeHost,
            GridCacheRequestCommand& cacheCmd, GridClientMessageCacheModifyResult&);

    /**
     * Execute cache metrics command.
     *
     * @param nodeHost Host/port to send command to.
     * @param cacheCmd Cache metrics request command.
     * @param result Cache metrics request result.
     */
    virtual void executeGetCacheMetricsCmd(const GridClientSocketAddress& nodeHost,
            GridCacheRequestCommand& cacheCmd, GridClientMessageCacheMetricResult&);

    /**
     * Execute task command.
     *
     * @param nodeHost Host/port to send command to.
     * @param taskCmd task request command.
     * @param result task request result.
     */
    virtual void executeTaskCmd(const GridClientSocketAddress& nodeHost, GridTaskRequestCommand& taskCmd,
            GridClientMessageTaskResult&);

    /**
     * Stops the command executor freeing all resources
     * and closing all connections.
     */
    virtual void stop();

private:
    /**
     * Sends protobuf wrapper object to the specified host and receives a reply.
     *
     * @param conn Connection to use.
     * @param tcpPacket TCP packet to send.
     * @param tcpResponse Response TCP packet.
     */
    void sendPacket (std::shared_ptr<GridClientTcpConnection> conn, const GridClientTcpPacket& tcpPacket,
            GridClientTcpPacket& tcpResponse);

    /**
     * Execute generic command on the node.
     *
     * @param nodeHost Host/port pair.
     * @param cmd Command to send.
     * @param response Response to fill.
     */
    template <class C, class R> void executeCmd(const GridClientSocketAddress& nodeHost, C& cmd, R& response);

    /** Connection pool. */
    boost::shared_ptr<GridClientConnectionPool> connPool;
};

#endif
