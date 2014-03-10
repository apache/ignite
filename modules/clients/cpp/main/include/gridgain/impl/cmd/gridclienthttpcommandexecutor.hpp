/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_HTTP_COMMAND_EXECUTOR_HPP_INCLUDED
#define GRID_CLIENT_HTTP_COMMAND_EXECUTOR_HPP_INCLUDED

#include <boost/thread.hpp>

#include "gridgain/gridsocketaddress.hpp"
#include "gridgain/impl/cmd/gridclientcommandexecutorprivate.hpp"
#include "gridgain/impl/connection/gridclientconnectionpool.hpp"
#include "gridgain/impl/marshaller/json/gridclientjsonmarshaller.hpp"

/**
 * HTTP command executor. Sends commands over HTTP transport.
 */
class GridClientHttpCommandExecutor : public GridClientCommandExecutorPrivate {
public:
    /**
     * Public constructor.
     *
     * @param connectionPool Connection pool to use.
     */
    GridClientHttpCommandExecutor(boost::shared_ptr<GridClientConnectionPool>& connectionPool) :
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
            GridCacheRequestCommand& cacheCmd, GridClientMessageCacheGetResult& result);

    /**
     * Execute cache modify command.
     *
     * @param nodeHost Host/port to send command to.
     * @param cacheCmd Cache modify request command.
     * @param result Cache modify request result.
     */
    virtual void executeModifyCacheCmd(const GridClientSocketAddress& nodeHost,
            GridCacheRequestCommand& cacheCmd, GridClientMessageCacheModifyResult& result);

    /**
     * Execute cache metrics command.
     *
     * @param nodeHost Host/port to send command to.
     * @param cacheCmd Cache metrics request command.
     * @param result Cache metrics request result.
     */
    virtual void executeGetCacheMetricsCmd(const GridClientSocketAddress& nodeHost,
            GridCacheRequestCommand& cacheCmd, GridClientMessageCacheMetricResult& result);

    /**
     * Execute task command.
     *
     * @param nodeHost Host/port to send command to.
     * @param taskCmd task request command.
     * @param result task request result.
     */
    virtual void executeTaskCmd(const GridClientSocketAddress& nodeHost, GridTaskRequestCommand& taskCmd,
            GridClientMessageTaskResult& result);

    /**
     * Stops the command executor freeing all resources
     * and closing all connections.
     */
    virtual void stop();

private:
    /**
     * Send key/value pairs over HTTP and return property tree.
     *
     * @param host Host/port pair to send request to.
     * @param msg Message key/value pairs.
     * @param respMsg Response object that will be filled with response data.
     */
    void sendPacket (const GridClientSocketAddress& host, const TRequestParams& msg,
            TJson& respMsg);

    /**
     * Execute a generic command and receive a generic response.
     *
     * @param nodeHost Host/port to send command to.
     * @param cmd Command to send.
     * @param response Response object to fill.
     */
    template <class C, class R> void executeCmd(const GridClientSocketAddress& nodeHost, C& cmd, R& response);

    /** Connection pool. */
    boost::shared_ptr<GridClientConnectionPool> connPool;

    /** Synchronization for underlying socket. */
    mutable boost::mutex mux;
};

#endif
