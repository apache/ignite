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
#include <sstream>
#include <boost/lexical_cast.hpp>

#include "gridgain/impl/cmd/gridclienttcpcommandexecutor.hpp"
#include "gridgain/impl/connection/gridclienttcpconnection.hpp"
#include "gridgain/impl/marshaller/protobuf/gridclientprotobufmarshaller.hpp"
#include "gridgain/impl/connection/gridclientconnectionpool.hpp"
#include "gridgain/gridclientexception.hpp"
#include "gridgain/impl/utils/gridclientlog.hpp"

using namespace org::gridgain::grid::kernal::processors::rest::client::message;

/**
 * Sends a log command to a remote host.
 *
 * @param host Host/port pair to connect to.
 * @param logCmd Log command to send.
 * @param rslt Log response message to fill.
 */
void GridClientTcpCommandExecutor::executeLogCmd(const GridSocketAddress& host, GridLogRequestCommand& logCmd,
        GridClientMessageLogResult& rslt) {
    executeCmd(host, logCmd, rslt);
}

/**
 * Sends a topology command to a remote host.
 *
 * @param host Host/port pair to connect to.
 * @param topCmd Topology command to send.
 * @param rslt Topology response message to fill.
 */
void GridClientTcpCommandExecutor::executeTopologyCmd(const GridSocketAddress& host, GridTopologyRequestCommand& topCmd,
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
void GridClientTcpCommandExecutor::executeGetCacheCmd(const GridSocketAddress& host, GridCacheRequestCommand& cacheCmd,
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
void GridClientTcpCommandExecutor::executeModifyCacheCmd(const GridSocketAddress& host,
        GridCacheRequestCommand& cacheCmd, GridClientMessageCacheModifyResult& rslt) {
    executeCmd(host, cacheCmd, rslt);
}

/**
 * Sends a cache metrics command to a remote host.
 *
 * @param host Host/port pair to connect to.
 * @param cacheCmd Cache metrics command to send.
 * @param rslt Cache metrics response message to fill.
 */
void GridClientTcpCommandExecutor::executeGetCacheMetricsCmd(const GridSocketAddress& host,
        GridCacheRequestCommand& cacheCmd, GridClientMessageCacheMetricResult& rslt) {
    executeCmd(host, cacheCmd, rslt);
}

/**
 * Sends a task command to a remote host.
 *
 * @param host Host/port pair to connect to.
 * @param taskCmd task command to send.
 * @param rslt Task response message to fill.
 */
void GridClientTcpCommandExecutor::executeTaskCmd(const GridSocketAddress& host, GridTaskRequestCommand& taskCmd,
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
template<class C, class R> void GridClientTcpCommandExecutor::executeCmd(const GridSocketAddress& host, C& cmd,
        R& rslt) {
    ObjectWrapper protoMsg;

    std::shared_ptr<GridClientTcpConnection> conn = connPool->rentTcpConnection(host.host(), host.port());

    cmd.sessionToken(conn->sessionToken());

    GridClientProtobufMarshaller::wrap(cmd, protoMsg);

    GG_LOG_DEBUG("Trying to execute requestId [%lld] on [%s:%d], sending [%s] request.",
            cmd.getRequestId(), host.host().c_str(), host.port(), GridClientProtobufMarshaller::msgType2Str(protoMsg).c_str());

    GridClientTcpPacket tcpPacket;
    GridClientTcpPacket tcpResponse;
    ProtoRequest req;

    tcpPacket.setData(protoMsg);
    tcpPacket.setAdditionalHeaders(cmd);

    try {
        sendPacket(conn, tcpPacket, tcpResponse);

        connPool->turnBack(conn);
    }
    catch (GridClientException& e) {
        GG_LOG_DEBUG("Failed to execute requestId [%lld] on [%s:%d]: %s",
                cmd.getRequestId(), host.host().c_str(), host.port(), e.what());

        throw;
    }

    ObjectWrapper respMsg = tcpResponse.getData();

    GG_LOG_DEBUG("Successfully executed requestId [%lld] on [%s:%d], received [%s] response.",
            cmd.getRequestId(), host.host().c_str(), host.port(), GridClientProtobufMarshaller::msgType2Str(respMsg).c_str());

    GridClientProtobufMarshaller::unwrap(respMsg, rslt);
}

/**
 * Stops the command executor freeing all resources
 * and closing all connections.
 */
void GridClientTcpCommandExecutor::stop() {
    connPool->close();
}

/**
 * Sends a binary packet to a remote host.
 *
 * @param conn Connection to use.
 * @param msg Protobuf ObjectWrapper to serialize and send to the host.
 * @param respMsg Protobuf ObjectWrapper response to fill.
 */
void GridClientTcpCommandExecutor::sendPacket(std::shared_ptr<GridClientTcpConnection> conn,
	const GridClientTcpPacket& tcpPacket, GridClientTcpPacket& tcpResponse) {
    try {
        conn->send(tcpPacket, tcpResponse);
    }
    catch (GridClientException&) {
        connPool->steal(conn);

        conn->close();

        throw;
    }
}
