// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include <iostream>
#include <deque>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/xtime.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
#include <boost/array.hpp>

#include "gridgain/impl/utils/gridclientbyteutils.hpp"
#include "gridgain/impl/connection/gridclienttcpconnection.hpp"
#include "gridgain/impl/connection/gridclientconnectionpool.hpp"
#include "gridgain/impl/cmd/gridclientmessageauthrequestcommand.hpp"
#include "gridgain/impl/cmd/gridclientmessageauthresult.hpp"
#include "gridgain/impl/marshaller/protobuf/gridclientprotobufmarshaller.hpp"
#include "gridgain/impl/marshaller/protobuf/ClientMessages.pb.h"
#include "gridgain/gridclientexception.hpp"

#include "gridgain/impl/utils/gridclientlog.hpp"
#include "gridgain/impl/utils/gridutil.hpp"

using namespace std;
using namespace boost;

#ifndef _MSC_VER

const int8_t GridClientTcpPacket::SIGNAL_CHAR;

const size_t GridClientTcpPacket::UUID_SIZE;

const size_t GridClientTcpPacket::ADDITIONAL_HEADERS_SIZE;

const size_t GridClientTcpPacket::BASIC_HEADER_SIZE;

const size_t GridClientTcpPacket::REGULAR_HEADER_SIZE;

#endif

/** Ping packet contents. */
static const int8_t PING_PACKET_SIZE_HEADER[] = { 0x0, 0x0, 0x0, 0x0 };

/** Version numeric array. */
const vector<int8_t> verByteVec = GridUtil::getVersionNumeric();

/** Client hint packet. */
const uint8_t HANDSHAKE_PACKET[] = {
        0x91,                                            // Signal char.
        (uint8_t)verByteVec[0],                          // Version.
        (uint8_t)verByteVec[1],                          //
        (uint8_t)verByteVec[2],                          //
        (uint8_t)verByteVec[3],                          //
        2                                                // GridClientProtobufMarshaller.PROTOCOL_ID
};

/**
 * Client handshake result codes.
 */
enum HandshakeResultCode {
    /** Successful handshake. */
    OK = 0,

    /** Version check failed. */
    ERR_VERSION_CHECK_FAILED = 1,

    /** Unknown protocol ID. */
    ERR_UNKNOWN_PROTO_ID = 2
};

using namespace org::gridgain::grid::kernal::processors::rest::client::message;

/** Fills packet with ping data. */
void GridClientTcpPacket::createPingPacket(GridClientTcpPacket& pingPacket) {
    pingPacket.sizeHeader.assign(PING_PACKET_SIZE_HEADER,
            PING_PACKET_SIZE_HEADER + sizeof(PING_PACKET_SIZE_HEADER) / sizeof(*PING_PACKET_SIZE_HEADER));
}

/** Checks if this is a ping packet. */
bool GridClientTcpPacket::isPingPacket() const {
    GridClientTcpPacket pingPacket;

    createPingPacket(pingPacket);

    return data == pingPacket.data;
}

size_t GridClientTcpPacket::getHeaderSize() const {
    return isPingPacket() ? BASIC_HEADER_SIZE : REGULAR_HEADER_SIZE;
}

void GridClientTcpPacket::setPacketSize(int32_t size) {
    GridClientByteUtils::valueToBytes(size, sizeHeader);
}

void GridClientTcpPacket::setData(const ObjectWrapper& protoMsg) {
    GridClientProtobufMarshaller::marshal(protoMsg, data);
    setPacketSize(ADDITIONAL_HEADERS_SIZE + data.size());
}

void GridClientTcpPacket::setData(int8_t* start, int8_t* end) {
    data.assign(start, end);
}

ObjectWrapper GridClientTcpPacket::getData() const {
    ObjectWrapper ret;
    GridClientProtobufMarshaller::unmarshal(data, ret);

    return ret;
}

size_t GridClientTcpPacket::getDataSize() const {
    return data.size();
}

void GridClientTcpPacket::setAdditionalHeaders(const GridClientMessage& msg) {
    GridClientProtobufMarshaller::marshal(msg.getRequestId(), requestIdHeader);
    GridClientProtobufMarshaller::marshal(msg.getClientId(), clientIdHeader);
    GridClientProtobufMarshaller::marshal(msg.getDestinationId(), destinationIdHeader);
}

void GridClientTcpPacket::setAdditionalHeaders(int8_t* pBuf) {
    int8_t* bufOffset = pBuf;
    requestIdHeader.assign(bufOffset, bufOffset + sizeof(int64_t));

    bufOffset += sizeof(int64_t);
    clientIdHeader.assign(bufOffset, bufOffset + UUID_SIZE);

    bufOffset += UUID_SIZE;
    destinationIdHeader.assign(bufOffset, bufOffset + UUID_SIZE);
}

void GridClientTcpPacket::setAdditionalHeadersAndData(int8_t* pBuf, size_t size) {
    setAdditionalHeaders(pBuf);

    setData(pBuf + ADDITIONAL_HEADERS_SIZE, pBuf + size);
    setPacketSize(ADDITIONAL_HEADERS_SIZE + data.size());
}

std::ostream& operator <<(std::ostream& stream, const GridClientTcpPacket& packet) {
    // Put the prefix for the packet.
    stream << GridClientTcpPacket::SIGNAL_CHAR;

    stream.write((const char*) packet.sizeHeader.data(), sizeof(int32_t));

    if (!packet.isPingPacket()) {
        stream.write((const char*) packet.requestIdHeader.data(), sizeof(int64_t));
        stream.write((const char*) packet.clientIdHeader.data(), packet.clientIdHeader.size());
        stream.write((const char*) packet.destinationIdHeader.data(),
                packet.destinationIdHeader.size());

        stream.write((const char*) packet.data.data(), packet.data.size());
    }

    return stream;
}

/**
 * Calculates packet size from packet data.
 *
 * @param header Packet header.
 * @return Packet size.
 */
int GridClientTcpConnection::getPacketSize(const int8_t* header) {
    if (*header != GridClientTcpPacket::SIGNAL_CHAR)
        return -1;

    int32_t packetSize = 0;

    GridClientByteUtils::bytesToValue(header + 1, sizeof(int32_t), packetSize);

    return packetSize;
}

/**
 * Connect to a host/port.
 *
 * @param pHost Host name.
 * @param pPort Port to connect to.
 */
void GridClientSyncTcpConnection::connect(const string& pHost, int pPort) {
    assert(!closed);

    host = pHost;
    port = pPort;

    asio::ip::tcp::resolver resolver(ioSrvc);
    asio::ip::tcp::resolver::query query(pHost, boost::lexical_cast<std::string>(pPort));

    try {
		asio::ip::tcp::resolver::iterator endpoint_iter = resolver.resolve(query);

		GG_LOG_DEBUG("Establishing connection [host=%s, port=%d]", pHost.c_str(), pPort);

        if (sslSock.get() == NULL)
            asio::connect(getSocket(), endpoint_iter);
        else {
            GG_LOG_DEBUG0("SSL connection started.");
            asio::connect(sslSock.get()->lowest_layer(), endpoint_iter);
            GG_LOG_DEBUG0("SSL connection established.");
            sslSock.get()->handshake(asio::ssl::stream_base::client);

            GG_LOG_DEBUG0("Handshake passed.");
        }
    }
    catch (system::system_error& e) {
        GG_LOG_DEBUG("Failed to connect: host is unreachable [host=%s, port=%d, errCode=%d]",
            pHost.c_str(), pPort, e.code().value());

        throw GridServerUnreachableException(
            fmtstring("Failed to connect (host is unreachable) [host=%s, port=%d, errorcode=%d]",
        	    pHost.c_str(), pPort, e.code().value()));
    }

    handshake();

    GG_LOG_INFO("Connection established [host=%s, port=%d, protocol=TCP]", pHost.c_str(), pPort);
}

/**
 * Authenticate client and store session token for later use.
 *
 * @param clientId Client ID to authenticate.
 * @param creds Credentials to use.
 */
void GridClientSyncTcpConnection::authenticate(const string& clientId, const string& creds) {
    ObjectWrapper protoMsg;

    GridAuthenticationRequestCommand authReq;
    GridClientMessageAuthenticationResult authResult;

    authReq.setClientId(clientId);
    authReq.credentials(creds);
    authReq.setRequestId(1);

    GridClientProtobufMarshaller::wrap(authReq, protoMsg);

    GridClientTcpPacket tcpPacket;
    GridClientTcpPacket tcpResponse;
    ProtoRequest req;

    //GridClientProtobufMarshaller::marshal(protoMsg, tcpPacket.data);
    tcpPacket.setData(protoMsg);
    tcpPacket.setAdditionalHeaders(authReq);

    send(tcpPacket, tcpResponse);

    //GridClientProtobufMarshaller::unmarshal(tcpResponse.data, respMsg);
    ObjectWrapper respMsg = tcpResponse.getData();

    GridClientProtobufMarshaller::unwrap(respMsg, authResult);

    sessToken = authResult.sessionToken();
}

/**
 * Returns session token associated with this connection.
 *
 * @return Session token or empty string if this is not a secure session.
 */
std::string GridClientSyncTcpConnection::sessionToken() {
    return sessToken;
}

/**
 * Sends a TCP packet over connection and receives a reply.
 *
 * @param gridTcpPacket Binary data to send.
 * @param result Binary response.
 */
void GridClientSyncTcpConnection::send(const GridClientTcpPacket& gridTcpPacket, GridClientTcpPacket& result) {
    int nBytes = gridTcpPacket.getDataSize();

    asio::streambuf request;
    ostream request_stream(&request);

    request_stream << gridTcpPacket;

    assert(request.size() == (size_t) nBytes + gridTcpPacket.getHeaderSize());

    system::error_code ec;

    {
        boost::lock_guard<boost::mutex> g(pingMux); // Protect from concurrent ping write.

        if (asio::write(getSocket(), request, ec) != nBytes + gridTcpPacket.getHeaderSize()) {
            GG_LOG_AND_THROW(GridClientConnectionResetException,
                    "Failed to send packet: connection was reset by the server [host=%s, port=%d, errorcode=%d]",
                    host.c_str(), port, ec.value());
        }
    }

    GG_LOG_DEBUG("Successfully sent a request [host=%s, port=%d, nbytes=%d]", host.c_str(), port, nBytes);

    int8_t headerBuf[GridClientTcpPacket::BASIC_HEADER_SIZE];

    int packetSize = 0;

    do {
        // Wait for the response from the server.
        nBytes = asio::read(getSocket(),
                asio::buffer(headerBuf, GridClientTcpPacket::BASIC_HEADER_SIZE),
                asio::transfer_exactly(GridClientTcpPacket::BASIC_HEADER_SIZE), ec);

        // Check error.
        if (ec) {
            GG_LOG_AND_THROW(GridClientConnectionResetException,
                    "Failed to read response header: connection was reset by the server [host=%s, port=%d, errorcode=%d]",
                    host.c_str(), port, ec.value());
        }

        // Get packet size with additional headers
        // (0 means we got ping packet and need to iterate again).
        packetSize = getPacketSize(headerBuf);

        if (packetSize == 0) {
            GG_LOG_DEBUG("Got ping response [host=%s, port=%d]", host.c_str(), port);
        }
    }
    while(packetSize == 0);

    GG_LOG_DEBUG("Done reading the response header [nbytes=%d]", nBytes);

    asio::streambuf responseBuf(packetSize);

    nBytes = asio::read(
            getSocket(),
            responseBuf,
            asio::transfer_exactly(packetSize),
            ec);

    if (ec) {
        GG_LOG_AND_THROW(GridClientConnectionResetException,
                "Failed to read response data: connection was reset by the server "
                "[host=%s, port=%d, errorcode=%d]",
                host.c_str(), port, ec.value());
    }

    GG_LOG_DEBUG("Done reading the response data [nbytes=%d]", nBytes);

    const unsigned char* charbuf = asio::buffer_cast<const unsigned char*>(responseBuf.data());

    //read headers from header buffer
    result.setAdditionalHeadersAndData((int8_t*)charbuf, responseBuf.size());
}

void GridClientSyncTcpConnection::sendPing() {
    GridClientTcpPacket pingPacket;
    GridClientTcpPacket::createPingPacket(pingPacket);

    asio::streambuf request;
    ostream request_stream(&request);

    request_stream << pingPacket;

    assert(request.size() == (size_t) pingPacket.getHeaderSize() + pingPacket.getDataSize());

    system::error_code ec;

    if (pingMux.try_lock()) {
        bool err = asio::write(getSocket(), request, ec) != pingPacket.getHeaderSize() + pingPacket.getDataSize();

        pingMux.unlock();

        if (err) {
            GG_LOG_AND_THROW(GridClientConnectionResetException,
                    "Failed to send ping packet: connection was reset by the server [host=%s, port=%d, errorcode=%d]",
                    host.c_str(), port, ec.value());
        }
    }
    else
        GG_LOG_DEBUG("Skipping ping as write is already being performed [host=%s, port=%d]", host.c_str(), port);

    GG_LOG_DEBUG("Successfully sent a ping packet [host=%s, port=%d]", host.c_str(), port);
}

void GridClientSyncTcpConnection::handshake() {
    asio::streambuf request;
    ostream request_stream(&request);

    request_stream.write((const char*)HANDSHAKE_PACKET, sizeof(HANDSHAKE_PACKET));

    system::error_code ec;
    size_t rc;

    if ((rc = asio::write(getSocket(), request, ec)) != sizeof(HANDSHAKE_PACKET))
        GG_LOG_AND_THROW(GridClientConnectionResetException,
                "Failed to send handshake packet: connection was reset by the server "
                "[host=%s, port=%d, rc=%d, errorcode=%d]",
                host.c_str(), port, rc, ec.value());

    GG_LOG_DEBUG("Successfully sent a handshake packet [host=%s, port=%d]", host.c_str(), port);

    boost::array<char, 1> buf;

    asio::read(
        getSocket(),
        asio::buffer(buf),
        asio::transfer_exactly(1),
        ec);

    if (ec)
        GG_LOG_AND_THROW(GridClientConnectionResetException,
                "Failed to read response data: connection was reset by the server "
                "[host=%s, port=%d, errorcode=%d]",
                host.c_str(), port, ec.value());

    switch (buf[0]) {
    case HandshakeResultCode::OK:
        GG_LOG_DEBUG("Received OK for handshake [host=%s, port=%d]", host.c_str(), port);

        break;

    case HandshakeResultCode::ERR_VERSION_CHECK_FAILED:
        GG_LOG_AND_THROW(GridClientException,
                "Handshake failed: bad version number (see server log for details) [host=%s, port=%d]",
                host.c_str(), port);

        break;

    case HandshakeResultCode::ERR_UNKNOWN_PROTO_ID:
        GG_LOG_AND_THROW(GridClientException,
                "Handshake failed: unknown/unsupported protocol ID (see server log for details) [host=%s, port=%d]",
                host.c_str(), port);

        break;

    default:
        GG_LOG_AND_THROW(GridClientException,
                "Handshake failed (see server log for details) [host=%s, port=%d]",
                host.c_str(), port);

        break;
    }
}

/**
 * Sends a TCP packet over connection and receives a reply.
 *
 * @param gridTcpPacket Binary data to send.
 * @param result Binary response.
 */
void GridClientAsyncTcpConnection::send(const GridClientTcpPacket& gridTcpPacket, GridClientTcpPacket& result) {
    int nBytes = gridTcpPacket.getDataSize();

    TGridAsyncIoDataPtr dataPtr(new GridAsyncIoData(result));
    dataPtr->isPingPacket = gridTcpPacket.isPingPacket();

    asio::streambuf request;
    ostream request_stream(&request);

    request_stream << gridTcpPacket;

    assert(request.size() == (size_t) nBytes + gridTcpPacket.getHeaderSize());

    pingMux.lock(); // Protect from concurrent ping write.

    boost::asio::async_write(
        getSocket(),
        request,
        boost::bind(
            &GridClientAsyncTcpConnection::handleAsyncWrite,
            this,
            dataPtr,
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred,
            nBytes + gridTcpPacket.getHeaderSize()));

    try {
        /* Runs the event loop for asynchronous operations.
         * Returns when all async operations for the current\
         * "chain" are complete. By chain we mean the above
         * asynch operation and all the operations that are
         * spawned by handlers.
         * If any of the handlers throws exception, this
         * exception gets thrown out of this run() call.
         *
         * After all async operations are complete, ioSrvc
         * gets into stopped state: all the further calls
         * to run() will return immediately and nothing will
         * be done. This is good if we already closed the
         * connection. Otherwise, we need to call ioSrvc.reset()
         * to turn it back to active state.
         *
         * Note: several threads may call ioSrvc.run(), but in
         * this case it doesn't work: overlapping reads and writes
         * on the same socket lead to undefined behaviour.
         */
        ioSrvc.run();
    }
    catch(...) {
        pingMux.unlock();

        ioSrvc.reset();

        throw;
    }

    if (closed)
        throw GridClientClosedException();

    ioSrvc.reset();
}

void GridClientAsyncTcpConnection::handleAsyncWrite(TGridAsyncIoDataPtr dataPtr,
        const boost::system::error_code& error,
        size_t nBytes, size_t nBytesExpected) {
    pingMux.unlock();

    if(error) {
        GG_LOG_AND_THROW(GridClientConnectionResetException,
                "Failed to send packet [host=%s, port=%d, errormsg=%s, errorcode=%d]",
                host.c_str(), port, error.message().c_str(), error.value());
    }

    if (nBytes != nBytesExpected) {
        GG_LOG_AND_THROW(GridClientConnectionResetException,
                "Failed to send packet: connection was reset by the server [host=%s, port=%d, errorcode=%d]",
                host.c_str(), port, error.value());
    }

    GG_LOG_DEBUG("Successfully sent a request [host=%s, port=%d, nbytes=%d]", host.c_str(), port, nBytes);

    boost::asio::async_read(
            getSocket(),
            asio::buffer(dataPtr->headerBuf, GridClientTcpPacket::BASIC_HEADER_SIZE),
            asio::transfer_exactly(GridClientTcpPacket::BASIC_HEADER_SIZE),
            boost::bind(
                &GridClientAsyncTcpConnection::handleAsyncReadHeader,
                this,
                dataPtr,
                boost::asio::placeholders::error,
                boost::asio::placeholders::bytes_transferred));
}

void GridClientAsyncTcpConnection::handleAsyncReadHeader(TGridAsyncIoDataPtr dataPtr,
        const boost::system::error_code& error, size_t nBytes) {
    if (error) {
        GG_LOG_AND_THROW(GridClientConnectionResetException,
                "Failed to read response header [host=%s, port=%d, errormsg=%s, errorcode=%d]",
                host.c_str(), port, error.message().c_str(), error.value());
    }


    int packetSize = getPacketSize(dataPtr->headerBuf);

    if (packetSize > 0) {
        GG_LOG_DEBUG("Done reading the response header [nbytes=%d]", nBytes);

        boost::asio::async_read(
                getSocket(),
                dataPtr->respBodyBuf,
                asio::transfer_exactly(packetSize),
                boost::bind(
                    &GridClientAsyncTcpConnection::handleAsyncReadBody,
                    this,
                    dataPtr,
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred));
    }
    else {
        GG_LOG_DEBUG("Got ping response [host=%s, port=%d]", host.c_str(), port);

        boost::asio::async_read(
                getSocket(),
                asio::buffer(dataPtr->headerBuf, GridClientTcpPacket::BASIC_HEADER_SIZE),
                asio::transfer_exactly(GridClientTcpPacket::BASIC_HEADER_SIZE),
                boost::bind(
                    &GridClientAsyncTcpConnection::handleAsyncReadHeader,
                    this,
                    dataPtr,
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred));
    }
}

void GridClientAsyncTcpConnection::handleAsyncReadBody(TGridAsyncIoDataPtr dataPtr,
        const boost::system::error_code& error, size_t nBytes) {
    if (error) {
        GG_LOG_AND_THROW(GridClientConnectionResetException,
                "Failed to read response data from [host=%s, port=%d, errormsg=%s, errorcode=%d]",
                host.c_str(), port, error.message().c_str(), error.value());
    }

    GG_LOG_DEBUG("Done reading the response data [nbytes=%d]", nBytes);

    const unsigned char* charbuf = asio::buffer_cast<const unsigned char*>(dataPtr->respBodyBuf.data());

    dataPtr->result.setAdditionalHeadersAndData((int8_t*)charbuf, dataPtr->respBodyBuf.size());
}
