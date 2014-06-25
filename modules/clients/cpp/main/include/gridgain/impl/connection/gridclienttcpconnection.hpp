/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_TCP_CONNECTION_HPP_INCLUDED
#define GRID_CLIENT_TCP_CONNECTION_HPP_INCLUDED

#include <deque>
#include <vector>
#include <exception>
#include <boost/asio.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>

#include "gridgain/impl/connection/gridclientconnection.hpp"
#include "gridgain/impl/cmd/gridclientmessage.hpp"
#include "gridgain/gridclientexception.hpp"


/** Forward declaration. */
class GridClientConnectionPool;

/** Forward declaration. */
class GridClientSyncTcpConnection;

/** Forward declaration. */
class GridClientRawSyncTcpConnection;

/**
 * This class represents a data packet used in
 * GridGain binary protocol for data exchange.
 *
 * The packet has the following structure:
 *
 * -----------------------------
 * |  0x90  (1 byte)           | Special character to determine packet start.
 * |---------------------------|
 * |  size  (4 bytes)          | Size of the data that follows.
 * |---------------------------|
 * |  requestId (8 bytes)      | |
 * |---------------------------| |
 * |  clientId  (16 bytes)     |  > These are called additional headers.
 * |---------------------------| |
 * |  destinationId (16 bytes) | |
 * |---------------------------|
 * |           DATA            |
 * -----------------------------
 */

class GridClientTcpPacket {
private:
    /** Size of UUID field. */
    static const size_t UUID_SIZE = 16;

    /** Size of the additional headers. */
    static const size_t ADDITIONAL_HEADERS_SIZE = sizeof(int64_t) + UUID_SIZE * 2;

public:
    /** Packet header size for ping packet. */
    static const size_t BASIC_HEADER_SIZE = 5;

    /** Maximum packet header size. */
    static const size_t REGULAR_HEADER_SIZE = BASIC_HEADER_SIZE + ADDITIONAL_HEADERS_SIZE;

    /** First byte in binary protocol message. */
    static const int8_t SIGNAL_CHAR = (int8_t)0x90;

    /** Fills packet with ping data. */
    static void createPingPacket(GridClientTcpPacket& pingPacket);

    /**
     * Checks if this is a ping packet.
     *
     * @return true If this packet is a ping packet.
     */
    bool isPingPacket() const;

    /**
     * Returns header size of this packet.
     */
    size_t getHeaderSize() const;

    void setData(boost::shared_ptr<std::vector<int8_t>>& dataPtr);

    const boost::shared_ptr<std::vector<int8_t>>& getData() const;

    size_t getDataSize() const;

    void setAdditionalHeaders(const GridClientMessage& msg);

    void setAdditionalHeaders(int8_t* pBuf);

    void setAdditionalHeadersAndData(int8_t* pBuf, size_t size);

    friend std::ostream& operator <<(std::ostream& stream, const GridClientTcpPacket& packet);

    friend class GridClientSyncTcpConnection;

    friend class GridClientRawSyncTcpConnection;

private:
    void copyData(int8_t* start, int8_t* end);

    /**
     * Marshals 64-bit integer to byte vector.
     *
     * @param i64 64-bit integer to marshal.
     * @param bytes Vector to fill.
     */
    static void marshal(int64_t i64, std::vector<int8_t>& bytes);

    /**
     * Marshals UUID to byte vector.
     *
     * @param uuid An UUID to marshal.
     * @param bytes Vector to fill.
     */
    static void marshal(const GridClientUuid& uuid, std::vector<int8_t>& bytes);

    /**
     * Sets the size header of this packet.
     *
     * @param size Packet size.
     */
    void setPacketSize(int32_t size);

    /** Header with packet size. */
    std::vector<int8_t> sizeHeader;

    /** Request ID header. */
    std::vector<int8_t> requestIdHeader;

    /** Client ID header. */
    std::vector<int8_t> clientIdHeader;

    /** Destination ID header. */
    std::vector<int8_t> destinationIdHeader;

    /** Data transferred. */
    boost::shared_ptr<std::vector<int8_t>> dataPtr;
};

/**
 * TCP connection class used for sending data and storing session token associated with it in case of secure connection.
 */
class GridClientTcpConnection : public GridClientConnection {
public:
    /**
     * Non-SSL TCP connection constructor.
     */
    GridClientTcpConnection() {}

    /**
     * SSL TCP connection constructor.
     *
     * @param ctx Shared pointer to a Boost SSL context.
     */
    GridClientTcpConnection(boost::shared_ptr<boost::asio::ssl::context>& ctx)
        : GridClientConnection(ctx) {}

    /** Destructor. */
    virtual ~GridClientTcpConnection() {};

    /**
     * Connect to a host/port.
     *
     * @param pHost Host name.
     * @param pPort Port to connect to.
     */
    virtual void connect(const std::string& host, int port) = 0;

    /**
     * Authenticate client and store session token for later use.
     *
     * @param clientId Client ID to authenticate.
     * @param creds Credentials to use.
     */
    virtual void authenticate(const std::string& clientId, const std::string& creds) = 0;

    /**
     * Returns session token associated with this connection.
     *
     * @return Session token or empty string if this is not a secure session.
     */
    virtual std::vector<int8_t> sessionToken() = 0;

    /**
     * Sends a TCP packet over connection and receives a reply.
     *
     * @param gridTcpPacket Binary data to send.
     * @param result Binary response.
     */
    virtual void send(const GridClientTcpPacket& gridTcpPacket, GridClientTcpPacket& result) = 0;
protected:

    /**
     * Calculates packet size from packet data.
     *
     * @param header Packet header.
     * @return Packet size.
     */
    int getPacketSize(const int8_t* header);
};

/**
 * Synchronous implementation of TCP connection.
 */
class  GridClientSyncTcpConnection : public GridClientTcpConnection {
public:
    /**
     * Non-SSL TCP connection constructor.
     *
     */
    GridClientSyncTcpConnection() {}
    /**
     * SSL TCP connection constructor.
     *
     * @param ctx Shared pointer to a Boost SSL context.
     */
    GridClientSyncTcpConnection(boost::shared_ptr<boost::asio::ssl::context>& ctx)
        : GridClientTcpConnection(ctx) {}

    /**
     * Connect to a host/port.
     *
     * @param pHost Host name.
     * @param pPort Port to connect to.
     */
    virtual void connect(const std::string& host, int port);

    /**
     * Authenticate client and store session token for later use.
     *
     * @param clientId Client ID to authenticate.
     * @param creds Credentials to use.
     */
    virtual void authenticate(const std::string& clientId, const std::string& creds);

    /**
     * Returns session token associated with this connection.
     *
     * @return Session token or empty string if this is not a secure session.
     */
    virtual std::vector<int8_t> sessionToken();

    /**
     * Sends a TCP packet over connection and receives a reply.
     *
     * @param gridTcpPacket Binary data to send.
     * @param result Binary response.
     */
    virtual void send(const GridClientTcpPacket& gridTcpPacket, GridClientTcpPacket& result);

    /**
     * Sends a ping packet to the peer.
     */
    virtual void sendPing();

private:
    /**
     * Performs a handshake with server.
     */
    virtual void handshake();

protected:
    /**
     * Ping mutex, that protects from
     * concurrent ping write intersection.
     */
    boost::mutex pingMux;
};


/**
 * Synchronous raw implementation of TCP connection.
 */
class  GridClientRawSyncTcpConnection : public GridClientTcpConnection {
public:
    /**
     * Non-SSL TCP connection constructor.
     *
     */
    GridClientRawSyncTcpConnection();

    /**
     * Destructor.
     *
     */
    ~GridClientRawSyncTcpConnection();

    /**
     * Connect to a host/port.
     *
     * @param pHost Host name.
     * @param pPort Port to connect to.
     */
    virtual void connect(const std::string& host, int port);

    /**
     * Authenticate client and store session token for later use.
     *
     * @param clientId Client ID to authenticate.
     * @param creds Credentials to use.
     */
    virtual void authenticate(const std::string& clientId, const std::string& creds);

    /**
     * Returns session token associated with this connection.
     *
     * @return Session token or empty string if this is not a secure session.
     */
    virtual std::vector<int8_t> sessionToken();

    /**
     * Sends a TCP packet over connection and receives a reply.
     *
     * @param gridTcpPacket Binary data to send.
     * @param result Binary response.
     */
    virtual void send(const GridClientTcpPacket& gridTcpPacket, GridClientTcpPacket& result);

    /**
     * Sends a ping packet to the peer.
     */
    virtual void sendPing();

    /**
     * Closes connection.
     */
    virtual void close();


private:
    /**
     * Performs a handshake with server.
     */
    virtual void handshake();

    /**
     * OS socket.
     */
    int sock;

    /**
     * Address to connect to.
     */
    std::string address;

    /**
     * Port to connect to.
     */
    int port;

    /**
     * OS socket address info struct.
     */
    struct sockaddr_in server;
};


/**
 * Data for async IO operations, that is passed between
 * asynchronous handlers.
 */
struct GridAsyncIoData {
    /**
     * Constructor.
     */
    GridAsyncIoData(GridClientTcpPacket& resultRef): isPingPacket(false), result(resultRef) {}

    /** Ping packet flag. */
    bool isPingPacket;

    /** Header buffer. */
    int8_t headerBuf[GridClientTcpPacket::BASIC_HEADER_SIZE];

    /** Response body buffer. */
    boost::asio::streambuf respBodyBuf;

    /** Resulting TCP packet. */
    GridClientTcpPacket& result;
};

typedef std::shared_ptr<GridAsyncIoData> TGridAsyncIoDataPtr;

/**
 * An asynchronous implementation of GridClientTcpConnection.
 */
class GridClientAsyncTcpConnection: public GridClientSyncTcpConnection {
public:
    /**
     * Non-SSL TCP connection constructor.
     */
    GridClientAsyncTcpConnection() {}

    /**
     * SSL TCP connection constructor.
     *
     * @param ctx Shared pointer to a Boost SSL context.
     */
    GridClientAsyncTcpConnection(boost::shared_ptr<boost::asio::ssl::context>& ctx)
        : GridClientSyncTcpConnection(ctx) {}

    /**
     * Sends a TCP packet over connection and receives a reply.
     *
     * @param gridTcpPacket Binary data to send.
     * @param result Binary response.
     */
    virtual void send(const GridClientTcpPacket& gridTcpPacket, GridClientTcpPacket& result);

private:
    /**
     * Handler for async write.
     *
     * @param dataPtr Data.
     * @param error Error info.
     * @param bytesTransferred Number of bytes written.
     * @param nBytesExpected Expected number of bytes to write (should be equal to nBytes).
     */
    void handleAsyncWrite(TGridAsyncIoDataPtr dataPtr, const boost::system::error_code& error,
            size_t nBytes, size_t nBytesExpected);

    /**
     * Handler for async read of the message header.
     *
     * @param dataPtr Data.
     * @param error Error info.
     * @param bytesTransferred Number of bytes read.
     */
    void handleAsyncReadHeader(TGridAsyncIoDataPtr dataPtr, const boost::system::error_code& error,
            size_t nBytes);

    /**
     * Handler for async read of the message body.
     *
     * @param dataPtr Data.
     * @param error Error info.
     * @param bytesTransferred Number of bytes read.
     */
    void handleAsyncReadBody(TGridAsyncIoDataPtr dataPtr, const boost::system::error_code& error,
            size_t nBytes);
};

#endif
