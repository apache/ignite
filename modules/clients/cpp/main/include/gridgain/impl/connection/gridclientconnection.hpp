/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_CONNECTION_HPP_INCLUDED
#define GRID_CLIENT_CONNECTION_HPP_INCLUDED

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/thread.hpp>
#include <boost/lexical_cast.hpp>

#include "gridgain/gridclienttypedef.hpp"

/** Forward declaration of connection pool. */
class GridClientConnectionPool;

/**
 * Generic connection class. Current implementations are TCP and HTTP connections.
 */
class GridClientConnection: private boost::noncopyable {
public:
    /**
     * Public constructor.
     *
     * @param pPool Shared pointer to a connection pool.
     */
    GridClientConnection();

    /**
     * Public constructor.
     *
     * @param pPool Shared pointer to a connection pool.
     * @param ctx Boost SSL context.
     */
    GridClientConnection(boost::shared_ptr<boost::asio::ssl::context> ctx);

    /** Destructor. */
    virtual ~GridClientConnection();

    /**
     * Getter method for Boost IO service.
     *
     * @return Boost IO service currently used.
     */
    boost::asio::io_service& getIoService();

    /**
     * Getter method for Boost socket.
     *
     * @return Boost socket associated with this connection.
     */
    boost::asio::ip::tcp::socket& getSocket();

    /**
     * Getter method for Boost socket - constant version.
     *
     * @return Boost socket associated with this connection.
     */
    const boost::asio::ip::tcp::socket& getSocket() const;

    /** Connection close. */
    virtual void close();

    /**
     * Test the state of the connection.
     *
     * @return true if it is open, false otherwise.
     */
    virtual bool isOpen() const {
        if (closed) return false;

        boost::lock_guard<boost::mutex> guard(mux_);
        return getSocket().is_open();
    }

    /**
     * Test if this connection is really connected.
     *
     * @param pHost Host it should be connected to.
     * @param pPort Port it should be connected to.
     * @return true if it is connected, false otherwise.
     */
    virtual bool isConnected(const std::string& pHost, int pPort) {
        return isOpen() && (host == pHost) && (port == pPort);
    }

    /**
     * Sets a new value for the connection timeout associated with this connection.
     *
     * @param pConnTimeout New value for connection timeout.
     */
    void setConnectionTimeout(unsigned int pConnTimeout) {
        connTimeout = pConnTimeout;
    }

    /**
     * Sets a new value for the connection timeout associated with this connection.
     *
     * @param pConnTimeout New value for connection timeout.
     */
    void setReadTimeout(unsigned int pReadTimeout) {
        readTimeout = pReadTimeout;
    }

    /**
     * Sets a new value for the connection timeout associated with this connection.
     *
     * @param pConnTimeout New value for connection timeout.
     */
    void setWriteTimeout(unsigned int pWriteTimeout) {
        writeTimeout = pWriteTimeout;
    }

    /**
     * Sets a new value for the connection timeout associated with this connection.
     *
     * @param pConnTimeout New value for connection timeout.
     */
    void setSslContext(boost::shared_ptr<boost::asio::ssl::context> ctx) {
        assert(ctx.get() != NULL);

        sslCtx = ctx;
    }

    /**
     * Sends a ping packet to the peer.
     */
    virtual void sendPing() = 0;

    /**
     * Returns a string representation of this connection.
     */
    virtual std::string toString() const {
        return host + ":" + boost::lexical_cast<std::string>(port);
    }

protected:
    /**
     * Verifies certificate. Returns true to follow calling conventions. Throws exception in case of problems.
     *
     * @return true.
     */
    virtual bool verifyCertificate(bool preverified, boost::asio::ssl::verify_context& ctx);

    /**
     * Tests if this socket is SSL-based or plain.
     *
     * @return true if this is a SSL socket, false otherwise.
     */
    bool isSslMode() const {
        return sslSock.get() == NULL;
    }

    /**
     * Resets the socket state.
     */
    void resetSocket();

    /** Host associated with this connection. */
    std::string host;

    /** Port associated with this connection. */
    int port;

    /** Boost IO service. */
    boost::asio::io_service ioSrvc;

    /** Boost socket. */
    boost::shared_ptr<boost::asio::ip::tcp::socket> sock;

    /** Boost SSL socket. */
    boost::shared_ptr<boost::asio::ssl::stream<boost::asio::ip::tcp::socket> > sslSock;

    /** Boost SSL context. */
    boost::shared_ptr<boost::asio::ssl::context> sslCtx;

    /** Session token associated with this connection. */
    std::string sessToken;

    /** Connection timeout. */
    unsigned int connTimeout;

    /** Read timeout. */
    unsigned int readTimeout;

    /** Write timeout. */
    unsigned int writeTimeout;

    /** Synchronization for the underlying socket. */
    mutable boost::mutex mux_;

    /** Closed flag. */
    TGridAtomicBool closed;
};
#endif
