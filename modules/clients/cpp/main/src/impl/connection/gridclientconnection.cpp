/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include "gridgain/impl/utils/gridclientdebug.hpp"

#include "gridgain/impl/connection/gridclientconnectionpool.hpp"
#include "gridgain/impl/connection/gridclientconnection.hpp"
#include "gridgain/impl/utils/gridclientlog.hpp"

using namespace boost;

void GridClientConnection::resetSocket() {
    sock.reset(new asio::ip::tcp::socket(ioSrvc));

    if (sslCtx.get() != NULL) {
        sslSock.reset(new asio::ssl::stream<asio::ip::tcp::socket>(ioSrvc, *sslCtx.get()));
        sslSock->set_verify_mode(asio::ssl::verify_none);
        sslSock->set_verify_callback(bind(&GridClientConnection::verifyCertificate, this, _1, _2));
    }
}

GridClientConnection::GridClientConnection()
        : port(0), connTimeout(0), readTimeout(0), writeTimeout(0) {
    closed = false;

    resetSocket();
}

/**
 * Creates an SSL connection using connection pool, Boost IO service and SSL context.
 *
 * @param pPool Connection pool.
 * @param ctx Boost SSL context.
 */
GridClientConnection::GridClientConnection(boost::shared_ptr<asio::ssl::context> ctx)
        : sslCtx(ctx) {
    closed = false;

    assert(ctx.get() != NULL);

    resetSocket();
}

/**
 * Connection destructor.
 */
GridClientConnection::~GridClientConnection() {
    close();
}

/**
 * Returns an IO service for this connection.
 *
 * @return Boost IO service.
 */
asio::io_service& GridClientConnection::getIoService() {
    return ioSrvc;
}

/**
 * Returns an underlying socket for the connection - works both for SSL and non-SSL connections.
 *
 * @return Boost socket.
 */
asio::ip::tcp::socket& GridClientConnection::getSocket() {
    return sslSock.get() != NULL ? sslSock.get()->next_layer() : *sock;
}

/**
 * Returns an underlying socket for the connection - works both for SSL and non-SSL connections - const version.
 *
 * @return Unmodifiable Boost socket.
 */
const asio::ip::tcp::socket& GridClientConnection::getSocket() const {
    return sslSock.get() != NULL ? sslSock.get()->next_layer() : *sock;
}

/**
 * Closes the connection.
 */
void GridClientConnection::close() {
    { // we use a mutex here, because compare-and-exchange operation on atomic are platform-dependent
        boost::lock_guard<boost::mutex> g(mux_);

        if (closed)
            return;

        closed = true;
    }

    ioSrvc.stop();

    if (sslSock.get() == NULL) {
        // Waits till all the asynchronous operations will be completed.
        system::error_code ec;

        sock->remote_endpoint(ec);

        if (ec.value() == 0) {
            sock->shutdown(asio::ip::tcp::socket::shutdown_both);

            sock->close();
        }

        sock.reset(new asio::ip::tcp::socket(ioSrvc));
    }
    else {
        sslSock.get()->shutdown();

        sslSock.get()->lowest_layer().close();
    }

    GG_LOG_INFO("Connection closed: [host=%s, port=%d]", host.c_str(), port);
}

/**
 * Verifies certificate. Returns {@code true} to follow calling conventions. Throws exception in case of problems.
 *
 * @return {@code true}.
 */
bool GridClientConnection::verifyCertificate(bool preverified, asio::ssl::verify_context& ctx) {
    // The verify callback can be used to check whether the certificate that is
    // being presented is valid for the peer. For example, RFC 2818 describes
    // the steps involved in doing this for HTTPS. Consult the OpenSSL
    // documentation for more details. Note that the callback is called once
    // for each certificate in the certificate chain, starting from the root
    // certificate authority.

    char subject_name[256];

    X509* cert = X509_STORE_CTX_get_current_cert(ctx.native_handle());
    X509_NAME_oneline(X509_get_subject_name(cert), subject_name, 256);

    GG_LOG_DEBUG("Verifying subject: %s", subject_name);
    GG_LOG_DEBUG("Verify certificate, preverified: %s", preverified?"true":"false");

    return true;
}
