/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include "gridgain/impl/utils/gridclientdebug.hpp"

#include <set>
#include <iostream>

#include <boost/bind.hpp>
#include <boost/asio.hpp>

#include "gridgain/impl/connection/gridclientconnectionpool.hpp"
#include "gridgain/impl/connection/gridclienttcpconnection.hpp"
#include "gridgain/impl/utils/gridutil.hpp"
#include "gridgain/gridclientexception.hpp"

#include "gridgain/impl/utils/gridclientlog.hpp"

using namespace std;

const int GridClientConnectionPool::TRACKER_INTERVAL_MS = 4000;

GridClientConnectionPool::GridClientConnectionPool(const GridClientConfiguration& config)
: config(config), protoCfg(config.protocolConfiguration()), connectionTracker(TRACKER_INTERVAL_MS, *this) {
    closed = false;
}

GridClientConnectionPool::~GridClientConnectionPool() {
    close();
}

std::shared_ptr<GridClientTcpConnection> GridClientConnectionPool::rentTcpConnection(const std::string& host,
        int port) {
    std::shared_ptr<GridClientTcpConnection> conn;

    {
        boost::lock_guard<boost::mutex> lock(mux_);

        if (closed)
            throw GridClientClosedException();

        conn = availableConnection<GridClientTcpConnection>(host, port);

        if (conn.get() != NULL) {
            availableConnections.erase(conn); //take away this connection

            return conn;
        }
    }

    // If no any available connection, we create and return a new one.
    GG_LOG_DEBUG("New TCP connection to %s:%d", host.c_str(), port);

    try {
        if (!protoCfg.sslEnabled())
            //TODO: GG-7677 move type of connection to client configuration ( sync, async, raw connection modes )
            conn.reset(new GridClientSyncTcpConnection());
        else {
            boost::shared_ptr<boost::asio::ssl::context> ssl = createSslContext();

            conn.reset(new GridClientAsyncTcpConnection(ssl));
        }

        conn->connect(host, port);

    }
    catch (GridServerUnreachableException&) {
        GG_LOG_DEBUG("Failed to establish connection to [%s:%d]", host.c_str(), port);

        throw;
    }

    try {
        if (!protoCfg.credentials().empty())
            conn->authenticate(protoCfg.uuid().uuid(), protoCfg.credentials());
    }
    catch (GridClientConnectionResetException& e) {
        GG_LOG_ERROR("Failed to authenticate the client with [%s:%d]: %s", host.c_str(), port, e.what());

        throw;
    }

    // Make pool aware of this connection.
    {
        boost::lock_guard<boost::mutex> guard(mux_);

        if (closed)
            throw GridClientClosedException();

        allConnections.insert(conn);
    }

    return conn;
}

void GridClientConnectionPool::turnBack(const TConnectionPtr& conn) {
    boost::lock_guard<boost::mutex> lock(mux_);

    // This should be a connection from this pool.
    assert(allConnections.count(conn) == 1 || (allConnections.empty() && closed));

    if (closed)
        throw GridClientClosedException();

    availableConnections.insert(conn);

    timestamps[conn] = GridUtil::currentTimeMillis();
}

void GridClientConnectionPool::steal(const TConnectionPtr& conn) {
    boost::lock_guard<boost::mutex> lock(mux_);

    if (closed)
        throw GridClientClosedException();

    assert(availableConnections.count(conn) == 0); // This connection should be given for rent.

    allConnections.erase(conn);
}

/**
 * Find an existing connection to a host.
 *
 * @returns Shared pointer to a connection.
 */
template<class T> std::shared_ptr<T> GridClientConnectionPool::availableConnection(const string& host, int port) {
    std::shared_ptr<T> res;

    for (auto it = availableConnections.begin(); it != availableConnections.end(); ++it) {
        if ((*it)->isConnected(host, port) && (dynamic_cast<T*>(it->get()) != NULL)) {
            GG_LOG_DEBUG("Get the connection from the pool [host=%s, port=%d]", host.c_str(), port);

            return reinterpret_cast<const std::shared_ptr<T>&>(*it);
        }
    }

    return res;
}

/**
 * Helper function needed by Boost SSL context creation.
 *
 * @return SSL store password.
 */
std::string GridClientConnectionPool::password() {
    return protoCfg.certificateFilePassword();
}

/**
 * Closes all connections in this pool and marks
 * this pool as closed.
 */
void GridClientConnectionPool::close() {
    boost::lock_guard<boost::mutex> guard(mux_);

    if (closed)
        return;

    closed = true;

    // Clear all connection lists (connections will be closed in destructors).
    availableConnections.clear();
    allConnections.clear();
    timestamps.clear();
}

/**
 * Creates SSL context based on configuration.
 *
 * @returns Shared pointer to Boost SSL context.
 */
boost::shared_ptr<boost::asio::ssl::context> GridClientConnectionPool::createSslContext() {
    boost::shared_ptr<boost::asio::ssl::context> ssl(
            new boost::asio::ssl::context(boost::asio::ssl::context::sslv23_client));

    ssl->set_options(boost::asio::ssl::context::default_workarounds);
    ssl->set_password_callback(boost::bind(&GridClientConnectionPool::password, this));
    ssl->set_verify_mode(boost::asio::ssl::context::verify_none);
    ssl->use_certificate_chain_file(GridUtil::prependHome(protoCfg.certificateFilePath()));
    ssl->use_private_key_file(GridUtil::prependHome(protoCfg.certificateFilePath()), boost::asio::ssl::context::pem);

    return ssl;
}

/**
 * Action, performed by connection tracker for
 * each connection.
 *
 * @param connection A connection to process.
 */
void GridClientConnectionPool::trackConnection(const TConnectionPtr& connection) {
    {
        boost::lock_guard<boost::mutex> g(mux_);

        bool available = availableConnections.count(connection) > 0;

        // Remove connection, if it is available and expired or closed.
        if (available) {
            if (!connection->isOpen()) {
                GG_LOG_DEBUG("Removing connection from pool [addr=%s, reason=closed]",
                        connection->toString().c_str());

                removeConnection(connection);

                return;
            }
            else if (GridUtil::currentTimeMillis() - timestamps[connection] > config.maxConnectionIdleTime()) {
                GG_LOG_DEBUG("Closing connection [addr=%s, reason=timeout]", connection->toString().c_str());

                removeConnection(connection);

                connection->close();

                return;
            }
        }
    }

    // Send ping.
    try {
        connection->sendPing();
    }
    catch (GridClientConnectionResetException&) {
        // Throw this connection away.
        boost::lock_guard<boost::mutex> g(mux_);

        removeConnection(connection);
    }
    catch (...) {
        GG_LOG_ERROR("Unknown exception is thrown when sending ping [connection=%s]",
                connection->toString().c_str());

        throw;
    }
}

void GridClientConnectionPool::onTimerEvent() {
    mux_.lock();

    TConnectionsSet localConnections = allConnections; //copy connections list

    mux_.unlock();

    // run trackConnection() for each connection
    std::for_each(
            localConnections.begin(),
            localConnections.end(),
            boost::bind(&GridClientConnectionPool::trackConnection, this, _1));
}

size_t GridClientConnectionPool::getAvailableConnectionsCount() const {
    boost::lock_guard<boost::mutex> g(mux_);

    return availableConnections.size();
}

size_t GridClientConnectionPool::getAllConnectionsCount() const {
    boost::lock_guard<boost::mutex> g(mux_);

    return allConnections.size();
}

void GridClientConnectionPool::removeConnection(const TConnectionPtr& connection) {
    availableConnections.erase(connection);
    allConnections.erase(connection);
    timestamps.erase(connection);
}
