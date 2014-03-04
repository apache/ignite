/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_CONNECTION_POOL_HPP_INCLUDED
#define GRID_CLIENT_CONNECTION_POOL_HPP_INCLUDED

#include <set>
#include <memory>

#include <boost/asio/ssl.hpp>
#include <boost/thread.hpp>

#include "gridgain/gridclienttypedef.hpp"
#include "gridgain/gridclientprotocolconfiguration.hpp"
#include "gridgain/impl/gridclientrecurringeventthread.hpp"
#include "gridgain/gridclientconfiguration.hpp"
#include "gridgain/impl/connection/gridclienttcpconnection.hpp"
#include "gridgain/impl/connection/gridclienthttpconnection.hpp"

/** Typedef of connection shared pointer. */
typedef std::shared_ptr<GridClientConnection> TConnectionPtr;

/**
 * Connection pool that implements rent/turn-back logic.
 *
 * When a thread needs a connection, it takes it for rent from this pool.
 * If no such connection is available, it is created and returned.
 *
 * If connection is available, it is returned to the thread and is no more
 * available to other thread. The thread must then turn back this connection,
 * so that other threads can use it.
 *
 * Thus, several connections to the same endpoint may be created by
 * this pool, if several threads need to communicate with this endpoint
 * simultaneously.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GridClientConnectionPool:
    private boost::noncopyable,
    public GridClientRecurringEventThread::EventListener {
private:
    static const int TRACKER_INTERVAL_MS;

public:
    /** Constructor. */
    GridClientConnectionPool(const GridClientConfiguration& config);

    /** Destructor. */
    virtual ~GridClientConnectionPool();

    /**
     * Gives a TCP connection for rent.
     *
     * If no connection to the given host-port is currently available,
     * a new one is returned.
     *
     * After no longer needed, the connection should be turned back
     * using turnBack() method.
     *
     * @param host A host to connect to.
     * @param port A port to connect to.
     * @return A TCP connection in the connected state (isOpen() returns true).
     * @throws GridClientClosedException if this pool is closed;
     * GridServerUnreachableException if the specified endpoint (host-port) is not reachable;
     * GridClientConnectionResetException if the connection cannot be authenticated.
     */
    std::shared_ptr<GridClientTcpConnection> rentTcpConnection(const std::string& host, int port);

    /**
     * Gives an HTTP connection for rent.
     *
     * If no connection to the given host-port is currently available,
     * a new one is returned.
     *
     * After no longer needed, the connection should be turned back
     * using turnBack() method.
     *
     * @param host A host to connect to.
     * @param port A port to connect to.
     * @return An HTTP connection.
     */
    std::shared_ptr<GridClientHttpConnection> rentHttpConnection(const std::string& host, int port);

    /**
     * Turns a connection back.
     *
     * After turned back, this connection is available to other
     * threads, until one of the threads takes it for rent.
     *
     * @param conn A connection to turn back.
     */
    void turnBack(const TConnectionPtr& conn);

    /**
     * Removes this connecton from the list of registered connections.
     *
     * The pool won't be able to access the connection or give it for
     * rent after this method has run.
     *
     * @param conn A connection to steal. It should be taken for rent, otherwise
     * the assertion will fail.
     */
    void steal(const TConnectionPtr& conn);

    /**
     * Closes all connections in this pool and marks
     * this pool as closed.
     */
    void close();

    /**
     * Returns the number of connections available for rent.
     */
    size_t getAvailableConnectionsCount() const;

    /**
     * Returns the total number of connections, currently
     * managed by this pool.
     */
    size_t getAllConnectionsCount() const;

protected:
    /**
     * Finds an connection, available for rent.
     *
     * @param host A host for the connection.
     * @param port A port for the connection.
     * @returns A connection (either TCP or HTTP depending on template parameter), that is available for rent.
     */
    template<class T> std::shared_ptr<T> availableConnection(const std::string& host, int port);

    /**
     * Assigns an existing session token to HTTP connection or authenticates connection if there's no session
     * token yet.
     *
     * @param host Host for this connection.
     * @port port Port for this connection.
     * @param conn Shared pointer to HTTP connection.
     */
    void authenticateHttpConnection(const std::string& host, int port,
            std::shared_ptr<GridClientHttpConnection> conn);

    /**
     * Action, performed by connection tracker for
     * each connection.
     *
     * @param connection A connection to process.
     */
    void trackConnection(const TConnectionPtr& connection);

    /**
     * Callback function for connection tracker thread.
     */
    void onTimerEvent();

    /**
     * Utility method, that removes a connection from this pool.
     *
     * This method is NOT thread-safe.
     *
     * @param connection A connection to remove.
     */
    void removeConnection(const TConnectionPtr& connection);

private:
    /**
     * Helper function needed by Boost SSL context creation.
     *
     * @return SSL store password.
     */
    std::string password();

    /**
     * Session keys storage.
     */
    std::map<std::string, std::string> sessToks;

    /**
     * Creates SSL context based on configuration.
     *
     * @returns Shared pointer to Boost SSL context.
     */
    boost::shared_ptr<boost::asio::ssl::context> createSslContext();

    /** Client configuration. */
    GridClientConfiguration config;

    /** Protocol configuration. */
    GridClientProtocolConfiguration protoCfg;

    /** Storage type for exiting connections. */
    typedef std::set<TConnectionPtr> TConnectionsSet;

    /** Connections, available for rent. */
    TConnectionsSet availableConnections;

    /** Connections, registered in this pool. */
    TConnectionsSet allConnections;

    /** Synchronization for connection management. */
    mutable boost::mutex mux_;

    /** Closed flag. */
    bool closed;

    /** Tread that perform periodical actions on all connections. */
    GridClientRecurringEventThread connectionTracker;

    std::map<TConnectionPtr, double> timestamps;
};

#endif
