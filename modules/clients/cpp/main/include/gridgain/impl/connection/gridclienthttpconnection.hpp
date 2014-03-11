/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_HTTP_CONNECTION_HPP_INCLUDED
#define GRID_CLIENT_HTTP_CONNECTION_HPP_INCLUDED

#include <string>
#include <map>

#include <boost/asio.hpp>
#include <boost/property_tree/ptree.hpp>

#include "gridgain/impl/connection/gridclientconnection.hpp"

/** HTTP request parameters structure. */
typedef std::map<std::string, std::string> TRequestParams;

/**
 * Client HTTP connection class.
 */
class GridClientHttpConnection: public GridClientConnection {
public:
    /**
     * Non-SSL HTTP connection constructor.
     *
     */
    GridClientHttpConnection() {}

    /**
     * SSL HTTP connection constructor.
     *
     * @param ctx Shared pointer to a Boost SSL context.
     */
    GridClientHttpConnection(boost::shared_ptr<boost::asio::ssl::context>& ctx)
            : GridClientConnection(ctx) {}

    /** Destructor */
    virtual ~GridClientHttpConnection() {}

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
    virtual void authenticate(std::string clientId, std::string creds);

    /**
     * Method that allows explicitly set session token for HTTP connection.
     *
     * @param sessTok New session token for the connection.
     */
    virtual void sessionToken(std::string sessTok) {
        sessToken = sessTok;
    }

    /**
     * Retrieves session token associated with the connection.
     *
     * @return Session token.
     */
    virtual std::string sessionToken() {
        return sessToken;
    }

    /**
     * Send a request to the host and receive reply.
     *
     * @param requestParams Key-value pairs.
     * @param jsonObj Boost property tree to be filled with response.
     */
    virtual void send(TRequestParams request, boost::property_tree::ptree& jsonObj);

    /**
     * Ping does not make sense for HTTP connection, so this method
     * does nothing.
     */
    virtual void sendPing() {}
};

#endif
