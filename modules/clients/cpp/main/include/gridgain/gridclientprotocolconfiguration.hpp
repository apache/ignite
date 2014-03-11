/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_PROTOCOL_CONFIGURATION_HPP_INCLUDED
#define GRID_CLIENT_PROTOCOL_CONFIGURATION_HPP_INCLUDED

#include <string>

#include <gridgain/gridconf.hpp>
#include <gridgain/gridclientuuid.hpp>
#include <gridgain/gridclientprotocol.hpp>

/**
 * Protocol configuration.
 */
class GRIDGAIN_API GridClientProtocolConfiguration {
public:
    /** Default client protocol. */
    static const GridClientProtocol DFLT_CLIENT_PROTOCOL = TCP;

    /** Default maximum time for a connection timeout. */
    static const long DFLT_MAX_CONN_TIMEOUT = 20000L;

    /** Default maximum time connection can be idle. */
    static const long DFLT_MAX_CONN_IDLE_TIME = 30000L;

    /** Default port for communication using binary TCP/IP protocol. */
    static const int DFLT_TCP_PORT = 11211;

    /** Default port for communication using HTTP. */
    static const int DFLT_HTTP_PORT = 8080;

    /** Default public constructor. */
    GridClientProtocolConfiguration();

    /**
     * Copy constructor.
     *
     * @param other Another instance of configuration.
     */
    GridClientProtocolConfiguration(const GridClientProtocolConfiguration& other);

    /**
     * Assignment operator override.
     *
     * @param rhs Right-hand side of the assignment operator.
     * @return This instance of the class.
     */
    GridClientProtocolConfiguration& operator=(const GridClientProtocolConfiguration& rhs);

    /** Destructor. */
    virtual ~GridClientProtocolConfiguration();

    /**
     * Gets protocol for communication between client and remote grid.
     *
     * @return Protocol for communication between client and remote grid.
     */
    GridClientProtocol protocol() const;

    /**
     * Sets protocol for communication between client and remote grid.
     *
     * @param p Protocol for communication between client and remote grid.
     */
    void protocol(GridClientProtocol p);

    /**
     * Gets timeout for socket connect operation.
     *
     * @return Socket connect timeout in milliseconds.
     */
    int connectTimeout() const;

    /**
     * Sets timeout for socket connect operation.
     *
     * @param t Socket connect timeout in milliseconds.
     */
    void connectTimeout(int t);

    /**
     * Flag indicating whether client should try to connect server with secure
     * socket layer enabled (regardless of protocol used).
     *
     * @return <tt>True</tt> if SSL should be enabled.
     */
    bool sslEnabled() const;

    /**
     * Flag indicating whether client should try to connect server with secure
     * socket layer enabled (regardless of protocol used).
     *
     * @param ssl New value for the flag.
     */
    void sslEnabled(bool ssl);

    /**
     * Path to file where SSL certificate is stored. If path is not absolute, it's considered
     * relative to <tt>GRIDGAIN_HOME</tt>.
     *
     * @return path to file.
     */
    std::string certificateFilePath() const;

    /**
     * Path to file where SSL certificate is stored. If path is not absolute, it's considered
     * relative to <tt>GRIDGAIN_HOME</tt>.
     *
     * @param cert New value.
     */
    void certificateFilePath(const std::string& cert);

    /**
     * Password for the certificate.
     *
     * @return Password.
     */
    std::string certificateFilePassword() const;

    /**
     * Password for the certificate.
     *
     * @param pass New password.
     */
    void certificateFilePassword(const std::string& pass);

    /**
     * Returns UUID used to authenticate
     */
    GridClientUuid uuid() const;

    /**
     * Gets client credentials to authenticate with.
     *
     * @return Credentials.
     */
    std::string credentials() const;

    /**
     * Sets client credentials to authenticate with.
     *
     * @param cred New value for the credentials.
     */
    void credentials(const std::string& cred);

    /**
     * Gets maximum amount of time that client connection can be idle before it is closed.
     *
     * @return Maximum idle time in milliseconds.
     */
    long maxConnectionIdleTime() const;

    /**
     * Sets maximum amount of time that client connection can be idle before it is closed.
     *
     * @param maxTime Maximum idle time in milliseconds.
     */
    void maxConnectionIdleTime(long maxTime);

private:
    class Impl;
    Impl* pimpl;
};

#endif
