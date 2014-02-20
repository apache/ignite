// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_SOCKET_ADDRESS_HPP_INCLUDED
#define GRID_SOCKET_ADDRESS_HPP_INCLUDED

#include <string>

/**
 * Grid host + port address holder.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GridSocketAddress {
public:
    /**
     * Public constructor.
     *
     * @param host Host address.
     * @param port Host port.
     */
    GridSocketAddress(const std::string& host, int port) : host_(host), port_(port) {}

    /**
     * Copy constructor.
     *
     * @param peer Address to copy data from.
     */
    GridSocketAddress(const GridSocketAddress& peer) : host_(peer.host_), port_(peer.port_) {}

    /**
     * Getter method for the host.
     *
     * @return Host set in the constructor.
     */
    std::string host() const { return host_; }

    /**
     * Getter method for the port.
     *
     * @return Port set in constructor.
     */
    int port() const { return port_; }

private:
    /** Host. */
    std::string host_;

    /** Port. */
    int port_;
};

#endif
