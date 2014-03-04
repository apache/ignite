/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_FACTORY_HPP_INCLUDED
#define GRID_CLIENT_FACTORY_HPP_INCLUDED

#include <gridgain/gridconf.hpp>

class GridClientConfiguration;
class GridUuid;

/**
 * Factory implementation.
 */
class GRIDGAIN_API GridClientFactory {
public:
    /**
     * Starts a client with given configuration. The client will be assigned a randomly generated
     * UUID which can be obtained by {@link GridClient#id()} method.
     *
     * @param cfg Client configuration.
     * @return The client instance.
     * @throws GridClientException If client could not be created.
     */
     static TGridClientPtr start(const GridClientConfiguration& cfg);

    /**
     * Stops all currently open clients.
     *
     * @param wait If <tt>true</tt> then each client will wait to finish all ongoing requests before
     *      closing (however, no new requests will be accepted). If <tt>false</tt>, clients will be
     *      closed immediately and all ongoing requests will be failed.
     */
    static void stopAll(bool wait = true);

    /**
     * Stops particular client.
     *
     * @param clientId Client identifier to close.
     * @param wait If <tt>true</tt> then client will wait to finish all ongoing requests before
     *      closing (however, no new requests will be accepted). If <tt>false</tt>, client will be
     *      closed immediately and all ongoing requests will be failed.
     */
    static void stop(const GridUuid& clientId, bool wait = true);
};

#endif
