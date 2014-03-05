/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLIENTMODEL_HPP_INCLUDED
#define GRIDCLIENTMODEL_HPP_INCLUDED

#include <string>

#include <gridgain/gridconf.hpp>

/**
 * Interface that will define the grid cache.
 */
class GRIDGAIN_API GridClientCache {
public:
    /** Cache modes. */
    enum GridClientCacheMode {
        /** Local cache. */
        LOCAL,

        /** Replicated cache. */
        REPLICATED,

        /** Partitioned cache. */
        PARTITIONED
    };

    virtual ~GridClientCache() {};

    /**
     * Retrieve the name of the cache.
     */
    virtual std::string name() const = 0;

     /**
     * Retrieve the mode of the cache.
     */
    virtual GridClientCacheMode mode() const = 0;
};

#endif // GRIDCLIENTMODEL_HPP_INCLUDED
