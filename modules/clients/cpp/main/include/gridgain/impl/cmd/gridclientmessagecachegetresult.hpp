/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENTMESSAGE_CACHEGET_RESULT_HPP_INCLUDED
#define GRID_CLIENTMESSAGE_CACHEGET_RESULT_HPP_INCLUDED

#include "gridgain/impl/cmd/gridclientmessagecacheresult.hpp"

/** Typedef for cache result key/value map. */
typedef boost::unordered_map<GridClientVariant, GridClientVariant> TCacheValuesMap;

/**
 * Cache get result message.
 */
class GridClientMessageCacheGetResult : public GridClientMessageCacheResult {
public:
    GridClientVariant res;
};
#endif
