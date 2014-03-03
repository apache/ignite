/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENTMESSAGE_CACHEMETRICS_RESULT_HPP_INCLUDED
#define GRID_CLIENTMESSAGE_CACHEMETRICS_RESULT_HPP_INCLUDED

#include <map>
#include "gridgain/impl/cmd/gridclientmessagecacheresult.hpp"

/** Typedef for cache metrics map. */
typedef std::map<std::string, GridClientVariant> TCacheMetrics;

/**
 * Cache metrics result message.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GridClientMessageCacheMetricResult : public GridClientMessageCacheResult {
public:
   /**
    * Set the cache metrics values.
    *
    * @param pCacheMetrics The new value of cache-metrics.
    */
    void setCacheMetrics(const TCacheMetrics& pCacheMetrics) {
        cacheMetrics = pCacheMetrics;
    }

   /**
    * Get the cache values.
    *
    * @return  The key/value map of the cache metrics.
    */
    TCacheMetrics getCacheMetrics() const {
        return cacheMetrics;
    }
private:
    /** Cache metrics. */
    TCacheMetrics cacheMetrics;
};

#endif
