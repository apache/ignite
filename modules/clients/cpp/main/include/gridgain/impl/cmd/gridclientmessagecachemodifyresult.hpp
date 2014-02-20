// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENTMESSAGE_CACHEMODIFY_RESULT_HPP_INCLUDED
#define GRID_CLIENTMESSAGE_CACHEMODIFY_RESULT_HPP_INCLUDED

#include "gridgain/impl/cmd/gridclientmessagecacheresult.hpp"

/**
 * Cache modify result message.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GridClientMessageCacheModifyResult : public GridClientMessageCacheResult {
public:
   /**
    * Set the result of a cache operation.
    *
    * @param pRslt - The result of the cache operation.
    */
    void setOperationResult(bool pRslt) { opRslt = pRslt; }

   /**
    * Get the result of a cache operation.
    *
    * @return The result of the cache operation.
    */
    bool getOperationResult() const { return opRslt; }
private:
    /** Operation result. */
    bool opRslt;
};

#endif
