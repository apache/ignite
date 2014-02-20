// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_COMMMAND_EXECUTOR_HPP_INCLUDED
#define GRID_CLIENT_COMMMAND_EXECUTOR_HPP_INCLUDED

#include "gridgain/impl/connection/gridclientconnection.hpp"
#include "gridgain/impl/cmd/gridclientmessagetopologyrequestcommand.hpp"
#include "gridgain/impl/cmd/gridclientmessagetopologyresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagelogrequestcommand.hpp"
#include "gridgain/impl/cmd/gridclientmessagelogresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagecacherequestcommand.hpp"
#include "gridgain/impl/cmd/gridclientmessagecacheresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagecachemodifyresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagecachemetricsresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagecachegetresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagetaskrequestcommand.hpp"
#include "gridgain/impl/cmd/gridclientmessagetaskresult.hpp"
#include "gridgain/gridsocketaddress.hpp"

/**
 * Generic command executor class. Currently is implemented with TCP and HTTP executors.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GridClientCommandExecutor {
public:
    /** Virtual destructor. */
    virtual ~GridClientCommandExecutor() {}

    /**
     * Execute log command.
     *
     * @param nodeHost Host/port to send command to.
     * @param logRequest Log request command.
     * @param result Log request result.
     */
    virtual void executeLogCmd(const GridSocketAddress& nodeHost,
            GridLogRequestCommand& logRequest, GridClientMessageLogResult& result) = 0;

    /**
     * Execute topology command.
     *
     * @param nodeHost Host/port to send command to.
     * @param topologyRequest Topology request command.
     * @param result Topology request result.
     */
    virtual void executeTopologyCmd(const GridSocketAddress& nodeHost,
            GridTopologyRequestCommand& topologyRequest, GridClientMessageTopologyResult& result) = 0;

    /**
     * Execute cache get command.
     *
     * @param nodeHost Host/port to send command to.
     * @param cacheCmd Cache get request command.
     * @param result Cache get request result.
     */
    virtual void executeGetCacheCmd(const GridSocketAddress& nodeHost,
            GridCacheRequestCommand& cacheCmd, GridClientMessageCacheGetResult& result)  = 0;

    /**
     * Execute cache modify command.
     *
     * @param nodeHost Host/port to send command to.
     * @param cacheCmd Cache modify request command.
     * @param result Cache modify request result.
     */
    virtual void executeModifyCacheCmd(const GridSocketAddress& nodeHost,
            GridCacheRequestCommand& cacheCmd, GridClientMessageCacheModifyResult& result) = 0;

    /**
     * Execute cache metrics command.
     *
     * @param nodeHost Host/port to send command to.
     * @param cacheCmd Cache metrics request command.
     * @param result Cache metrics request result.
     */
    virtual void executeGetCacheMetricsCmd(const GridSocketAddress& nodeHost,
            GridCacheRequestCommand& cacheCmd, GridClientMessageCacheMetricResult& result) = 0;

    /**
     * Execute task command.
     *
     * @param nodeHost Host/port to send command to.
     * @param taskCmd task request command.
     * @param result task request result.
     */
    virtual void executeTaskCmd(const GridSocketAddress& nodeHost,
            GridTaskRequestCommand& taskCmd, GridClientMessageTaskResult& result) = 0;
};

#endif
