// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLIENT_JSON_MARSHALLER_HPP_INCLUDED
#define GRIDCLIENT_JSON_MARSHALLER_HPP_INCLUDED

#include <vector>
#include <boost/property_tree/json_parser.hpp>

#include "gridgain/impl/cmd/gridclientmessageauthrequestcommand.hpp"
#include "gridgain/impl/cmd/gridclientmessageauthresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagelogrequestcommand.hpp"
#include "gridgain/impl/cmd/gridclientmessagelogresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagetopologyrequestcommand.hpp"
#include "gridgain/impl/cmd/gridclientmessagetopologyresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagecacheresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagecacherequestcommand.hpp"
#include "gridgain/impl/cmd/gridclientmessagecachemodifyresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagecachemetricsresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagecachegetresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagetaskrequestcommand.hpp"
#include "gridgain/impl/cmd/gridclientmessagetaskresult.hpp"

#include "gridgain/gridclientexception.hpp"

/** Type definition for HTTP request key/value pairs. */
typedef std::map<std::string, std::string> TRequestParams;

/** Type definition for parsed JSON response. */
typedef boost::property_tree::ptree TJson;

/**
 * Grid JSON marshaler.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GridClientJsonMarshaller {
private:
    /**
     * Fills common request parameters based on common message data.
     *
     * @param req Message to take data from.
     * @param params Request key/value map to fill.
     */
    static void fillRequestHeader(const GridClientMessage& req, TRequestParams& params);

    /**
     * Fills common response message result data from JSON response.
     *
     * @param json Parsed HTTP output.
     * @param clientMsg Response to be returned to client.
     */
    static void fillResponseHeader(const TJson& json, GridClientMessageResult& clientMsg) throw (GridClientException);
public:
    /**
     * Sets HTTP request parameters based on data from authentication request.
     *
     * @param req Authentication request.
     * @param params Request key/value map.
     */
    static void wrap(const GridAuthenticationRequestCommand& req, TRequestParams& params);

    /**
     * Parses HTTP response into authentication result.
     *
     * @param json Data received from server.
     * @param resp Authentication response.
     */
    static void unwrap(const TJson& json, GridClientMessageAuthenticationResult& resp);

    /**
     * Sets HTTP request parameters based on data from log request.
     *
     * @param req Log request.
     * @param params Request key/value map.
     */
    static void wrap(const GridLogRequestCommand& req, TRequestParams& params);

    /**
     * Parses HTTP response into log result.
     *
     * @param json Data received from server.
     * @param resp Log response.
     */
    static void unwrap(const TJson& json, GridClientMessageLogResult& resp);

    /**
     * Sets HTTP request parameters based on data topology log request.
     *
     * @param req Topology request.
     * @param params Request key/value map.
     */
    static void wrap(const GridTopologyRequestCommand& req, TRequestParams& params);

    /**
     * Parses HTTP response into topology result.
     *
     * @param json Data received from server.
     * @param resp Topology response.
     */
    static void unwrap(const TJson& json, GridClientMessageTopologyResult& resp);

    /**
     * Sets HTTP request parameters based on data from cache request.
     *
     * @param req Cache request.
     * @param params Request key/value map.
     */
    static void wrap(const GridCacheRequestCommand& req, TRequestParams& params);

    /**
     * Parses HTTP response into cache modify result.
     *
     * @param json Data received from server.
     * @param resp Cache modify response.
     */
    static void unwrap(const TJson& json, GridClientMessageCacheModifyResult& resp);

    /**
     * Parses HTTP response into cache metrics result.
     *
     * @param json Data received from server.
     * @param resp Cache metrics response.
     */
    static void unwrap(const TJson& json, GridClientMessageCacheMetricResult& resp);

    /**
     * Parses HTTP response into cache get result.
     *
     * @param json Data received from server.
     * @param resp Cache get response.
     */
    static void unwrap(const TJson& json, GridClientMessageCacheGetResult& resp);

    /**
     * Sets HTTP request parameters based on data from task call request.
     *
     * @param req Topology request.
     * @param params Request key/value map.
     */
    static void wrap(const GridTaskRequestCommand& req, TRequestParams& params);

    /**
     * Parses HTTP response into task call result.
     *
     * @param json Data received from server.
     * @param resp Task call response.
     */
    static void unwrap(const TJson& json, GridClientMessageTaskResult& resp);
};

#endif
