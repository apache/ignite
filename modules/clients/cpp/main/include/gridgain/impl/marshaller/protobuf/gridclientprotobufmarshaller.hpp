// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLIENT_PROTOBUFF_MARSHALLER_HPP_INCLUDED
#define GRIDCLIENT_PROTOBUFF_MARSHALLER_HPP_INCLUDED

#include <vector>

#include "gridgain/impl/marshaller/protobuf/ClientMessages.pb.h"
#include "gridgain/gridclientnodemetricsbean.hpp"
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
#include "gridgain/impl/marshaller/protobuf/gridclientobjectwrapperconvertor.hpp"

using namespace org::gridgain::grid::kernal::processors::rest::client::message;

/**
 * Grid protobuf marshaler.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GridClientProtobufMarshaller {
public:
    /**
     * Sets protobuf request parameters based on data from authentication request.
     *
     * @param req Authentication request.
     * @param obj Protobuf object wrapper.
     */
    static void wrap(const GridAuthenticationRequestCommand& req, ObjectWrapper& obj);

    /**
     * Parses protobuf response into authentication result.
     *
     * @param obj Data received from server.
     * @param resp Authentication response.
     */
    static void unwrap(const ObjectWrapper& obj, GridClientMessageAuthenticationResult& resp);

    /**
     * Sets protobuf request parameters based on data from log request.
     *
     * @param req Authentication request.
     * @param obj Protobuf object wrapper.
     */
    static void wrap(const GridLogRequestCommand& req, ObjectWrapper& obj);

    /**
     * Parses protobuf response into log result.
     *
     * @param obj Data received from server.
     * @param resp Log response.
     */
    static void unwrap(const ObjectWrapper& obj, GridClientMessageLogResult& resp);

    /**
     * Sets protobuf request parameters based on data from topology request.
     *
     * @param req Topology request.
     * @param obj Protobuf object wrapper.
     */
    static void wrap(const GridTopologyRequestCommand& req, ObjectWrapper& obj);

    /**
     * Parses protobuf response into topology result.
     *
     * @param obj Data received from server.
     * @param resp Topology response.
     */
    static void unwrap(const ObjectWrapper& obj, GridClientMessageTopologyResult& resp);

    /**
     * Sets protobuf request parameters based on data from cache request.
     *
     * @param req Cache request.
     * @param obj Protobuf object wrapper.
     */
    static void wrap(const GridCacheRequestCommand& req, ObjectWrapper& obj);

    /**
     * Parses protobuf response into cache modification result.
     *
     * @param obj Data received from server.
     * @param resp Cache modification response.
     */
    static void unwrap(const ObjectWrapper& obj, GridClientMessageCacheModifyResult& resp);

    /**
     * Parses protobuf response into cache metrics result.
     *
     * @param obj Data received from server.
     * @param resp Cache metrics response.
     */
    static void unwrap(const ObjectWrapper& obj, GridClientMessageCacheMetricResult& resp);

    /**
     * Parses protobuf response into cache get result.
     *
     * @param obj Data received from server.
     * @param resp Cache get response.
     */
    static void unwrap(const ObjectWrapper& obj, GridClientMessageCacheGetResult&);

    /**
     * Sets protobuf request parameters based on data from task call request.
     *
     * @param req Task call request.
     * @param obj Protobuf object wrapper.
     */
    static void wrap(const GridTaskRequestCommand& req, ObjectWrapper& obj);

    /**
     * Parses protobuf response into task call result.
     *
     * @param obj Data received from server.
     * @param resp Task call response.
     */
    static void unwrap(const ObjectWrapper& obj, GridClientMessageTaskResult& resp);

    /**
     * General marshaling function for converting protobuf message to byte array.
     *
     * @param msg Protobuf message to marshal.
     * @param bytes Array to fill.
     */
    static void marshal(const ::google::protobuf::Message& msg, std::vector<int8_t>& bytes);

    /**
     * Marshals std::string to byte vector.
     *
     * @param str String to marshal.
     * @param bytes Vector to fill.
     */
    static void marshal(const std::string& str, std::vector<int8_t>& bytes);

    /**
     * Marshals 64-bit integer to byte vector.
     *
     * @param i64 64-bit integer to marshal.
     * @param bytes Vector to fill.
     */
    static void marshal(int64_t i64, std::vector<int8_t>& bytes);

    /**
     * Marshals UUID to byte vector.
     *
     * @param uuid An UUID to marshal.
     * @param bytes Vector to fill.
     */
    static void marshal(const GridUuid& uuid, std::vector<int8_t>& bytes);

    /**
     * General unmarshaling function for converting byte array to protobuf message.
     *
     * @param bytes Binary data.
     * @param msg Protobuf message to fill.
     */
    static void unmarshal(const std::vector<int8_t>& bytes, ::google::protobuf::Message& msg);

    static const std::string& msgType2Str(const ObjectWrapper& msg){
        return ObjectWrapperType_Name(msg.type());
    }

    /**
     * Marshals a protobuf message to an array of bytes. Note that memory is allocated using new[] inside the call and client code is responsible for calling delete[] on the buffer
     *
     * @param msg Message to marshal.
     * @param pBuffer - buffer to accept serialized message
     * @param bufferLength - Length of data packed into buffer. Note that client is responsible for calling delete[] on the buffer
     */
    static void marshalMsg(const ::google::protobuf::Message& msg, int8_t*& pBuffer, unsigned long & bufferLength);
};

/**
 * Utility class for inserting elements into Protobuf collection.
 *
 * Used in std::for_each().
 */
class ProtobufCollInserter {
public:
    /**
     * Construction.
     *
     * @param coll An output collection.
     */
    ProtobufCollInserter(::Collection& coll) :
        protoColl(coll) {}

    /**
     * Performs the insertion.
     *
     * @param val An element to insert.
     */
    void operator() (GridClientVariant val) {
        ObjectWrapper* item = protoColl.add_item();

        GridClientObjectWrapperConvertor::wrapSimpleType(val, *item);
    }
private:
    ::Collection& protoColl;
};

#endif
