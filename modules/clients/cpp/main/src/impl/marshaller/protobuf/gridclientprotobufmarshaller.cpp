// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include <iostream>
#include <sstream>
#include <iterator>
#include <algorithm>
#include <map>

#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>

#include "gridgain/gridclientexception.hpp"
#include "gridgain/impl/marshaller/protobuf/gridclientprotobufmarshaller.hpp"
#include "gridgain/impl/marshaller/protobuf/gridclientobjectwrapperconvertor.hpp"
#include "gridgain/impl/marshaller/gridnodemarshallerhelper.hpp"
#include "gridgain/gridclientnode.hpp"

#include "gridgain/impl/utils/gridclientlog.hpp"
#include "gridgain/impl/utils/gridclientbyteutils.hpp"

using namespace org::gridgain::grid::kernal::processors::rest::client::message;

/** Forward declaration of the function. */
static void unwrap(const ObjectWrapper& obj, GridClientNode& node);

void GridClientProtobufMarshaller::marshalMsg(const ::google::protobuf::Message& msg, int8_t * & pBuffer, unsigned long & bufferLength) {
    bufferLength = msg.ByteSize();
    pBuffer = new int8_t[bufferLength];
    msg.SerializeToArray(pBuffer, bufferLength);
}

static void unmarshalMsg(int8_t * pBuffer, int size, ::google::protobuf::Message& msg) {
    bool parsedOk = msg.ParseFromArray(pBuffer, size);

    assert(parsedOk);
}

void GridClientProtobufMarshaller::marshal(const ::google::protobuf::Message& msg, std::vector<int8_t>& bytes) {

    int8_t * pBuffer;
    unsigned long bufferSize;

    marshalMsg(msg, pBuffer, bufferSize);

    bytes.assign(pBuffer, pBuffer + bufferSize);
    delete[] pBuffer;
}

void GridClientProtobufMarshaller::marshal(const std::string& str, std::vector<int8_t>& bytes) {
    bytes.assign(str.data(), str.data() + str.size());
}

void GridClientProtobufMarshaller::marshal(int64_t i64, std::vector<int8_t>& bytes) {

    bytes.resize(sizeof(i64));
    memset(&bytes[0],0,sizeof(i64));

    GridClientByteUtils::valueToBytes(i64, &bytes[0], sizeof(i64));
}

void GridClientProtobufMarshaller::marshal(const GridUuid& uuid, std::vector<int8_t>& bytes) {
    uuid.rawBytes(bytes);
}

void GridClientProtobufMarshaller::unmarshal(const std::vector<int8_t>& bytes, ::google::protobuf::Message& msg) {

    ::unmarshalMsg((int8_t*)bytes.data(), bytes.size(), msg);
}

/** Helper templates for unwrapping data from the ProtoBuf collections. */
template <class TD>
void unwrapCollection(const ::google::protobuf::RepeatedPtrField<ObjectWrapper>& src,
        std::vector<TD>& dst) {
    dst.clear();

    for (auto it = src.begin(); it != src.end(); ++it) {
        const ObjectWrapper& el = (*it);

        TD result;

        unwrap(el, result);

        dst.push_back(result);
    }
}

template <class TS, class TD> void unwrapCollection(const ::google::protobuf::RepeatedPtrField<TS> src,
        std::vector<TD>& dst) {
    std::back_insert_iterator< std::vector<TD> > backInsertIter(dst);

    dst.clear();

    std::copy(src.begin(), src.end(), backInsertIter);
}

template <class T> void unwrapCollection(const ObjectWrapper& objWrapper,
        std::vector<T>& res) {
    assert(objWrapper.type() == COLLECTION);
    assert(objWrapper.has_binary());

    res.clear();

    ::Collection coll;

    unmarshalMsg(objWrapper.binary().c_str(), objWrapper.binary().size() , coll);

    unwrapCollection(coll, res);
}

template <class K, class V, class T> class MapInserter {
public:
    MapInserter(std::map<K, V>& pMap) : map(pMap) {}

    void operator() (const T& obj) {
        std::pair<K,V> pair;

        transformValues(obj, pair);

        assert(map.find(pair.first) == map.end());

        map.insert(pair);
    }
private:
    std::map<K, V>& map;
};

class ProtobufMapInserter {
public:
    ProtobufMapInserter(::Map& pProtoMap) :
        protoMap(pProtoMap) {
    }

    void operator() (std::pair<GridClientVariant,GridClientVariant> pair) {
        KeyValue* keyValue = protoMap.add_entry();
        ObjectWrapper* key = keyValue->mutable_key();
        ObjectWrapper* value = keyValue->mutable_value();

        GridClientObjectWrapperConvertor::wrapSimpleType(pair.first, *key);
        GridClientObjectWrapperConvertor::wrapSimpleType(pair.second, *value);
    }

private:
    ::Map& protoMap;
};

template <class K, class V> void unwrapMap(::Map map, std::map<K,V>& res) {
    res.clear();

    const ::google::protobuf::RepeatedPtrField< ::KeyValue >& repFileds = map.entry();

    std::for_each(repFileds.begin(), repFileds.end(), MapInserter<K, V,
            ::KeyValue>(res));
}

template <class K, class V> void unwrapMap(ObjectWrapper objWrapper,
        std::map<K,V>& res) {
    assert(objWrapper.type() == MAP);
    assert(objWrapper.has_binary());

    res.clear();

    ::Map map;

    unmarshalMsg((int8_t*)objWrapper.binary().c_str(), objWrapper.binary().size(), map);

    const ::google::protobuf::RepeatedPtrField< ::KeyValue >& repFileds = map.entry();

    std::for_each(repFileds.begin(), repFileds.end(), MapInserter<K, V,
            ::KeyValue>(res));
}

/** Helper templates for unwrapping data from the ProtoBuf maps. */
static void transformValues (::KeyValue keyValue, std::pair<GridClientVariant,
        GridClientVariant>& pair) {
    assert(keyValue.has_key());
    assert(keyValue.has_value());

    GridClientObjectWrapperConvertor::unwrapSimpleType(keyValue.key(), pair.first);
    GridClientObjectWrapperConvertor::unwrapSimpleType(keyValue.value(), pair.second);
}

static void transformValues(::KeyValue keyValue, std::pair<std::string,
        GridClientVariant>& pair) {
    assert(keyValue.has_key());
    assert(keyValue.has_value());

    GridClientVariant key;

    GridClientObjectWrapperConvertor::unwrapSimpleType(keyValue.key(), key);
    GridClientObjectWrapperConvertor::unwrapSimpleType(keyValue.value(), pair.second);

    pair.first = key.toString();
}

static void fillRequestHeader(const GridClientMessage& clientMsg, ProtoRequest& protoReq) {
    //    protoReq.set_requestid(clientMsg.getRequestId());
    //    protoReq.set_clientid(clientMsg.getClientId());
    protoReq.set_sessiontoken(clientMsg.sessionToken());
}

static void fillResponseHeader(const ProtoResponse& resp, GridClientMessageResult& clientMsg) {
    //    clientMsg.setClientId(resp.clientid());
    //    clientMsg.setRequestId(resp.requestid());
    clientMsg.setStatus((GridClientMessageResult::StatusCode) resp.status());

    clientMsg.sessionToken(resp.sessiontoken());

    if (resp.has_errormessage())
        throw GridClientCommandException(resp.errormessage());
}

static void wrapRequest(const GridClientMessageCommand& cmd, const ObjectWrapperType& type,
        const ::google::protobuf::Message& src, ObjectWrapper& objWrapper) {

    GG_LOG_DEBUG("Wrapping request: %s", src.DebugString().c_str());

    ProtoRequest req;

    fillRequestHeader(cmd, req);

    objWrapper.set_type(type);

    int8_t * pBuffer;
    unsigned long bufferLength;

    GridClientProtobufMarshaller::marshalMsg(src, pBuffer, bufferLength);
    req.set_body(pBuffer, bufferLength);
    delete[] pBuffer;

    GridClientProtobufMarshaller::marshalMsg(req, pBuffer, bufferLength);
    objWrapper.set_binary(pBuffer, bufferLength);
    delete[] pBuffer;
}

void GridClientProtobufMarshaller::wrap(const GridTopologyRequestCommand& reqCmd, ObjectWrapper& objWrapper) {

    ProtoTopologyRequest topReq;

    topReq.set_includeattributes(reqCmd.getIncludeAttributes());

    topReq.set_includemetrics(reqCmd.getIncludeMetrics());

    if (! reqCmd.getNodeId().empty())
        topReq.set_nodeid(reqCmd.getNodeId());
    else if (! reqCmd.getNodeIp().empty())
        topReq.set_nodeip(reqCmd.getNodeIp());

    wrapRequest(reqCmd, TOPOLOGY_REQUEST, topReq, objWrapper);
}

void GridClientProtobufMarshaller::wrap(const GridLogRequestCommand& reqCmd, ObjectWrapper& objWrapper) {
    ProtoLogRequest logReq;

    logReq.set_from(reqCmd.from());
    logReq.set_to(reqCmd.to());
    logReq.set_path(reqCmd.path());

    wrapRequest(reqCmd, LOG_REQUEST, logReq, objWrapper);
}

void GridClientProtobufMarshaller::wrap(const GridAuthenticationRequestCommand& reqCmd, ObjectWrapper& objWrapper) {
    ProtoAuthenticationRequest authReq;

    ::ObjectWrapper* value = authReq.mutable_credentials();

    GridClientObjectWrapperConvertor::wrapSimpleType(reqCmd.credentials(), *value);

    wrapRequest(reqCmd, AUTH_REQUEST, authReq, objWrapper);
}

static void unwrapResponse(const ObjectWrapper& objWrapper, ProtoResponse& resp) {
    assert(objWrapper.type() == RESPONSE);
    assert(objWrapper.has_binary());

    unmarshalMsg((int8_t*)objWrapper.binary().c_str(),objWrapper.binary().size(), resp);
}

void GridClientProtobufMarshaller::unwrap(const ObjectWrapper& objWrapper, GridClientMessageTopologyResult& topRslt) {
    ProtoResponse resp;

    unwrapResponse(objWrapper, resp);

    fillResponseHeader(resp, topRslt);

    std::vector<GridClientNode> nodes;

    if (resp.has_resultbean()) {
        if (resp.resultbean().type() == NODE_BEAN) {
            GridClientNode nodeBean;

            ::unwrap(resp.resultbean(), nodeBean);

            nodes.push_back(nodeBean);
        }
        else {
            assert(resp.resultbean().type() == COLLECTION);

            std::string binary = resp.resultbean().binary();

            ::Collection coll;

            unmarshalMsg((int8_t*)binary.c_str(), binary.size(), coll);

            unwrapCollection(coll.item(), nodes);

            assert(nodes.size() == (size_t) coll.item().size());
        }
    }

    topRslt.setNodes(nodes);
}

void GridClientProtobufMarshaller::unwrap(const ObjectWrapper& objWrapper,
        GridClientMessageAuthenticationResult& authRslt) {
    ProtoResponse resp;

    GG_LOG_DEBUG("Unwrap: %s", objWrapper.DebugString().c_str());

    unwrapResponse(objWrapper, resp);

    GG_LOG_DEBUG("Unwrap result: %s", resp.DebugString().c_str());

    fillResponseHeader(resp, authRslt);
}

void GridClientProtobufMarshaller::unwrap(const ObjectWrapper& objWrapper, GridClientMessageLogResult& logRslt) {
    ProtoResponse resp;

    GG_LOG_DEBUG("Unwrap: %s", objWrapper.DebugString().c_str());

    unwrapResponse(objWrapper, resp);

    GG_LOG_DEBUG("Unwrap result: %s", resp.DebugString().c_str());

    fillResponseHeader(resp, logRslt);

    std::vector<std::string> lines;

    if (resp.has_resultbean()) {
        assert(resp.resultbean().type() == COLLECTION);

        const std::string& binary = resp.resultbean().binary();

        ::Collection coll;

        unmarshalMsg((int8_t*)binary.c_str(), binary.size(), coll);

        for (auto it = coll.item().begin(); it != coll.item().end(); ++it) {
            const ObjectWrapper& el = (*it);

            lines.push_back(el.binary());
        }
    }

    logRslt.lines(lines);
}

void GridClientProtobufMarshaller::wrap(const GridCacheRequestCommand& cmd, ObjectWrapper& objWrapper) {
    ProtoCacheRequest cacheReq;

    cacheReq.set_operation((::ProtoCacheRequest_GridCacheOperation)cmd.getOperation());

    if (cmd.getCacheName().size() > 0)
        cacheReq.set_cachename(cmd.getCacheName());
    else
        cacheReq.set_cachename((const char*)NULL, 0);

    if (cmd.getKey().hasAnyValue()) {
        ::ObjectWrapper* key = cacheReq.mutable_key();

        GridClientObjectWrapperConvertor::wrapSimpleType(cmd.getKey(), *key);
    }

    if (cmd.getValue().hasAnyValue()) {
        ::ObjectWrapper* value = cacheReq.mutable_value();

        GridClientObjectWrapperConvertor::wrapSimpleType(cmd.getValue(), *value);
    }

    if (cmd.getValue2().hasAnyValue()) {
        ::ObjectWrapper* value = cacheReq.mutable_value2();

        GridClientObjectWrapperConvertor::wrapSimpleType(cmd.getValue2(), *value);
    }

    if (! cmd.getValues().empty()) {
        ::Map* map = cacheReq.mutable_values();
        const GridCacheRequestCommand::TKeyValueMap& srcMap = cmd.getValues();

        std::for_each(srcMap.begin(), srcMap.end(), ProtobufMapInserter(*map));
    }

    std::set<GridClientCacheFlag> flags = cmd.getFlags();

    if (!flags.empty())
        cacheReq.set_cacheflagson(GridClientByteUtils::bitwiseOr(flags.begin(), flags.end(), 0));

    wrapRequest(cmd, CACHE_REQUEST, cacheReq, objWrapper);
}

void GridClientProtobufMarshaller::unwrap(const ObjectWrapper& objWrapper,
        GridClientMessageCacheModifyResult& msgRes) {
    ProtoResponse resp;

    unwrapResponse(objWrapper, resp);

    fillResponseHeader(resp, msgRes);

    if (resp.has_resultbean()) {
        const ObjectWrapper& boolWrapper = resp.resultbean();
        GridClientVariant var;

        GridClientObjectWrapperConvertor::unwrapSimpleType(boolWrapper, var);

        assert(var.hasBool());

        msgRes.setOperationResult(var.getBool());
    }
    else
        msgRes.setOperationResult(false);
}

void GridClientProtobufMarshaller::unwrap(const ObjectWrapper& objWrapper, GridClientMessageCacheMetricResult& msgRes){
    ProtoResponse resp;

    unwrapResponse(objWrapper, resp);

    fillResponseHeader(resp, msgRes);

    TCacheMetrics metrics;

    if (resp.has_resultbean()) {
        const ObjectWrapper& metricWrapper = resp.resultbean();

        assert(metricWrapper.type() == MAP);

        unwrapMap(metricWrapper, metrics);

        assert(metrics.size() > 0);
    }
    else
        assert(false);

    msgRes.setCacheMetrics(metrics);
}

void GridClientProtobufMarshaller::unwrap(const ObjectWrapper& objWrapper, GridClientMessageCacheGetResult& msgRes) {
    ProtoResponse resp;

    unwrapResponse(objWrapper, resp);

    TCacheValuesMap keyValues;

    fillResponseHeader(resp, msgRes);

    if (resp.has_resultbean()) {
        if (resp.resultbean().type() == MAP) {
            const ObjectWrapper& cacheValuesWrapper = resp.resultbean();

            unwrapMap(cacheValuesWrapper, keyValues);
        }
        else {
            GridClientVariant value;

            GridClientObjectWrapperConvertor::unwrapSimpleType(resp.resultbean(), value);

            keyValues.insert(std::make_pair(GridClientVariant(), value));
        }
    }

    msgRes.setCacheValues(keyValues);
}

void GridClientProtobufMarshaller::wrap(const GridTaskRequestCommand& cmd, ObjectWrapper& objWrapper) {
    ProtoTaskRequest taskReq;

    taskReq.set_taskname(cmd.getTaskName());

    GridClientVariant arg = cmd.getArg();

    ObjectWrapper* taskArg = taskReq.mutable_argument();

    GridClientObjectWrapperConvertor::wrapSimpleType(arg, *taskArg);

    wrapRequest(cmd, TASK_REQUEST, taskReq, objWrapper);
}

void GridClientProtobufMarshaller::unwrap(const ObjectWrapper& objWrapper, GridClientMessageTaskResult& msgRes) {
    ProtoResponse resp;

    unwrapResponse(objWrapper, resp);

    fillResponseHeader(resp, msgRes);

    GridClientVariant taskRslt;

    if (resp.has_resultbean()) {
        const ObjectWrapper& wrapper = resp.resultbean();

        GridClientObjectWrapperConvertor::unwrapSimpleType(wrapper, taskRslt);

        msgRes.setTaskResult(taskRslt);
    }

    msgRes.setTaskResult(taskRslt);
}

static void unwrap(const ObjectWrapper& objWrapper, GridClientNode& res) {
    assert(objWrapper.type() == NODE_BEAN);
    assert(objWrapper.has_binary());

    ::ProtoNodeBean bean;

    boost::asio::io_service ioSrvc;
    boost::asio::ip::tcp::resolver resolver(ioSrvc);

    unmarshalMsg((int8_t*)objWrapper.binary().c_str(),objWrapper.binary().size(), bean);

    GridNodeMarshallerHelper helper(res);

    helper.setNodeId(GridUuid::fromBytes(bean.nodeid()));

    GridClientVariant consistentId;

    GridClientObjectWrapperConvertor::unwrapSimpleType(bean.consistentid(), consistentId);

    helper.setConsistentId(consistentId);

    int tcpport = bean.tcpport();
    int jettyport = bean.jettyport();

    std::vector<GridSocketAddress> addresses;

    {
        for (int i = 0; i < bean.jettyaddress_size(); ++i)
        {
            if (bean.jettyaddress(i).size())
            {
                GridSocketAddress newJettyAddress = GridSocketAddress(bean.jettyaddress(i), jettyport);
                boost::asio::ip::tcp::resolver::query queryIp(bean.jettyaddress(i), boost::lexical_cast<std::string>(bean.jettyport()));
                boost::system::error_code ec;
                boost::asio::ip::tcp::resolver::iterator endpoint_iter = resolver.resolve(queryIp,ec);
                if (!ec)
                    addresses.push_back(newJettyAddress);
                else
                    GG_LOG_ERROR("Error resolving hostname: %s, %s",bean.jettyaddress(i).c_str(), ec.message().c_str());
            }
        }

        for (int i = 0; i < bean.jettyhostname_size(); ++i)
        {
            if (bean.jettyhostname(i).size())
            {
                GridSocketAddress newJettyAddress = GridSocketAddress(bean.jettyhostname(i), jettyport);
                boost::asio::ip::tcp::resolver::query queryHostname(bean.jettyhostname(i), boost::lexical_cast<std::string>(bean.jettyport()));
                boost::system::error_code ec;
                boost::asio::ip::tcp::resolver::iterator endpoint_iter = resolver.resolve(queryHostname,ec);
                if (!ec)
                    addresses.push_back(newJettyAddress);
                else
                    GG_LOG_ERROR("Error resolving hostname: %s, %s",bean.jettyhostname(i).c_str(), ec.message().c_str());
            }

        }

        helper.setJettyAddresses(addresses);
    }
    addresses.clear();
    {
        for (int i = 0; i < bean.tcpaddress_size(); ++i)
        {
            if (bean.tcpaddress(i).size())
            {
                GridSocketAddress newTCPAddress = GridSocketAddress(bean.tcpaddress(i), tcpport);
                boost::asio::ip::tcp::resolver::query queryIp(bean.tcpaddress(i), boost::lexical_cast<std::string>(bean.tcpport()));
                boost::system::error_code ec;
                boost::asio::ip::tcp::resolver::iterator endpoint_iter = resolver.resolve(queryIp,ec);
                if (!ec)
                    addresses.push_back(newTCPAddress);
                else
                    GG_LOG_ERROR("Error resolving hostname: %s, %s",bean.tcpaddress(i).c_str(), ec.message().c_str());
            }
        }
        for (int i = 0; i < bean.tcphostname_size(); ++i)
        {
            if (bean.tcphostname(i).size())
            {
                GridSocketAddress newTCPAddress = GridSocketAddress(bean.tcphostname(i), tcpport);
                boost::asio::ip::tcp::resolver::query queryHostname(bean.tcphostname(i), boost::lexical_cast<std::string>(bean.tcpport()));
                boost::system::error_code ec;
                boost::asio::ip::tcp::resolver::iterator endpoint_iter = resolver.resolve(queryHostname,ec);
                if (!ec)
                    addresses.push_back(newTCPAddress);
                else
                    GG_LOG_ERROR("Error resolving hostname: %s, %s",bean.tcphostname(i).c_str(), ec.message().c_str());
            }
        }

        helper.setTcpAddresses(addresses);
    }

    {
        std::map<GridClientVariant, GridClientVariant> cachesVars;

        unwrapMap(bean.caches(), cachesVars);

        helper.setCaches(cachesVars);
    }

    {
        std::map<GridClientVariant, GridClientVariant> attrs;

        unwrapMap(bean.attributes(), attrs);

        helper.setAttributes(attrs);
    }

    if (bean.has_metrics()) {
        const ::ProtoNodeMetricsBean& metrics = bean.metrics();
        GridClientNodeMetricsBean metricsBean;

        metricsBean.setStartTime(metrics.starttime());
        metricsBean.setAverageActiveJobs(metrics.averageactivejobs());
        metricsBean.setAverageCancelledJobs(metrics.averagecancelledjobs());
        metricsBean.setAverageCpuLoad(metrics.averagecpuload());
        metricsBean.setAverageJobExecuteTime(metrics.averagejobexecutetime());
        metricsBean.setAverageJobWaitTime(metrics.averagejobwaittime());
        metricsBean.setAverageRejectedJobs(metrics.averagerejectedjobs());
        metricsBean.setAverageWaitingJobs(metrics.averagewaitingjobs());
        metricsBean.setCurrentActiveJobs(metrics.currentactivejobs());
        metricsBean.setCurrentCancelledJobs(metrics.currentcancelledjobs());
        metricsBean.setCurrentCpuLoad(metrics.currentcpuload());
        metricsBean.setCurrentDaemonThreadCount(metrics.currentdaemonthreadcount());
        metricsBean.setCurrentIdleTime(metrics.currentidletime());
        metricsBean.setCurrentJobExecuteTime(metrics.currentjobexecutetime());
        metricsBean.setCurrentJobWaitTime(metrics.currentjobwaittime());
        metricsBean.setCurrentRejectedJobs(metrics.currentrejectedjobs());
        metricsBean.setCurrentThreadCount(metrics.currentthreadcount());
        metricsBean.setCurrentWaitingJobs(metrics.currentwaitingjobs());
        metricsBean.setFileSystemFreeSpace(metrics.filesystemfreespace());
        metricsBean.setFileSystemTotalSpace(metrics.filesystemtotalspace());
        metricsBean.setFileSystemUsableSpace(metrics.filesystemusablespace());
        metricsBean.setHeapMemoryCommitted(metrics.heapmemorycommitted());
        metricsBean.setHeapMemoryInitialized(metrics.heapmemoryinitialized());
        metricsBean.setHeapMemoryMaximum(metrics.heapmemorymaximum());
        metricsBean.setHeapMemoryUsed(metrics.heapmemoryused());
        metricsBean.setLastDataVersion(metrics.lastdataversion());
        metricsBean.setLastUpdateTime(metrics.lastupdatetime());
        metricsBean.setMaximumActiveJobs(metrics.maximumactivejobs());
        metricsBean.setMaximumCancelledJobs(metrics.maximumcancelledjobs());
        metricsBean.setMaximumJobExecuteTime(metrics.maximumjobexecutetime());
        metricsBean.setMaximumJobWaitTime(metrics.maximumjobwaittime());
        metricsBean.setMaximumRejectedJobs(metrics.maximumrejectedjobs());
        metricsBean.setMaximumThreadCount(metrics.maximumthreadcount());
        metricsBean.setMaximumWaitingJobs(metrics.maximumwaitingjobs());
        metricsBean.setNodeStartTime(metrics.nodestarttime());
        metricsBean.setNonHeapMemoryCommitted(metrics.nonheapmemorycommitted());
        metricsBean.setNonHeapMemoryInitialized(metrics.nonheapmemoryinitialized());
        metricsBean.setNonHeapMemoryMaximum(metrics.nonheapmemorymaximum());
        metricsBean.setNonHeapMemoryUsed(metrics.nonheapmemoryused());
        metricsBean.setTotalCancelledJobs(metrics.totalcancelledjobs());
        metricsBean.setTotalCpus(metrics.totalcpus());
        metricsBean.setTotalExecutedJobs(metrics.totalexecutedjobs());
        metricsBean.setTotalIdleTime(metrics.totalidletime());
        metricsBean.setTotalRejectedJobs(metrics.totalrejectedjobs());
        metricsBean.setTotalStartedThreadCount(metrics.totalstartedthreadcount());
        metricsBean.setUpTime(metrics.uptime());

        helper.setMetrics(metricsBean);
    }
}
