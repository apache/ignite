/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLIENT_PORTABLE_MESSAGES_HPP_INCLUDED
#define GRIDCLIENT_PORTABLE_MESSAGES_HPP_INCLUDED

#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>

#include "gridgain/gridclientexception.hpp"
#include "gridgain/gridclientnode.hpp"
#include "gridgain/gridportable.hpp"
#include "gridgain/gridportablereader.hpp"
#include "gridgain/gridportablewriter.hpp"
#include "gridgain/impl/cmd/gridclientmessagecacherequestcommand.hpp"
#include "gridgain/impl/marshaller/gridnodemarshallerhelper.hpp"
#include "gridgain/impl/queries/gridqueriesimpl.hpp"
#include "gridgain/impl/utils/gridclientbyteutils.hpp"
#include "gridgain/impl/utils/gridclientlog.hpp"

/** */
const GridClientVariant nullVariant;

/**
 * Base request class.
 */
class GridClientPortableMessage : public GridPortable {
public:
    /**
     * Default constructor.
     */
    GridClientPortableMessage() : sndTok(0) {
    }

    /**
     * @param writer Writer.
     */
    void writePortable(GridPortableWriter& writer) const {
        if (sndTok)
            writer.rawWriter().writeByteArray(sndTok->data(), sndTok->size());
        else
            writer.rawWriter().writeByteArray((int8_t*)0, 0);
    }

    /**
     * @param reader Reader.
     */
    void readPortable(GridPortableReader& reader) {
        reader.rawReader().readByteArray(rcvTok);
    }

    /** Token to send (pointer to avoid copies). */
    std::vector<int8_t>* sndTok;

    /** Received token. */
    std::vector<int8_t> rcvTok;
};

/**
 * Response.
 */
class GridClientResponse : public GridClientPortableMessage {
public:
    /** */
    static const int32_t TYPE_ID = 56;

    /**
     * @return Type ID.
     */
    int32_t typeId() const {
        return TYPE_ID;
    }

    /**
     * @param writer Writer.
     */
    void writePortable(GridPortableWriter& writer) const {
        GridClientPortableMessage::writePortable(writer);

        GridPortableRawWriter& raw = writer.rawWriter();

        raw.writeInt32(status);
        raw.writeString(errorMsg);
        raw.writeVariant(res);
    }

    /**
     * @param reader Reader.
     */
    void readPortable(GridPortableReader& reader) {
        GridClientPortableMessage::readPortable(reader);

        GridPortableRawReader& raw = reader.rawReader();

        status = raw.readInt32();

        boost::optional<std::string> msg = raw.readString();

        if (msg.is_initialized())
            errorMsg = std::move(msg.get());

        res = raw.readVariant();
    }

    /** */
    int32_t status;

    /** */
    std::string errorMsg;

    /** */
    GridClientVariant res;
};

/**
 * Metrics.
 */
class GridClientMetricsBean : public GridPortable {
public:
    /** */
    static const int32_t TYPE_ID = 58;

    /**
     * @return Type ID.
     */
    int32_t typeId() const {
        return TYPE_ID;
    }

    /**
     * @param writer Writer.
     */
    void writePortable(GridPortableWriter& writer) const {
        GridPortableRawWriter& raw = writer.rawWriter();

        raw.writeInt64(lastUpdateTime);
        raw.writeInt32(maxActiveJobs);
        raw.writeInt32(curActiveJobs);
        raw.writeFloat(avgActiveJobs);
        raw.writeInt32(maxWaitingJobs);
        raw.writeInt32(curWaitingJobs);
        raw.writeFloat(avgWaitingJobs);
        raw.writeInt32(maxRejectedJobs);
        raw.writeInt32(curRejectedJobs);
        raw.writeFloat(avgRejectedJobs);
        raw.writeInt32(maxCancelledJobs);
        raw.writeInt32(curCancelledJobs);
        raw.writeFloat(avgCancelledJobs);
        raw.writeInt32(totalRejectedJobs);
        raw.writeInt32(totalCancelledJobs);
        raw.writeInt32(totalExecutedJobs);
        raw.writeInt64(maxJobWaitTime);
        raw.writeInt64(curJobWaitTime);
        raw.writeDouble(avgJobWaitTime);
        raw.writeInt64(maxJobExecTime);
        raw.writeInt64(curJobExecTime);
        raw.writeDouble(avgJobExecTime);
        raw.writeInt32(totalExecTasks);
        raw.writeInt64(totalIdleTime);
        raw.writeInt64(curIdleTime);
        raw.writeInt32(availProcs);
        raw.writeDouble(load);
        raw.writeDouble(avgLoad);
        raw.writeDouble(gcLoad);
        raw.writeInt64(heapInit);
        raw.writeInt64(heapUsed);
        raw.writeInt64(heapCommitted);
        raw.writeInt64(heapMax);
        raw.writeInt64(nonHeapInit);
        raw.writeInt64(nonHeapUsed);
        raw.writeInt64(nonHeapCommitted);
        raw.writeInt64(nonHeapMax);
        raw.writeInt64(upTime);
        raw.writeInt64(startTime);
        raw.writeInt64(nodeStartTime);
        raw.writeInt32(threadCnt);
        raw.writeInt32(peakThreadCnt);
        raw.writeInt64(startedThreadCnt);
        raw.writeInt32(daemonThreadCnt);
        raw.writeInt64(fileSysFreeSpace);
        raw.writeInt64(fileSysTotalSpace);
        raw.writeInt64(fileSysUsableSpace);
        raw.writeInt64(lastDataVer);
        raw.writeInt32(sentMsgsCnt);
        raw.writeInt64(sentBytesCnt);
        raw.writeInt32(rcvdMsgsCnt);
        raw.writeInt64(rcvdBytesCnt);
    }

    /**
     * @param reader Reader.
     */
    void readPortable(GridPortableReader& reader) {
        GridPortableRawReader& raw = reader.rawReader();

        lastUpdateTime = raw.readInt64();
        maxActiveJobs = raw.readInt32();
        curActiveJobs = raw.readInt32();
        avgActiveJobs = raw.readFloat();
        maxWaitingJobs = raw.readInt32();
        curWaitingJobs = raw.readInt32();
        avgWaitingJobs = raw.readFloat();
        maxRejectedJobs = raw.readInt32();
        curRejectedJobs = raw.readInt32();
        avgRejectedJobs = raw.readFloat();
        maxCancelledJobs = raw.readInt32();
        curCancelledJobs = raw.readInt32();
        avgCancelledJobs = raw.readFloat();
        totalRejectedJobs = raw.readInt32();
        totalCancelledJobs = raw.readInt32();
        totalExecutedJobs = raw.readInt32();
        maxJobWaitTime = raw.readInt64();
        curJobWaitTime = raw.readInt64();
        avgJobWaitTime = raw.readDouble();
        maxJobExecTime = raw.readInt64();
        curJobExecTime = raw.readInt64();
        avgJobExecTime = raw.readDouble();
        totalExecTasks = raw.readInt32();
        totalIdleTime = raw.readInt64();
        curIdleTime = raw.readInt64();
        availProcs = raw.readInt32();
        load = raw.readDouble();
        avgLoad = raw.readDouble();
        gcLoad = raw.readDouble();
        heapInit = raw.readInt64();
        heapUsed = raw.readInt64();
        heapCommitted = raw.readInt64();
        heapMax = raw.readInt64();
        nonHeapInit = raw.readInt64();
        nonHeapUsed = raw.readInt64();
        nonHeapCommitted = raw.readInt64();
        nonHeapMax = raw.readInt64();
        upTime = raw.readInt64();
        startTime = raw.readInt64();
        nodeStartTime = raw.readInt64();
        threadCnt = raw.readInt32();
        peakThreadCnt = raw.readInt32();
        startedThreadCnt = raw.readInt64();
        daemonThreadCnt = raw.readInt32();
        fileSysFreeSpace = raw.readInt64();
        fileSysTotalSpace = raw.readInt64();
        fileSysUsableSpace = raw.readInt64();
        lastDataVer = raw.readInt64();
        sentMsgsCnt = raw.readInt32();
        sentBytesCnt = raw.readInt64();
        rcvdMsgsCnt = raw.readInt32();
        rcvdBytesCnt = raw.readInt64();
    }

    /** */
    int64_t lastUpdateTime;

    /** */
    int32_t maxActiveJobs;

    /** */
    int32_t curActiveJobs;

    /** */
    float avgActiveJobs;

    /** */
    int32_t maxWaitingJobs;

    /** */
    int32_t curWaitingJobs;

    /** */
    float avgWaitingJobs;

    /** */
    int32_t maxRejectedJobs;

    /** */
    int32_t curRejectedJobs;

    /** */
    float avgRejectedJobs;

    /** */
    int32_t maxCancelledJobs;

    /** */
    int32_t curCancelledJobs;

    /** */
    float avgCancelledJobs;

    /** */
    int32_t totalRejectedJobs;

    /** */
    int32_t totalCancelledJobs;

    /** */
    int32_t totalExecutedJobs;

    /** */
    int64_t maxJobWaitTime;

    /** */
    int64_t curJobWaitTime;

    /** */
    double avgJobWaitTime;

    /** */
    int64_t maxJobExecTime;

    /** */
    int64_t curJobExecTime;

    /** */
    double avgJobExecTime;

    /** */
    int32_t totalExecTasks;

    /** */
    int64_t totalIdleTime;

    /** */
    int64_t curIdleTime;

    /** */
    int32_t availProcs;

    /** */
    double load;

    /** */
    double avgLoad;

    /** */
    double gcLoad;

    /** */
    int64_t heapInit;

    /** */
    int64_t heapUsed;

    /** */
    int64_t heapCommitted;

    /** */
    int64_t heapMax;

    /** */
    int64_t nonHeapInit;

    /** */
    int64_t nonHeapUsed;

    /** */
    int64_t nonHeapCommitted;

    /** */
    int64_t nonHeapMax;

    /** */
    int64_t upTime;

    /** */
    int64_t startTime;

    /** */
    int64_t nodeStartTime;

    /** */
    int32_t threadCnt;

    /** */
    int32_t peakThreadCnt;

    /** */
    int64_t startedThreadCnt;

    /** */
    int32_t daemonThreadCnt;

    /** */
    int64_t fileSysFreeSpace;

    /** */
    int64_t fileSysTotalSpace;

    /** */
    int64_t fileSysUsableSpace;

    /** */
    int64_t lastDataVer;

    /** */
    int32_t sentMsgsCnt;

    /** */
    int64_t sentBytesCnt;

    /** */
    int32_t rcvdMsgsCnt;

    /** */
    int64_t rcvdBytesCnt;
};

/**
 * Node.
 */
class GridClientNodeBean : public GridPortable {
public:
    /** */
    static const int32_t TYPE_ID = 57;

    /**
     * @return Type ID.
     */
    int32_t typeId() const {
        return TYPE_ID;
    }

    /**
     * @param writer Writer.
     */
    void writePortable(GridPortableWriter& writer) const {
        GridPortableRawWriter& raw = writer.rawWriter();

        raw.writeInt32(tcpPort);
        raw.writeInt32(replicaCnt);

        raw.writeString(dfltCacheMode);

        raw.writeVariantMap(attrs);
        raw.writeVariantMap(caches);

        raw.writeVariantCollection(tcpAddrs);
        raw.writeVariantCollection(tcpHostNames);

        raw.writeUuid(nodeId);

        raw.writeVariant(consistentId);
        raw.writeVariant(metrics);
    }

    /**
     * @param reader Reader.
     */
    void readPortable(GridPortableReader& reader) {
        GridPortableRawReader& raw = reader.rawReader();

        tcpPort = raw.readInt32();
        replicaCnt = raw.readInt32();

        boost::optional<std::string> optDfltCacheMode = raw.readString();
        if (optDfltCacheMode.is_initialized())
            dfltCacheMode = optDfltCacheMode.get();

        raw.readVariantMap(attrs);

        raw.readVariantMap(caches);

        raw.readVariantCollection(tcpAddrs);

        raw.readVariantCollection(tcpHostNames);

        nodeId = raw.readUuid();

        consistentId = raw.readVariant();
        metrics = raw.readVariant();
    }

    /**
     * @return Node object.
     */
    GridClientNode createNode() {
        GridClientNode res;

        GridClientNodeMarshallerHelper helper(res);

        if (!nodeId)
            throw GridClientCommandException("Failed to read node ID");

        helper.setNodeId(nodeId.get());

        helper.setConsistentId(consistentId);

        int tcpport = tcpPort;

        std::vector<GridClientSocketAddress> addresses;

        boost::asio::io_service ioSrvc;
        boost::asio::ip::tcp::resolver resolver(ioSrvc);

        for (size_t i = 0; i < tcpAddrs.size(); ++i) {
            GridClientVariant& tcpAddr = tcpAddrs[i];

            GridClientSocketAddress newTCPAddress = GridClientSocketAddress(tcpAddr.getString(), tcpport);

            boost::asio::ip::tcp::resolver::query queryIp(tcpAddr.getString(), boost::lexical_cast<std::string>(tcpport));

            boost::system::error_code ec;

            boost::asio::ip::tcp::resolver::iterator endpoint_iter = resolver.resolve(queryIp, ec);

            if (!ec)
                addresses.push_back(newTCPAddress);
            else
                GG_LOG_ERROR("Error resolving hostname: %s, %s", tcpAddr.getString().c_str(), ec.message().c_str());
        }

        for (size_t i = 0; i < tcpHostNames.size(); ++i) {
            GridClientVariant& tcpHost = tcpHostNames[i];

            GridClientSocketAddress newTCPAddress = GridClientSocketAddress(tcpHost.getString(), tcpport);

            boost::asio::ip::tcp::resolver::query queryHostname(tcpHost.getString(), boost::lexical_cast<std::string>(tcpport));

            boost::system::error_code ec;

            boost::asio::ip::tcp::resolver::iterator endpoint_iter = resolver.resolve(queryHostname, ec);

            if (!ec)
                addresses.push_back(newTCPAddress);
            else
                GG_LOG_ERROR("Error resolving hostname: %s, %s", tcpHost.getString().c_str(), ec.message().c_str());
        }

        helper.setTcpAddresses(addresses);

        helper.setDefaultCacheMode(dfltCacheMode);

        if (!dfltCacheMode.empty())
            caches[GridClientVariant()] = GridClientVariant(dfltCacheMode);

        helper.setCaches(caches);

        helper.setAttributes(attrs);

        if (metrics.hasPortableObject() || metrics.hasPortable()) {
            std::unique_ptr<GridClientMetricsBean> pMetrics(metrics.deserializePortable<GridClientMetricsBean>());

            GridClientNodeMetricsBean metricsBean;

            metricsBean.setStartTime(pMetrics->startTime);
            metricsBean.setAverageActiveJobs(pMetrics->avgActiveJobs);
            metricsBean.setAverageCancelledJobs(pMetrics->avgCancelledJobs);
            metricsBean.setAverageCpuLoad(pMetrics->avgLoad);
            metricsBean.setAverageJobExecuteTime(pMetrics->avgJobExecTime);
            metricsBean.setAverageJobWaitTime(pMetrics->avgJobWaitTime);
            metricsBean.setAverageRejectedJobs(pMetrics->avgRejectedJobs);
            metricsBean.setAverageWaitingJobs(pMetrics->avgWaitingJobs);
            metricsBean.setCurrentActiveJobs(pMetrics->curActiveJobs);
            metricsBean.setCurrentCancelledJobs(pMetrics->curCancelledJobs);
            metricsBean.setCurrentCpuLoad(pMetrics->load);
            metricsBean.setCurrentDaemonThreadCount(pMetrics->daemonThreadCnt);
            metricsBean.setCurrentIdleTime(pMetrics->curIdleTime);
            metricsBean.setCurrentJobExecuteTime(pMetrics->curJobExecTime);
            metricsBean.setCurrentJobWaitTime(pMetrics->curJobWaitTime);
            metricsBean.setCurrentRejectedJobs(pMetrics->curRejectedJobs);
            metricsBean.setCurrentThreadCount(pMetrics->threadCnt);
            metricsBean.setCurrentWaitingJobs(pMetrics->curWaitingJobs);
            metricsBean.setFileSystemFreeSpace(pMetrics->fileSysFreeSpace);
            metricsBean.setFileSystemTotalSpace(pMetrics->fileSysTotalSpace);
            metricsBean.setFileSystemUsableSpace(pMetrics->fileSysUsableSpace);
            metricsBean.setHeapMemoryCommitted(pMetrics->heapCommitted);
            metricsBean.setHeapMemoryInitialized(pMetrics->heapInit);
            metricsBean.setHeapMemoryMaximum(pMetrics->heapMax);
            metricsBean.setHeapMemoryUsed(pMetrics->heapUsed);
            metricsBean.setLastDataVersion(pMetrics->lastDataVer);
            metricsBean.setLastUpdateTime(pMetrics->lastUpdateTime);
            metricsBean.setMaximumActiveJobs(pMetrics->maxActiveJobs);
            metricsBean.setMaximumCancelledJobs(pMetrics->maxCancelledJobs);
            metricsBean.setMaximumJobExecuteTime(pMetrics->maxJobExecTime);
            metricsBean.setMaximumJobWaitTime(pMetrics->maxJobWaitTime);
            metricsBean.setMaximumRejectedJobs(pMetrics->maxRejectedJobs);
            metricsBean.setMaximumThreadCount(pMetrics->peakThreadCnt);
            metricsBean.setMaximumWaitingJobs(pMetrics->maxWaitingJobs);
            metricsBean.setNodeStartTime(pMetrics->nodeStartTime);
            metricsBean.setNonHeapMemoryCommitted(pMetrics->nonHeapCommitted);
            metricsBean.setNonHeapMemoryInitialized(pMetrics->nonHeapInit);
            metricsBean.setNonHeapMemoryMaximum(pMetrics->nonHeapMax);
            metricsBean.setNonHeapMemoryUsed(pMetrics->nonHeapUsed);
            metricsBean.setTotalCancelledJobs(pMetrics->totalCancelledJobs);
            metricsBean.setTotalCpus(pMetrics->availProcs);
            metricsBean.setTotalExecutedJobs(pMetrics->totalExecutedJobs);
            metricsBean.setTotalIdleTime(pMetrics->totalIdleTime);
            metricsBean.setTotalRejectedJobs(pMetrics->totalRejectedJobs);
            metricsBean.setTotalStartedThreadCount(pMetrics->startedThreadCnt);
            metricsBean.setUpTime(pMetrics->upTime);

            helper.setMetrics(metricsBean);
        }

        return res;
    }

    /** */
    int32_t tcpPort;

    /** */
    int32_t replicaCnt;

    /** */
    std::string dfltCacheMode;

    /** */
    TGridClientVariantMap attrs;

    /** */
    TGridClientVariantMap caches;

    /** */
    TGridClientVariantSet tcpAddrs;

    /** */
    TGridClientVariantSet tcpHostNames;

    /** */
    boost::optional<GridClientUuid> nodeId;

    /** */
    GridClientVariant consistentId;

    /** */
    GridClientVariant metrics;
};

/**
 * Topology request.
 */
class GridClientTopologyRequest : public GridClientPortableMessage {
public:
    /** */
    static const int32_t TYPE_ID = 52;

    /**
     * @return Type ID.
     */
    int32_t typeId() const {
        return TYPE_ID;
    }

    /**
     * @param writer Writer.
     */
    void writePortable(GridPortableWriter& writer) const {
        GridClientPortableMessage::writePortable(writer);

        GridPortableRawWriter& raw = writer.rawWriter();

        raw.writeUuid(nodeId);
        raw.writeString(nodeIp);
        raw.writeBool(includeMetrics);
        raw.writeBool(includeAttrs);
    }

    /**
     * @param reader Reader.
     */
    void readPortable(GridPortableReader& reader) {
        GridClientPortableMessage::readPortable(reader);

        GridPortableRawReader& raw = reader.rawReader();

        nodeId = raw.readUuid();
        nodeIp  = raw.readString().get_value_or(std::string());
        includeMetrics = raw.readBool();
        includeAttrs = raw.readBool();
    }

    /** Id of requested node. */
     boost::optional<GridClientUuid> nodeId;

    /** IP address of requested node. */
     boost::optional<std::string> nodeIp;

    /** Include metrics flag. */
     bool includeMetrics;

    /** Include node attributes flag. */
     bool includeAttrs;
};

/**
 * Cache operation request.
 */
class GridClientCacheRequest : public GridClientPortableMessage {
public:
    /** */
    static const int32_t TYPE_ID = 54;

    /**
     * @return Type ID.
     */
    int32_t typeId() const {
        return TYPE_ID;
    }

    /**
     * Required default constructor.
     */
    GridClientCacheRequest() : key(0), val(0), val2(0), vals(0), cacheName(0) {
    }

    /**
     * @param cacheCmd Command.
     */
    GridClientCacheRequest(GridCacheRequestCommand& cacheCmd) : key(cacheCmd.getKey()), val(cacheCmd.getValue()),
        val2(cacheCmd.getValue2()), vals(cacheCmd.getValues()), cacheName(&cacheCmd.getCacheName()),
        cacheFlagsOn(cacheCmd.getFlags())  {
        op = static_cast<int32_t>(cacheCmd.getOperation());
    }

    /**
     * @param writer Writer.
     */
    void writePortable(GridPortableWriter& writer) const;

    /**
     * @param reader Reader.
     */
    void readPortable(GridPortableReader& reader) {
        assert(false);
    }

    /** */
    int32_t op;

    /** */
    const std::string* cacheName;

    /** */
    const GridClientVariant* key;

    /** */
    const GridClientVariant* val;

    /** */
    const GridClientVariant* val2;

    /** */
    int32_t cacheFlagsOn;

    /** */
    const TGridClientVariantMap* vals;
};

/**
 * Log request.
 */
class GridClientLogRequest : public GridClientPortableMessage {
public:
    /** */
    static const int32_t TYPE_ID = 55;

    /**
     * @return Type ID.
     */
    int32_t typeId() const {
        return TYPE_ID;
    }

    /**
     * @param writer Writer.
     */
    void writePortable(GridPortableWriter& writer) const {
        GridClientPortableMessage::writePortable(writer);

        GridPortableRawWriter& raw = writer.rawWriter();

        raw.writeString(path);

        raw.writeInt32(from);
        raw.writeInt32(to);
    }

    /**
     * @param reader Reader.
     */
    void readPortable(GridPortableReader& reader) {
        GridClientPortableMessage::readPortable(reader);

        GridPortableRawReader& raw = reader.rawReader();

        path = raw.readString().get_value_or(std::string());

        from = raw.readInt32();
        to = raw.readInt32();
    }

    /** */
    std::string path;

    /** */
    int32_t from;

    /** */
    int32_t to;
};

/**
 * Task request.
 */
class GridClientTaskRequest : public GridClientPortableMessage {
public:
    /** */
    static const int32_t TYPE_ID = 53;

    /**
     * @return Type ID.
     */
    int32_t typeId() const {
        return TYPE_ID;
    }

    /**
     * @param writer Writer.
     */
    void writePortable(GridPortableWriter &writer) const {
        GridClientPortableMessage::writePortable(writer);

        GridPortableRawWriter& raw = writer.rawWriter();

        raw.writeString(taskName);
        raw.writeVariant(arg);
    }

    /**
     * @param reader Reader.
     */
    void readPortable(GridPortableReader &reader) {
        assert(false);
    }

    /** */
    std::string taskName;

    /** */
    GridClientVariant arg;
};

/**
 * Task result.
 */
class GridClientTaskResultBean : public GridPortable {
public:
    /** */
    static const int32_t TYPE_ID = 59;

    /**
     * @return Type ID.
     */
    int32_t typeId() const {
        return TYPE_ID;
    }

    /**
     * @param writer Writer.
     */
    void writePortable(GridPortableWriter &writer) const {
        GridPortableRawWriter& raw = writer.rawWriter();

        raw.writeString(id);
        raw.writeBool(finished);
        raw.writeVariant(res);
        raw.writeString(error);
    }

    /**
     * @param reader Reader.
     */
    void readPortable(GridPortableReader &reader) {
        GridPortableRawReader& raw = reader.rawReader();

        boost::optional<std::string> idOpt = raw.readString();

        if (idOpt.is_initialized())
            id = idOpt.get();

        finished = raw.readBool();
        res = raw.readVariant();

        boost::optional<std::string> errOpt = raw.readString();

        if (errOpt.is_initialized())
            error = errOpt.get();
    }

    /** */
    std::string id;

    /** */
    bool finished;

    /** */
    GridClientVariant res;

    /** */
    std::string error;
};

/**
 * Authentication result.
 */
class GridClientAuthenticationRequest : public GridClientPortableMessage {
public:
    /** */
    static const int32_t TYPE_ID = 51;

    /**
     * Required default constructor.
     */
    GridClientAuthenticationRequest() {
    }

    /**
     * @param credStr Credentials.
     */
    GridClientAuthenticationRequest(std::string credStr) : cred(credStr) {
    }

    /**
     * @return Type ID.
     */
    int32_t typeId() const {
        return TYPE_ID;
    }

    /**
     * @param writer Writer.
     */
    void writePortable(GridPortableWriter &writer) const {
        GridClientPortableMessage::writePortable(writer);

        GridPortableRawWriter& raw = writer.rawWriter();

        raw.writeVariant(cred);
    }

    /**
     * @param reader Reader.
     */
    void readPortable(GridPortableReader &reader) {
        GridClientPortableMessage::readPortable(reader);

        GridPortableRawReader& raw = reader.rawReader();

        cred = raw.readVariant();
    }

    /** */
    GridClientVariant cred;
};

/**
 * Query request.
 */
class GridClientCacheQueryRequest : public GridClientPortableMessage {
public:
    /** */
    static const int32_t TYPE_ID = 0;

    GridClientCacheQueryRequest() {
    }

    /**
     * @return Type ID.
     */
    int32_t typeId() const {
        return TYPE_ID;
    }

    /**
     * @param writer Writer.
     */
    void writePortable(GridPortableWriter &writer) const {
        GridClientPortableMessage::writePortable(writer);

        GridPortableRawWriter& raw = writer.rawWriter();

    }

    /**
     * @param reader Reader.
     */
    void readPortable(GridPortableReader &reader) {
        assert(false);
    }

    /**
     * Operation type.
     */
    enum Operation {
        EXECUTE = 0,
        FETCH,
        REBUILD_INDEXES
    };

    /** */
    int64_t qryId;

    /** */
    Operation op;

    /** */
    GridQueryType type;

    /** */
    std::string* cacheName;

    /** */
    std::string* clause;

    /** */
    int32_t pageSize;

    /** */
    int64_t timeout;

    /** */
    bool incBackups;

    /** */
    bool dedup;

    /** */
    std::string* className;

    /** */
    std::string* reducerClassName;

    /** */
    std::string* transformerClassName;

    /** */
    std::vector<GridClientVariant>* classArgs;

    /** */
    std::vector<GridClientVariant>* args;
};

/**
 * Query result.
 */
class GridClientDataQueryResult : public GridPortable {
public:
    /** */
    static const int32_t TYPE_ID = 0;

    /**
     * @return Type ID.
     */
    int32_t typeId() const {
        return TYPE_ID;
    }

    /**
     * @param writer Writer.
     */
    void writePortable(GridPortableWriter &writer) const {
        assert(false);
    }

    /**
     * @param reader Reader.
     */
    void readPortable(GridPortableReader &reader) {
        GridPortableRawReader& raw = reader.rawReader();
    }

    /** */
    int64_t qryId;

    /** */
    std::vector<GridClientVariant> items;

    /** */
    bool last;

    /** */
    GridClientUuid nodeId;
};

#endif // GRIDCLIENT_PORTABLE_MESSAGES_HPP_INCLUDED
