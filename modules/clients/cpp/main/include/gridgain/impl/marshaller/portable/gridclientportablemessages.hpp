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
#include "gridgain/impl/marshaller/gridnodemarshallerhelper.hpp"
#include "gridgain/impl/cmd/gridclientmessagecacherequestcommand.hpp"
#include "gridgain/impl/utils/gridclientlog.hpp"

class GridClientPortableMessage : public GridPortable {
public:
    void writePortable(GridPortableWriter &writer) const override {
        writer.writeByteCollection("sesTok", sesTok);
    }

    void readPortable(GridPortableReader &reader) override {
        boost::optional<std::vector<int8_t>> bytes = reader.readByteCollection("sesTok");

        if (bytes.is_initialized())
            sesTok = std::move(bytes.get());
    }

    std::vector<int8_t> sesTok;
};

class GridClientResponse : public GridClientPortableMessage {
public:
    int32_t typeId() const {
        return -6;
    }

    void writePortable(GridPortableWriter &writer) const override {
        GridClientPortableMessage::writePortable(writer);

        writer.writeInt32("status", status);
        writer.writeString("errorMsg", errorMsg);
        writer.writeVariant("res", res);
    }

    void readPortable(GridPortableReader &reader) override {
        GridClientPortableMessage::readPortable(reader);

        status = reader.readInt32("status");
        
        boost::optional<std::string> msg = reader.readString("errorMsg");
        
        if (msg.is_initialized())
            errorMsg = std::move(msg.get());

        res = reader.readVariant("res");
    }

    int32_t getStatus() {
        return status;
    }

    std::string getErrorMessage() {
        return errorMsg;
    }

    GridClientVariant getResult() {
        return res;
    }

    int32_t status;

    std::string errorMsg;

    GridClientVariant res;
};

class GridClientMetricsBean : public GridPortable {
public:
    int32_t typeId() const {
        return -5;            
    }

    void writePortable(GridPortableWriter &writer) const override {
        writer.writeInt64("lastUpdateTime", lastUpdateTime);
        writer.writeInt32("maxActiveJobs", maxActiveJobs);
        writer.writeInt32("curActiveJobs", curActiveJobs);
        writer.writeFloat("avgActiveJobs", avgActiveJobs);
        writer.writeInt32("maxWaitingJobs", maxWaitingJobs);
        writer.writeInt32("curWaitingJobs", curWaitingJobs);
        writer.writeFloat("avgWaitingJobs", avgWaitingJobs);
        writer.writeInt32("maxRejectedJobs", maxRejectedJobs);
        writer.writeInt32("curRejectedJobs", curRejectedJobs);
        writer.writeFloat("avgRejectedJobs", avgRejectedJobs);
        writer.writeInt32("maxCancelledJobs", maxCancelledJobs);
        writer.writeInt32("curCancelledJobs", curCancelledJobs);
        writer.writeFloat("avgCancelledJobs", avgCancelledJobs);
        writer.writeInt32("totalRejectedJobs", totalRejectedJobs);
        writer.writeInt32("totalCancelledJobs", totalCancelledJobs);
        writer.writeInt32("totalExecutedJobs", totalExecutedJobs);
        writer.writeInt64("maxJobWaitTime", maxJobWaitTime);
        writer.writeInt64("curJobWaitTime", curJobWaitTime);
        writer.writeDouble("avgJobWaitTime", avgJobWaitTime);
        writer.writeInt64("maxJobExecTime", maxJobExecTime);
        writer.writeInt64("curJobExecTime", curJobExecTime);
        writer.writeDouble("avgJobExecTime", avgJobExecTime);
        writer.writeInt32("totalExecTasks", totalExecTasks);
        writer.writeInt64("totalIdleTime", totalIdleTime);
        writer.writeInt64("curIdleTime", curIdleTime);
        writer.writeInt32("availProcs", availProcs);
        writer.writeDouble("load", load);
        writer.writeDouble("avgLoad", avgLoad);
        writer.writeDouble("gcLoad", gcLoad);
        writer.writeInt64("heapInit", heapInit);
        writer.writeInt64("heapUsed", heapUsed);
        writer.writeInt64("heapCommitted", heapCommitted);
        writer.writeInt64("heapMax", heapMax);
        writer.writeInt64("nonHeapInit", nonHeapInit);
        writer.writeInt64("nonHeapUsed", nonHeapUsed);
        writer.writeInt64("nonHeapCommitted", nonHeapCommitted);
        writer.writeInt64("nonHeapMax", nonHeapMax);
        writer.writeInt64("upTime", upTime);
        writer.writeInt64("startTime", startTime);
        writer.writeInt64("nodeStartTime", nodeStartTime);
        writer.writeInt32("threadCnt", threadCnt);
        writer.writeInt32("peakThreadCnt", peakThreadCnt);
        writer.writeInt64("startedThreadCnt", startedThreadCnt);
        writer.writeInt32("daemonThreadCnt", daemonThreadCnt);
        writer.writeInt64("fileSysFreeSpace", fileSysFreeSpace);
        writer.writeInt64("fileSysTotalSpace", fileSysTotalSpace);
        writer.writeInt64("fileSysUsableSpace", fileSysUsableSpace);
        writer.writeInt64("lastDataVer", lastDataVer);
        writer.writeInt32("sentMsgsCnt", sentMsgsCnt);
        writer.writeInt64("sentBytesCnt", sentBytesCnt);
        writer.writeInt32("rcvdMsgsCnt", rcvdMsgsCnt);
        writer.writeInt64("rcvdBytesCnt", rcvdBytesCnt);
    }

    void readPortable(GridPortableReader &reader) override {
        lastUpdateTime = reader.readInt64("lastUpdateTime");
        maxActiveJobs = reader.readInt32("maxActiveJobs");
        curActiveJobs = reader.readInt32("curActiveJobs");
        avgActiveJobs = reader.readFloat("avgActiveJobs");
        maxWaitingJobs = reader.readInt32("maxWaitingJobs");
        curWaitingJobs = reader.readInt32("curWaitingJobs");
        avgWaitingJobs = reader.readFloat("avgWaitingJobs");
        maxRejectedJobs = reader.readInt32("maxRejectedJobs");
        curRejectedJobs = reader.readInt32("curRejectedJobs");
        avgRejectedJobs = reader.readFloat("avgRejectedJobs");
        maxCancelledJobs = reader.readInt32("maxCancelledJobs");
        curCancelledJobs = reader.readInt32("curCancelledJobs");
        avgCancelledJobs = reader.readFloat("avgCancelledJobs");
        totalRejectedJobs = reader.readInt32("totalRejectedJobs");
        totalCancelledJobs = reader.readInt32("totalCancelledJobs");
        totalExecutedJobs = reader.readInt32("totalExecutedJobs");
        maxJobWaitTime = reader.readInt64("maxJobWaitTime");
        curJobWaitTime = reader.readInt64("curJobWaitTime");
        avgJobWaitTime = reader.readDouble("avgJobWaitTime");
        maxJobExecTime = reader.readInt64("maxJobExecTime");
        curJobExecTime = reader.readInt64("curJobExecTime");
        avgJobExecTime = reader.readDouble("avgJobExecTime");
        totalExecTasks = reader.readInt32("totalExecTasks");
        totalIdleTime = reader.readInt64("totalIdleTime");
        curIdleTime = reader.readInt64("curIdleTime");
        availProcs = reader.readInt32("availProcs");
        load = reader.readDouble("load");
        avgLoad = reader.readDouble("avgLoad");
        gcLoad = reader.readDouble("gcLoad");
        heapInit = reader.readInt64("heapInit");
        heapUsed = reader.readInt64("heapUsed");
        heapCommitted = reader.readInt64("heapCommitted");
        heapMax = reader.readInt64("heapMax");
        nonHeapInit = reader.readInt64("nonHeapInit");
        nonHeapUsed = reader.readInt64("nonHeapUsed");
        nonHeapCommitted = reader.readInt64("nonHeapCommitted");
        nonHeapMax = reader.readInt64("nonHeapMax");
        upTime = reader.readInt64("upTime");
        startTime = reader.readInt64("startTime");
        nodeStartTime = reader.readInt64("nodeStartTime");
        threadCnt = reader.readInt32("threadCnt");
        peakThreadCnt = reader.readInt32("peakThreadCnt");
        startedThreadCnt = reader.readInt64("startedThreadCnt");
        daemonThreadCnt = reader.readInt32("daemonThreadCnt");
        fileSysFreeSpace = reader.readInt64("fileSysFreeSpace");
        fileSysTotalSpace = reader.readInt64("fileSysTotalSpace");
        fileSysUsableSpace = reader.readInt64("fileSysUsableSpace");
        lastDataVer = reader.readInt64("lastDataVer");
        sentMsgsCnt = reader.readInt32("sentMsgsCnt");
        sentBytesCnt = reader.readInt64("sentBytesCnt");
        rcvdMsgsCnt = reader.readInt32("rcvdMsgsCnt");
        rcvdBytesCnt = reader.readInt64("rcvdBytesCnt");
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

class GridClientNodeBean : public GridPortable {
public:
    int32_t typeId() const {
        return -4;            
    }

    void writePortable(GridPortableWriter &writer) const override {
        writer.writeInt32("tcpPort", tcpPort);
        writer.writeInt32("jettyPort", jettyPort);
        writer.writeInt32("replicaCnt", replicaCnt);

        writer.writeString("dfltCacheMode", dfltCacheMode);

        writer.writeVariantMap("attrs", attrs);
        writer.writeVariantMap("caches", caches);

        writer.writeVariantCollection("tcpAddrs", tcpAddrs);
        writer.writeVariantCollection("tcpHostNames", tcpHostNames);
        writer.writeVariantCollection("jettyAddrs", jettyAddrs);
        writer.writeVariantCollection("jettyHostNames", jettyHostNames);

        writer.writeUuid("nodeId", nodeId);

        writer.writeVariant("consistentId", consistentId);
        writer.writeVariant("metrics", metrics);
    }

    void readPortable(GridPortableReader &reader) override {
        tcpPort = reader.readInt32("tcpPort");
        jettyPort = reader.readInt32("jettyPort");
        replicaCnt = reader.readInt32("replicaCnt");

        dfltCacheMode = reader.readString("dfltCacheMode").get_value_or(std::string());

        attrs = reader.readVariantMap("attrs").get_value_or(TGridClientVariantMap());
        caches = reader.readVariantMap("caches").get_value_or(TGridClientVariantMap());

        tcpAddrs = reader.readVariantCollection("tcpAddrs").get_value_or(TGridClientVariantSet());
        tcpHostNames = reader.readVariantCollection("tcpHostNames").get_value_or(TGridClientVariantSet());
        jettyAddrs = reader.readVariantCollection("jettyAddrs").get_value_or(TGridClientVariantSet());
        jettyHostNames = reader.readVariantCollection("jettyHostNames").get_value_or(TGridClientVariantSet());

        nodeId = reader.readUuid("nodeId");

        consistentId = reader.readVariant("consistentId");
        metrics = reader.readVariant("metrics");
    }

    GridClientNode createNode() {
        GridClientNode res;

        GridClientNodeMarshallerHelper helper(res);

        if (!nodeId)
            throw GridClientCommandException("Failed to read node ID");

        helper.setNodeId(nodeId.get());

        helper.setConsistentId(consistentId);

        int tcpport = tcpPort;
        int jettyport = jettyPort;

        std::vector<GridClientSocketAddress> addresses;

        boost::asio::io_service ioSrvc;
        boost::asio::ip::tcp::resolver resolver(ioSrvc);

        for (size_t i = 0; i < jettyAddrs.size(); ++i) {
            GridClientVariant addr = jettyAddrs[i];

            GridClientSocketAddress newJettyAddress = GridClientSocketAddress(addr.getString(), jettyport);

            boost::asio::ip::tcp::resolver::query queryIp(addr.getString(), boost::lexical_cast<std::string>(jettyport));

            boost::system::error_code ec;

            boost::asio::ip::tcp::resolver::iterator endpoint_iter = resolver.resolve(queryIp, ec);

            if (!ec)
                addresses.push_back(newJettyAddress);
            else
                GG_LOG_ERROR("Error resolving hostname: %s, %s", addr.getString().c_str(), ec.message().c_str());
        }

        for (size_t i = 0; i < jettyHostNames.size(); ++i) {
            GridClientSocketAddress newJettyAddress = GridClientSocketAddress(jettyHostNames[i].getString(), jettyport);

            boost::asio::ip::tcp::resolver::query queryHostname(jettyHostNames[i].getString(),
                boost::lexical_cast<std::string>(jettyport));

            boost::system::error_code ec;

            boost::asio::ip::tcp::resolver::iterator endpoint_iter = resolver.resolve(queryHostname, ec);

            if (!ec)
                addresses.push_back(newJettyAddress);
            else
                GG_LOG_ERROR("Error resolving hostname: %s, %s", jettyHostNames[i].getString().c_str(), ec.message().c_str());
        }

        helper.setJettyAddresses(addresses);
        addresses.clear();

        for (size_t i = 0; i < tcpAddrs.size(); ++i) {
            GridClientSocketAddress newTCPAddress = GridClientSocketAddress(tcpAddrs[i].getString(), tcpport);

            boost::asio::ip::tcp::resolver::query queryIp(tcpAddrs[i].getString(),
                boost::lexical_cast<std::string>(tcpport));

            boost::system::error_code ec;

            boost::asio::ip::tcp::resolver::iterator endpoint_iter = resolver.resolve(queryIp, ec);

            if (!ec)
                addresses.push_back(newTCPAddress);
            else
                GG_LOG_ERROR("Error resolving hostname: %s, %s", tcpAddrs[i].getString().c_str(), ec.message().c_str());
        }

        for (size_t i = 0; i < tcpHostNames.size(); ++i) {
            GridClientSocketAddress newTCPAddress = GridClientSocketAddress(tcpHostNames[i].getString(), tcpport);

            boost::asio::ip::tcp::resolver::query queryHostname(tcpHostNames[i].getString(),
                boost::lexical_cast<std::string>(tcpport));

            boost::system::error_code ec;

            boost::asio::ip::tcp::resolver::iterator endpoint_iter = resolver.resolve(queryHostname, ec);

            if (!ec)
                addresses.push_back(newTCPAddress);
            else
                GG_LOG_ERROR("Error resolving hostname: %s, %s", tcpHostNames[i].getString().c_str(), ec.message().c_str());
        }

        helper.setTcpAddresses(addresses);

        helper.setCaches(caches);

        helper.setAttributes(attrs);

        if (metrics.hasPortable()) {
            assert(metrics.getPortable()->typeId() == -5);

            GridClientMetricsBean* pMetrics = metrics.getPortable<GridClientMetricsBean>();

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

            delete pMetrics;
        }

        return res;
    }

    int32_t tcpPort;
    
    int32_t jettyPort;
    
    int32_t replicaCnt;

    std::string dfltCacheMode;

    TGridClientVariantMap attrs;
    
    TGridClientVariantMap caches;

    TGridClientVariantSet tcpAddrs;

    TGridClientVariantSet tcpHostNames;

    TGridClientVariantSet jettyAddrs;

    TGridClientVariantSet jettyHostNames;

    boost::optional<GridClientUuid> nodeId;

    GridClientVariant consistentId;

    GridClientVariant metrics;
};

class GridClientTopologyRequest : public GridClientPortableMessage {
public:
    int32_t typeId() const {
        return -9;
    }

    void writePortable(GridPortableWriter &writer) const override {
        GridClientPortableMessage::writePortable(writer);

        writer.writeUuid("nodeId", nodeId);
        writer.writeString("nodeIp", nodeIp);
        writer.writeBool("includeMetrics", includeMetrics);
        writer.writeBool("includeAttrs", includeAttrs);
    }

    void readPortable(GridPortableReader &reader) override {
        GridClientPortableMessage::readPortable(reader);

        nodeId = reader.readUuid("nodeId");
        nodeIp  = reader.readString("nodeIp").get_value_or(std::string());
        includeMetrics = reader.readBool("includeMetrics");
        includeAttrs = reader.readBool("includeAttrs");
    }

    /** Id of requested node. */
     boost::optional<GridClientUuid> nodeId;

    /** IP address of requested node. */
     std::string nodeIp;

    /** Include metrics flag. */
     bool includeMetrics;

    /** Include node attributes flag. */
     bool includeAttrs;
};

class GridClientCacheRequest : public GridClientPortableMessage {
public:
    void init(GridCacheRequestCommand& cacheCmd) {
        op = static_cast<int32_t>(cacheCmd.getOperation());
       
        cacheName = cacheCmd.getCacheName();
        
        key = cacheCmd.getKey();
        val = cacheCmd.getValue();
        val2 = cacheCmd.getValue2();
        vals = cacheCmd.getValues();

        std::set<GridClientCacheFlag> flags = cacheCmd.getFlags();

        cacheFlagsOn = flags.empty() ? 0 : GridClientByteUtils::bitwiseOr(flags.begin(), flags.end(), 0);
    }

    int32_t typeId() const {
        return -2;
    }

    void writePortable(GridPortableWriter &writer) const override {
        GridClientPortableMessage::writePortable(writer);

        writer.writeInt32("op", op);

        writer.writeString("cacheName", cacheName);

        writer.writeVariant("key", key);
        writer.writeVariant("val", val);
        writer.writeVariant("val2", val2);

        writer.writeVariantMap("vals", vals);

        writer.writeInt32("flags", cacheFlagsOn);
    }

    void readPortable(GridPortableReader &reader) override {
        GridClientPortableMessage::readPortable(reader);

        op = reader.readInt32("op");

        cacheName = reader.readString("cacheName").get_value_or(std::string());

        key = reader.readVariant("key");
        val = reader.readVariant("val");
        val2 = reader.readVariant("val2");

        vals = reader.readVariantMap("vals").get_value_or(TGridClientVariantMap());

        cacheFlagsOn = reader.readInt32("flags");
    }

    int32_t op;

    std::string cacheName;

    GridClientVariant key;

    GridClientVariant val;

    GridClientVariant val2;

    int32_t cacheFlagsOn;

    TGridClientVariantMap vals;
};

class GridClientLogRequest : public GridClientPortableMessage {
public:
    int32_t typeId() const {
        return -3;
    }

    void writePortable(GridPortableWriter &writer) const override {
        GridClientPortableMessage::writePortable(writer);

        writer.writeString("path", path);

        writer.writeInt32("from", from);
        writer.writeInt32("to", to);
    }

    void readPortable(GridPortableReader &reader) override {
        GridClientPortableMessage::readPortable(reader);

        path = reader.readString("path").get_value_or(std::string());

        from = reader.readInt32("from");
        to = reader.readInt32("to");
    }

    std::string path;

    int32_t from;

    int32_t to;
};

class GridClientTaskRequest : public GridClientPortableMessage {
public:
    int32_t typeId() const {
        return -7;
    }

    void writePortable(GridPortableWriter &writer) const override {
        GridClientPortableMessage::writePortable(writer);

        writer.writeString("taskName", taskName);

        writer.writeVariant("arg", arg);
    }

    void readPortable(GridPortableReader &reader) override {
        GridClientPortableMessage::readPortable(reader);

        taskName = reader.readString("taskName").get_value_or(std::string());

        arg = reader.readVariant("arg");
    }

    std::string taskName;

    GridClientVariant arg;
};

class GridClientTaskResultBean : public GridPortable {
public:
    int32_t typeId() const {
        return -8;
    }

    void writePortable(GridPortableWriter &writer) const override {
        writer.writeString("id", id);
        
        writer.writeBool("finished", finished);
        
        writer.writeVariant("res", res);
        
        writer.writeString("error", error);
    }

    void readPortable(GridPortableReader &reader) override {
        id = reader.readString("id").get_value_or(std::string());

        finished = reader.readBool("finished");

        res = reader.readVariant("res");

        error = reader.readString("error").get_value_or(std::string());;
    }

    std::string id;

    bool finished;

    GridClientVariant res;

    std::string error;
};

class GridClientAuthenticationRequest : public GridClientPortableMessage {
public:
    GridClientAuthenticationRequest() {
    }

    GridClientAuthenticationRequest(std::string credStr) : cred(credStr) {
    }

    int32_t typeId() const {
        return -1; // TODO 8536
    }

    void writePortable(GridPortableWriter &writer) const override {
        GridClientPortableMessage::writePortable(writer);

        writer.writeVariant("cred", cred);
    }

    void readPortable(GridPortableReader &reader) override {
        GridClientPortableMessage::readPortable(reader);

        cred = reader.readVariant("cred");
    }

    GridClientVariant cred;
};

class GridClientCacheMetricsBean : public GridPortable {
public:
    int32_t typeId() const override {
        return -8; // TODO 8536.
    }

    void writePortable(GridPortableWriter &writer) const override {
        writer.writeInt64("createTime", createTime);
        writer.writeInt64("readTime", readTime);
        writer.writeInt64("writeTime", writeTime);
        writer.writeInt32("reads", reads);
        writer.writeInt32("writes", writes);
        writer.writeInt32("hits", hits);
        writer.writeInt32("misses", misses);
    }

    void readPortable(GridPortableReader &reader) override {
        createTime = reader.readInt64("createTime");
        readTime = reader.readInt64("readTime");
        writeTime = reader.readInt64("writeTime");
        reads = reader.readInt32("reads");
        writes = reader.readInt32("writes");
        hits = reader.readInt32("hots");
        misses = reader.readInt32("misses");
    }

    int64_t createTime;

    int64_t readTime;

    int64_t writeTime;

    int32_t reads;

    int32_t writes;

    int32_t hits;

    int32_t misses;
};

#endif // GRIDCLIENT_PORTABLE_MESSAGES_HPP_INCLUDED
