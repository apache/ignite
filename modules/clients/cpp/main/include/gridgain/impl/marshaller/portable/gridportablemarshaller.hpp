/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLIENT_PORTABLE_MARSHALLER_HPP_INCLUDED
#define GRIDCLIENT_PORTABLE_MARSHALLER_HPP_INCLUDED

#include <vector>
#include <iterator>
#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>

#include "gridgain/gridportable.hpp"
#include "gridgain/gridportablereader.hpp"
#include "gridgain/gridportablewriter.hpp"
#include "gridgain/impl/utils/gridclientbyteutils.hpp"

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
#include "gridgain/impl/utils/gridclientlog.hpp"
#include "gridgain/impl/marshaller/gridnodemarshallerhelper.hpp"
#include "gridgain/gridclientexception.hpp"
#include "gridgain/gridclientnode.hpp"
#include "gridgain/impl/utils/gridclientlog.hpp"

const int8_t TYPE_NULL = 0;
const int8_t TYPE_BYTE = 1;
const int8_t TYPE_SHORT = 2;
const int8_t TYPE_INT = 3;
const int8_t TYPE_LONG = 4;
const int8_t TYPE_FLOAT = 5;
const int8_t TYPE_DOUBLE = 6;
const int8_t TYPE_BOOLEAN = 7;
const int8_t TYPE_CHAR = 8;
const int8_t TYPE_STRING = 9;
const int8_t TYPE_BYTE_ARRAY = 10;

const int8_t TYPE_LIST = 18;
const int8_t TYPE_MAP = 19;
const int8_t TYPE_UUID = 20;
const int8_t TYPE_USER_OBJECT = 21;

const int8_t OBJECT_TYPE_OBJECT = 0;
const int8_t OBJECT_TYPE_NULL = 2;

GridPortable* createPortable(int32_t typeId, GridPortableReader &reader);

class GridClientPortableMessage : public GridPortable {
public:
    void writePortable(GridPortableWriter &writer) const {
        writer.writeBytes("sesTok", sesTok); //TODO 8536.
    }

    void readPortable(GridPortableReader &reader) {
        sesTok = reader.readBytes("sesTok");
    }

    bool operator==(const GridPortable& other) const {
        assert(false);

        return false; // Not needed since is not used as key.
    }

    int hashCode() const {
        assert(false);

        return 0; // Not needed since is not used as key.
    }

    std::vector<int8_t> sesTok;
};

class GridClientResponse : public GridClientPortableMessage {
public:
    int32_t typeId() const {
        return -6;
    }

    void writePortable(GridPortableWriter &writer) const {
        GridClientPortableMessage::writePortable(writer);

        writer.writeInt32("status", status);
        writer.writeString("errorMsg", errorMsg);
        writer.writeVariant("res", res);
    }

    void readPortable(GridPortableReader &reader) {
        GridClientPortableMessage::readPortable(reader);

        status = reader.readInt32("status");
        errorMsg = reader.readString("errorMsg");
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

    void writePortable(GridPortableWriter &writer) const {
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

    void readPortable(GridPortableReader &reader) {
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

    bool operator==(const GridPortable& other) const {
        assert(false);

        return false; // Not needed since is not used as key.
    }

    int hashCode() const {
        assert(false);

        return 0; // Not needed since is not used as key.
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

// TODO: 8536.
class GridClientNodeBean : public GridPortable {
public:
    int32_t typeId() const {
        return -4;            
    }

    void writePortable(GridPortableWriter &writer) const {
        writer.writeInt32("tcpPort", tcpPort);
        writer.writeInt32("jettyPort", jettyPort);
        writer.writeInt32("replicaCnt", replicaCnt);

        writer.writeString("dfltCacheMode", dfltCacheMode);

        writer.writeMap("attrs", attrs);
        writer.writeMap("caches", caches);

        writer.writeCollection("tcpAddrs", tcpAddrs);
        writer.writeCollection("tcpHostNames", tcpHostNames);
        writer.writeCollection("jettyAddrs", jettyAddrs);
        writer.writeCollection("jettyHostNames", jettyHostNames);

        writer.writeUuid("nodeId", nodeId);

        writer.writeVariant("consistentId", consistentId);
        writer.writeVariant("metrics", metrics);
    }

    void readPortable(GridPortableReader &reader) {
        tcpPort = reader.readInt32("tcpPort");
        jettyPort = reader.readInt32("jettyPort");
        replicaCnt = reader.readInt32("replicaCnt");

        dfltCacheMode = reader.readString("dfltCacheMode");

        attrs = reader.readMap("attrs");
        caches = reader.readMap("caches");

        tcpAddrs = reader.readCollection("tcpAddrs");
        tcpHostNames = reader.readCollection("tcpHostNames");
        jettyAddrs = reader.readCollection("jettyAddrs");
        jettyHostNames = reader.readCollection("jettyHostNames");

        nodeId = reader.readUuid("nodeId");

        consistentId = reader.readVariant("consistentId");
        metrics = reader.readVariant("metrics");
    }

    bool operator==(const GridPortable& other) const {
        assert(false);

        return false; // Not needed since is not used as key.
    }

    int hashCode() const {
        assert(false);

        return 0; // Not needed since is not used as key.
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
                GG_LOG_ERROR("Error resolving hostname: %s, %s", addr.getString(), ec.message().c_str());
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
                GG_LOG_ERROR("Error resolving hostname: %s, %s", jettyHostNames[i].getString(), ec.message().c_str());
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
                GG_LOG_ERROR("Error resolving hostname: %s, %s", tcpAddrs[i].getString(), ec.message().c_str());
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
                GG_LOG_ERROR("Error resolving hostname: %s, %s", tcpHostNames[i].getString(), ec.message().c_str());
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

    std::unordered_map<GridClientVariant, GridClientVariant> attrs;
    
    std::unordered_map<GridClientVariant, GridClientVariant> caches;

    std::vector<GridClientVariant> tcpAddrs;

    std::vector<GridClientVariant> tcpHostNames;

    std::vector<GridClientVariant> jettyAddrs;

    std::vector<GridClientVariant> jettyHostNames;

    boost::optional<GridClientUuid> nodeId;

    GridClientVariant consistentId;

    GridClientVariant metrics;
};

// TODO: 8536 reuse existing message classes.
class GridClientTopologyRequest : public GridClientPortableMessage {
public:
    /**
     * Getter method for include metrics flag.
     *
     * @return Include metrics flag.
     */
    bool getIncludeMetrics() const {
        return includeMetrics;
    }

    /**
     * Setter method for include metrics flag.
     *
     * @param pIncludeMetrics Include metrics flag.
     */
    void setIncludeMetrics(bool pIncludeMetrics) {
        includeMetrics = pIncludeMetrics;
    }

    /**
     * Getter method for include attributes flag.
     *
     * @return Include node attributes flag.
     */
     bool getIncludeAttributes() const {
        return includeAttrs;
     }

    /**
     * Setter method for include attributes flag.
     *
     * @param pIncludeAttrs Include node attributes flag.
     */
     void setIncludeAttributes(bool pIncludeAttrs) {
        includeAttrs = pIncludeAttrs;
     }

    /**
     * Getter method for node id.
     *
     * @return Node identifier, if specified, empty string otherwise.
     */
     boost::optional<GridClientUuid> getNodeId() const {
        return nodeId;
     }

    /**
     * Setter method for node id.
     *
     * @param pNodeId Node identifier to lookup.
     */
     void setNodeId(const GridClientUuid& nodeId) {
        this->nodeId = nodeId;
     }

    /**
     * Getter method for node id.
     *
     * @return Node ip address if specified, empty string otherwise.
     */
    std::string getNodeIp() const {
        return nodeIp;
    }

    /**
     * Setter method for node id.
     *
     * @param pNodeIp Node ip address to lookup.
     */
    void setNodeIp(const std::string& nodeIp) {
        this->nodeIp = nodeIp;
    }

    int32_t typeId() const {
        return -9;
    }

    void writePortable(GridPortableWriter &writer) const {
        GridClientPortableMessage::writePortable(writer);

        writer.writeUuid("nodeId", nodeId);
        writer.writeString("nodeIp", nodeIp);
        writer.writeBool("includeMetrics", includeMetrics);
        writer.writeBool("includeAttrs", includeAttrs);
    }

    void readPortable(GridPortableReader &reader) {
        GridClientPortableMessage::readPortable(reader);

        nodeId = reader.readUuid("nodeId");
        nodeIp  = reader.readString("nodeIp");
        includeMetrics = reader.readBool("includeMetrics");
        includeAttrs = reader.readBool("includeAttrs");
    }

private:
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

    void writePortable(GridPortableWriter &writer) const {
        GridClientPortableMessage::writePortable(writer);

        writer.writeInt32("op", op);

        writer.writeString("cacheName", cacheName);

        writer.writeVariant("key", key);
        writer.writeVariant("val", val);
        writer.writeVariant("val2", val2);

        writer.writeMap("vals", vals);

        writer.writeInt32("flags", cacheFlagsOn);
    }

    void readPortable(GridPortableReader &reader) {
        GridClientPortableMessage::readPortable(reader);

        op = reader.readInt32("op");

        cacheName = reader.readString("cacheName");

        key = reader.readVariant("key");
        val = reader.readVariant("val");
        val2 = reader.readVariant("val2");

        vals = reader.readMap("vals");

        cacheFlagsOn = reader.readInt32("flags");
    }

    int32_t op;

    std::string cacheName;

    GridClientVariant key;

    GridClientVariant val;

    GridClientVariant val2;

    int32_t cacheFlagsOn;

    std::unordered_map<GridClientVariant, GridClientVariant> vals;
};

class GridClientLogRequest : public GridClientPortableMessage {
public:
    int32_t typeId() const {
        return -3;
    }

    void writePortable(GridPortableWriter &writer) const {
        GridClientPortableMessage::writePortable(writer);

        writer.writeString("path", path);

        writer.writeInt32("from", from);
        writer.writeInt32("to", to);
    }

    void readPortable(GridPortableReader &reader) {
        GridClientPortableMessage::readPortable(reader);

        path = reader.readString("path");

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

    void writePortable(GridPortableWriter &writer) const {
        GridClientPortableMessage::writePortable(writer);

        writer.writeString("taskName", taskName);

        writer.writeVariant("arg", arg);
    }

    void readPortable(GridPortableReader &reader) {
        GridClientPortableMessage::readPortable(reader);

        taskName = reader.readString("taskName");

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

    void writePortable(GridPortableWriter &writer) const {
        writer.writeString("id", id);
        
        writer.writeBool("finished", finished);
        
        writer.writeVariant("res", res);
        
        writer.writeString("error", error);
    }

    void readPortable(GridPortableReader &reader) {
        id = reader.readString("id");

        finished = reader.readBool("finished");

        res = reader.readVariant("res");

        error = reader.readString("error");
    }

    bool operator==(const GridPortable& other) const {
        assert(false);

        return false; // Not needed since is not used as key.
    }

    int hashCode() const {
        assert(false);

        return 0; // Not needed since is not used as key.
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
        return -1;
    }

    void writePortable(GridPortableWriter &writer) const {
        GridClientPortableMessage::writePortable(writer);

        writer.writeVariant("cred", cred);
    }

    void readPortable(GridPortableReader &reader) {
        GridClientPortableMessage::readPortable(reader);

        cred = reader.readVariant("cred");
    }

    GridClientVariant cred;
};

class PortableOutput {
public:
	PortableOutput(size_t capacity) {
        out.reserve(capacity);
	}

    void writeByte(int8_t val) {
        out.push_back(val);
    }

	void writeInt16(int16_t val) {
        int8_t* bytes = reinterpret_cast<int8_t*>(&val);

        out.insert(out.end(), bytes, bytes + 2);
	}

	void writeInt32(int32_t val) {
        int8_t* bytes = reinterpret_cast<int8_t*>(&val);

        out.insert(out.end(), bytes, bytes + 4);
	}

    void writeInt64(int64_t val) {
        int8_t* bytes = reinterpret_cast<int8_t*>(&val);

        out.insert(out.end(), bytes, bytes + 8);
	}

    void writeFloat(float val) {
        int8_t* bytes = reinterpret_cast<int8_t*>(&val);

        out.insert(out.end(), bytes, bytes + 4);
	}

    void writeDouble(double val) {
        int8_t* bytes = reinterpret_cast<int8_t*>(&val);

        out.insert(out.end(), bytes, bytes + 8);
	}

	void writeBytes(const void* src, size_t size) {
        const int8_t* bytes = reinterpret_cast<const int8_t*>(src);

        out.insert(out.end(), bytes, bytes + size);
	}

	std::vector<int8_t> bytes() {
        return out;
	}

private:
    std::vector<int8_t> out;
};

class GridPortableWriterImpl : public GridPortableWriter {
public:
	GridPortableWriterImpl() : out(1024) {
	}

    void writePortable(GridPortable &portable) {
        out.writeByte(OBJECT_TYPE_OBJECT);

        out.writeInt32(portable.typeId());

        portable.writePortable(*this);
    }

    void writeByte(char* fieldName, int8_t val) override {
		out.writeByte(val);
	}

    void writeInt16(char* fieldName, int16_t val) override {
		out.writeInt16(val);
	}

    void writeInt32(char* fieldName, int32_t val) override {
		out.writeInt32(val);
	}

    void writeInt64(char* fieldName, int64_t val) override {
		out.writeInt64(val);
	}

    void writeFloat(char* fieldName, float val) override {
		out.writeFloat(val);
	}

    void writeDouble(char* fieldName, double val) override {
		out.writeDouble(val);
	}

	void writeString(char* fieldName, const std::string &str) override {
		if (!str.empty()) {
            out.writeInt32(str.length());
		    out.writeBytes(str.data(), str.length());
        }
        else
            out.writeInt32(-1);
	}
	
    void writeBytes(char* fieldName, const std::vector<int8_t>& val) override {
        out.writeInt32(val.size());
        out.writeBytes(val.data(), val.size());
    }
    
	void writeBool(char* fieldName, bool val) override {
		out.writeByte(val ? 1 : 0);
	}

	void writeUuid(char* fieldName, const boost::optional<GridClientUuid>& val) override {
        if (val) {
            out.writeByte(1);

            out.writeInt64(val.get().mostSignificantBits());
            out.writeInt64(val.get().leastSignificantBits());
        }
        else
            out.writeByte(0);
	}

    void writeVariant(char* fieldName, const GridClientVariant &val) override {
        if (val.hasInt()) {
            out.writeByte(TYPE_INT);
            out.writeInt32(val.getInt());
        }
        else if (val.hasString()) {
            out.writeByte(TYPE_STRING);

            std::string str = val.getString();

            writeString(nullptr, str);
        }
        else if (val.hasUuid()) {
            out.writeByte(TYPE_UUID);

            GridClientUuid uuid = val.getUuid();

            out.writeInt64(uuid.mostSignificantBits());
            out.writeInt64(uuid.leastSignificantBits());
        }
        else if (val.hasPortable()) {
            out.writeByte(TYPE_USER_OBJECT);

            writePortable(*val.getPortable<GridPortable>());
        }
        else if (val.hasVariantVector()) {
            out.writeByte(TYPE_LIST);

            writeCollection(val.getVariantVector());
        }
        else if (val.hasVariantMap()) {
            out.writeByte(TYPE_MAP);

            writeMap(val.getVariantMap());
        }
        else if (!val.hasAnyValue()) {
            out.writeByte(TYPE_NULL);
        }
        else {
            assert(false);
        }
    }
    
    void writeCollection(char* fieldName, const std::vector<GridClientVariant> &val) override {
        writeCollection(val);
    }

    void writeMap(char* fieldName, const std::unordered_map<GridClientVariant, GridClientVariant> &map) override {
        writeMap(map);
    }

	std::vector<int8_t> bytes() {
		return out.bytes();
	}

private:
    void writeCollection(const std::vector<GridClientVariant>& col) {
        out.writeByte(OBJECT_TYPE_OBJECT);

        out.writeInt32(col.size());

        for (auto iter = col.begin(); iter != col.end(); ++iter) {
            GridClientVariant variant = *iter;

            writeVariant(nullptr, variant);
        }
    }

    void writeMap(const std::unordered_map<GridClientVariant, GridClientVariant>& map) {
        out.writeByte(OBJECT_TYPE_OBJECT);

        out.writeInt32(map.size());

        for (auto iter = map.begin(); iter != map.end(); ++iter) {
            GridClientVariant key = iter->first;
            GridClientVariant val = iter->second;

            writeVariant(nullptr, key);
            writeVariant(nullptr, val);
        }
    }

    PortableOutput out;
};

class PortableInput {
public:
    PortableInput(std::vector<int8_t>& data) : bytes(data), pos(0) {
    }

    int8_t readByte() {
        checkAvailable(1);

        return bytes[pos++];
    }

    int16_t readInt16() {
        checkAvailable(2);

        int32_t res = 0;

        memcpy(&res, bytes.data() + pos, 2);

        pos += 2;

        return res;
    }

    int32_t readInt32() {
        checkAvailable(4);

        int32_t res = 0;

        memcpy(&res, bytes.data() + pos, 4);

        pos += 4;

        return res;
    }

    int64_t readInt64() {
        checkAvailable(8);

        int64_t res = 0;

        memcpy(&res, bytes.data() + pos, 8);

        pos += 8;

        return res;
    }

    float readFloat() {
        checkAvailable(4);

        float res = 0;

        memcpy(&res, bytes.data() + pos, 4);

        pos += 4;

        return res;
    }

    double readDouble() {
        checkAvailable(8);

        double res = 0;

        memcpy(&res, bytes.data() + pos, 8);

        pos += 4;

        return res;
    }

    std::vector<int8_t> readBytes(int32_t size) {
        checkAvailable(size);

        std::vector<int8_t> vec;

        vec.insert(vec.end(), bytes.data() + pos, bytes.data() + pos + size);

        pos += size;

        return vec;
    }

private:
    void checkAvailable(size_t cnt) {
        assert(pos + cnt <= bytes.size());
    }
    
    int32_t pos;

    std::vector<int8_t>& bytes;
};

class GridPortableReaderImpl : GridPortableReader {
public:
    GridPortableReaderImpl(std::vector<int8_t>& data) : in(data) {
    }

    GridPortable* readPortable() {
        GG_LOG_DEBUG("Start read portable");

        int8_t type = in.readByte();

        assert(type == OBJECT_TYPE_OBJECT);

        int32_t typeId = in.readInt32();

        GG_LOG_DEBUG("Portable type %d", typeId);

        GridPortable* portable = createPortable(typeId, *this);

        GG_LOG_DEBUG("End read portable");

        return portable;
    }

    int8_t readByte(char* fieldName) override {
        GG_LOG_DEBUG("Read byte %s", fieldName);

        return in.readByte();
    }

    int16_t readInt16(char* fieldName) override {
        GG_LOG_DEBUG("Read int16 %s", fieldName);

        return in.readInt32();
    }

    int32_t readInt32(char* fieldName) override {
        GG_LOG_DEBUG("Read int32 %s", fieldName);

        return in.readInt32();
    }

    int64_t readInt64(char* fieldName) override {
        GG_LOG_DEBUG("Read int64 %s", fieldName);

        return in.readInt64();
    }

    float readFloat(char* fieldName) override {
        GG_LOG_DEBUG("Read float %s", fieldName);

        return in.readFloat();
    }

    double readDouble(char* fieldName) override {
        GG_LOG_DEBUG("Read double %s", fieldName);

        return in.readDouble();
    }

    std::vector<int8_t> readBytes(char* fieldName) override {
        GG_LOG_DEBUG("Read bytes %s", fieldName);

        int32_t size = in.readInt32();

        if (size > 0)
            return in.readBytes(size);

        return std::vector<int8_t>();
    }

    std::string readString(char* fieldName) override {
        GG_LOG_DEBUG("Read string %s", fieldName);

        int size = in.readInt32();

        if (size == -1)
            return std::string();

        std::vector<int8_t> bytes = in.readBytes(size);

        return std::string((char*)bytes.data(), size);
    }

    bool readBool(char* fieldName) override {
        GG_LOG_DEBUG("Read bool %s", fieldName);

        int8_t val = in.readByte();

        return val == 0 ? false : true;
    }

    boost::optional<GridClientUuid> readUuid(char* fieldName) override {
        GG_LOG_DEBUG("Read uuid %s", fieldName);

        boost::optional<GridClientUuid> res;

        if (in.readByte() != 0) {
            int64_t mostSignificantBits = in.readInt64();
            int64_t leastSignificantBits = in.readInt64();

            res = GridClientUuid(mostSignificantBits, leastSignificantBits);
        }

        return res;
    }

    GridClientVariant readVariant(char* fieldName) override {
        GG_LOG_DEBUG("Read variant %s", fieldName);

        int8_t type = in.readByte();

        GG_LOG_DEBUG("Variant type %d", type);

        switch (type) {
            case TYPE_NULL:
                GG_LOG_DEBUG("Read null");

                return GridClientVariant();

            case TYPE_INT:
                GG_LOG_DEBUG("Read int");

                return GridClientVariant(in.readInt32());

            case TYPE_BOOLEAN:
                GG_LOG_DEBUG("Read boolean");

                return GridClientVariant(in.readByte() != 0);

            case TYPE_STRING:
                GG_LOG_DEBUG("Read string");

                return GridClientVariant(readString(nullptr));

            case TYPE_LONG:
                GG_LOG_DEBUG("Read long");

                return GridClientVariant(in.readInt64());

            case TYPE_UUID: {
                GG_LOG_DEBUG("Read uuid");

                if (in.readByte() == 0)
                    return GridClientVariant();

                int64_t mostSignificantBits = in.readInt64();
                int64_t leastSignificantBits = in.readInt64();

                return GridClientVariant(GridClientUuid(mostSignificantBits, leastSignificantBits));
            }

            case TYPE_LIST:
                GG_LOG_DEBUG("Read list");

                return GridClientVariant(readCollection());

            case TYPE_MAP:
                GG_LOG_DEBUG("Read map");

                return GridClientVariant(readMap());

            case TYPE_USER_OBJECT:
                GG_LOG_DEBUG("Read portable");

                return GridClientVariant(readPortable());

            default:
                assert(false);
        }

        return GridClientVariant();
    }

    std::vector<GridClientVariant> readCollection(char* fieldName) override {
        GG_LOG_DEBUG("Read collection %s", fieldName);

        return readCollection();
    }

    std::unordered_map<GridClientVariant, GridClientVariant> readMap(char* fieldName) override {
        GG_LOG_DEBUG("Read map %s", fieldName);

        return readMap();
    }

private:
    std::vector<GridClientVariant> readCollection() {
        GG_LOG_DEBUG("Read collection");

        int8_t type = in.readByte();

        assert(type == OBJECT_TYPE_OBJECT);

        int32_t size = in.readInt32();

        GG_LOG_DEBUG("Collection size %d", size);

        if (size == -1)
            return std::vector<GridClientVariant>();

        std::vector<GridClientVariant> vec;

        for (int i = 0; i < size; i++) {
            GG_LOG_DEBUG("Read collection element");

            vec.push_back(readVariant(nullptr));
        }

        GG_LOG_DEBUG("End read collection");

        return vec;
    }

    std::unordered_map<GridClientVariant, GridClientVariant> readMap() {
        GG_LOG_DEBUG("Read map");

        int8_t type = in.readByte();

        if (type == OBJECT_TYPE_NULL)
            return std::unordered_map<GridClientVariant, GridClientVariant>();

        assert(type == OBJECT_TYPE_OBJECT);

        int32_t size = in.readInt32();

        GG_LOG_DEBUG("Map size %d", size);

        if (size == -1)
            return std::unordered_map<GridClientVariant, GridClientVariant>();
        
        std::unordered_map<GridClientVariant, GridClientVariant> map;

        for (int i = 0; i < size; i++) {
            GG_LOG_DEBUG("Read map key");

            GridClientVariant key = readVariant(nullptr);
            
            GG_LOG_DEBUG("Read map value");

            GridClientVariant val = readVariant(nullptr);

            map[key] = val;
        }

        GG_LOG_DEBUG("End read map");

        return map;
    }
    
    PortableInput in;
};

class GridPortableMarshaller {
public:
    std::vector<int8_t> marshal(GridPortable& portable) {
		GridPortableWriterImpl writer;

		writer.writePortable(portable);

		return writer.bytes();
	}

    template<typename T>
    T* unmarshal(std::vector<int8_t>& data) {
		GridPortableReaderImpl reader(data);

        return static_cast<T*>(reader.readPortable());
    }

    void parseResponse(GridClientResponse* msg, GridClientMessageTopologyResult& resp) {
        GridClientVariant res = msg->getResult();

        assert(res.hasVariantVector());

        std::vector<GridClientVariant> vec = res.getVariantVector();

        std::vector<GridClientNode> nodes;

        for (auto iter = vec.begin(); iter != vec.end(); ++iter) {
            GridClientVariant nodeVariant = *iter;

            GridClientNodeBean* nodeBean = nodeVariant.getPortable<GridClientNodeBean>();
        
            nodes.push_back(nodeBean->createNode());

            delete nodeBean;
        }

        resp.setNodes(nodes);
    }

    void parseResponse(GridClientResponse* msg, GridClientMessageLogResult& resp) {
        GridClientVariant res = msg->getResult();

        assert(res.hasVariantVector());

        std::vector<GridClientVariant> vec = res.getVariantVector();

        std::vector<std::string> lines;
        
        for (auto iter = vec.begin(); iter != vec.end(); ++iter) {
            GridClientVariant line = *iter;

            assert(line.hasString());

            lines.push_back(line.getString());
        }

        resp.lines(lines);
    }

    void parseResponse(GridClientResponse* msg, GridClientMessageCacheGetResult& resp) {
        GridClientVariant res = msg->getResult();

        if (res.hasVariantMap()) {
            TCacheValuesMap keyValues = res.getVariantMap();
            
            resp.setCacheValues(keyValues);
        }
        else {
            TCacheValuesMap keyValues;

            keyValues.insert(std::make_pair(GridClientVariant(), res));

            resp.setCacheValues(keyValues);
        }
    }

    void parseResponse(GridClientResponse* msg, GridClientMessageCacheModifyResult& resp) {
        GridClientVariant res = msg->getResult();

        if (res.hasBool())
            resp.setOperationResult(res.getBool());
        else
            resp.setOperationResult(false);
    }

    void parseResponse(GridClientResponse* msg, GridClientMessageCacheMetricResult& resp) {
        GridClientVariant res = msg->getResult();

        assert(res.hasPortable());

        TCacheMetrics metrics;

        // TODO: 8536.

        resp.setCacheMetrics(metrics);
    }
    
    void parseResponse(GridClientResponse* msg, GridClientMessageTaskResult& resp) {
        GridClientVariant res = msg->getResult();

        if (res.hasPortable()) {
            assert(res.getPortable()->typeId() == -8);

            GridClientTaskResultBean* resBean = res.getPortable<GridClientTaskResultBean>();

            resp.setTaskResult(resBean->res);

            delete resBean;
        }
    }
};

#endif
