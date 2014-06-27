/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include "gridgain/impl/gridclientdataprojection.hpp"
#include "gridgain/impl/projectionclosure/gridclientmessageprojectionclosure.hpp"
#include "gridgain/impl/hash/gridclientvarianthasheableobject.hpp"
#include "gridgain/impl/filter/gridclientnodefilter.hpp"
#include "gridgain/impl/utils/gridfutureimpl.hpp"
#include "gridgain/gridclientexception.hpp"
#include "gridgain/gridclientdatametrics.hpp"
#include "gridgain/impl/cmd/gridclientmessagecacherequestcommand.hpp"
#include "gridgain/impl/cmd/gridclientmessagecachemodifyresult.hpp"
#include "gridgain/impl/cmd/gridclientcommandexecutor.hpp"

#include "gridgain/impl/utils/gridclientlog.hpp"

using namespace std;

class CacheRequestProjectionClosure: public ClientMessageProjectionClosure {
public:
    CacheRequestProjectionClosure(const char* clientId, GridCacheRequestCommand& cacheCmd)
            : ClientMessageProjectionClosure(clientId), cmd(cacheCmd) {
    }

    CacheRequestProjectionClosure(GridClientUuid & clientId, GridCacheRequestCommand& cacheCmd)
            : ClientMessageProjectionClosure(clientId), cmd(cacheCmd) {
    }

    CacheRequestProjectionClosure(std::string clientId, GridCacheRequestCommand& cacheCmd)
            : ClientMessageProjectionClosure(clientId.c_str()), cmd(cacheCmd) {
    }

    virtual void apply(TGridClientNodePtr node, GridClientSocketAddress connParams, GridClientCommandExecutor& cmdExecutor) {
        fillRequestHeader(cmd, node);

        cmdExecutor.executeModifyCacheCmd(connParams, cmd, rslt);
    }

    const GridClientMessageCacheModifyResult& getResult() const {
        return rslt;
    }

private:
    /** Cache command. */
    GridCacheRequestCommand& cmd;

    /** Command result. */
    GridClientMessageCacheModifyResult rslt;
};

class CacheMetricProjectionClosure: public ClientMessageProjectionClosure {
public:
    CacheMetricProjectionClosure(const char* clientId, GridCacheRequestCommand& cacheCmd)
            : ClientMessageProjectionClosure(clientId), cmd(cacheCmd) {
    }

    CacheMetricProjectionClosure(std::string clientId, GridCacheRequestCommand& cacheCmd)
            : ClientMessageProjectionClosure(clientId.c_str()), cmd(cacheCmd) {
    }

    virtual void apply(TGridClientNodePtr node, GridClientSocketAddress connParams, GridClientCommandExecutor& cmdExecutor) {
        fillRequestHeader(cmd, node);

        cmdExecutor.executeGetCacheMetricsCmd(connParams, cmd, rslt);
    }

    const GridClientMessageCacheMetricResult& getResult() const {
        return rslt;
    }

private:
    GridCacheRequestCommand& cmd;
    GridClientMessageCacheMetricResult rslt;
};

class CacheGetProjectionClosure: public ClientMessageProjectionClosure {
public:
    CacheGetProjectionClosure(const char* clientId, GridCacheRequestCommand& cacheCmd)
            : ClientMessageProjectionClosure(clientId), cmd(cacheCmd) {

    }

    CacheGetProjectionClosure(std::string clientId, GridCacheRequestCommand& cacheCmd)
            : ClientMessageProjectionClosure(clientId.c_str()), cmd(cacheCmd) {

    }

    virtual void apply(TGridClientNodePtr node, GridClientSocketAddress connParams, GridClientCommandExecutor& cmdExecutor) {
        fillRequestHeader(cmd, node);

        cmdExecutor.executeGetCacheCmd(connParams, cmd, rslt);
    }

    const GridClientMessageCacheGetResult& getResult() const {
        return rslt;
    }

private:
    GridCacheRequestCommand& cmd;
    GridClientMessageCacheGetResult rslt;
};

GridClientDataProjectionImpl::GridClientDataProjectionImpl(
    TGridClientSharedDataPtr sharedData,
    GridClientProjectionListener& prjLsnr,
    const string& cacheName,
    TGridClientNodePredicatePtr filter,
    TGridThreadPoolPtr& threadPool,
    const std::set<GridClientCacheFlag>& flags)
    : GridClientProjectionImpl(sharedData, prjLsnr, filter), prjCacheName(cacheName), invalidated(false),
    threadPool(threadPool), prjFlags(flags) {
}

string GridClientDataProjectionImpl::cacheName() const {
    return prjCacheName;
}

TGridClientDataPtr GridClientDataProjectionImpl::pinNodes(const TGridClientNodeList& nodes) {
    TGridClientNodePredicatePtr pinnedNodeFilter(new GridClientNodeUuidFilter(nodes));

    GridClientCompositeFilter<GridClientNode>* compFilter = new GridClientCompositeFilter<GridClientNode>();

    compFilter->add(filter);

    compFilter->add(pinnedNodeFilter);

    TGridClientNodePredicatePtr clientDataFilter(compFilter);

    GridClientData* gcdp =
        new GridClientDataProjectionImpl(sharedData, prjLsnr, prjCacheName, clientDataFilter, threadPool, prjFlags);

    TGridClientDataPtr clientDataPtr(gcdp);

    subProjections.push_back(clientDataPtr);

    return clientDataPtr;
}

bool GridClientDataProjectionImpl::put(const GridClientVariant& key, const GridClientVariant& val) {
    if (invalidated) throw GridClientClosedException();

    GridCacheRequestCommand cmd(GridCacheRequestCommand::PUT);

    cmd.setKey(key);
    cmd.setValue(val);
    cmd.setCacheName(prjCacheName);
    cmd.setFlags(prjFlags);

    CacheRequestProjectionClosure c(clientUniqueUuid(), cmd);

    this->withReconnectHandling(c, prjCacheName, GridClientVariantHasheableObject(key));

    return c.getResult().isSuccess();
}

TGridBoolFuturePtr GridClientDataProjectionImpl::putAsync(const GridClientVariant& key, const GridClientVariant& val) {
    if (invalidated) return TGridBoolFuturePtr(new GridBoolFailFutureImpl<GridClientClosedException>());

    GridBoolFutureImpl* fut = new GridBoolFutureImpl(threadPool);
    TGridBoolFuturePtr res(fut);

    boost::packaged_task<bool> pt(boost::bind(&GridClientDataProjectionImpl::put, this, key, val));

    fut->task(pt);

    return res;
}

bool GridClientDataProjectionImpl::putAll(const TGridClientVariantMap& entries) {
    if (invalidated) throw GridClientClosedException();

    GridCacheRequestCommand cmd(GridCacheRequestCommand::PUT_ALL);

    cmd.setValues(entries);
    cmd.setCacheName(prjCacheName);
    cmd.setFlags(prjFlags);

    CacheRequestProjectionClosure c(clientUniqueId(), cmd);

    if (entries.size() > 0)
        this->withReconnectHandling(c, prjCacheName, GridClientVariantHasheableObject(entries.begin()->first));

    return c.getResult().isSuccess();
}

TGridBoolFuturePtr GridClientDataProjectionImpl::putAllAsync(const TGridClientVariantMap& entries) {
    if (invalidated) return TGridBoolFuturePtr(new GridBoolFailFutureImpl<GridClientClosedException>());

    GridBoolFutureImpl* fut = new GridBoolFutureImpl(threadPool);
    TGridBoolFuturePtr res(fut);

    boost::packaged_task<bool> pt(boost::bind(&GridClientDataProjectionImpl::putAll, this, entries));

    fut->task(pt);

    return res;
}

bool GridClientDataProjectionImpl::remove(const GridClientVariant& key) {
    if (invalidated) throw GridClientClosedException();

    GridCacheRequestCommand cmd(GridCacheRequestCommand::RMV);

    cmd.setCacheName(prjCacheName);
    cmd.setKey(key);
    cmd.setFlags(prjFlags);

    CacheRequestProjectionClosure c(clientUniqueId(), cmd);

    this->withReconnectHandling(c, prjCacheName, GridClientVariantHasheableObject(key));

    if (!c.getResult().isSuccess())
        return false;

    return c.getResult().getOperationResult();
}

TGridBoolFuturePtr GridClientDataProjectionImpl::removeAsync(const GridClientVariant& key) {
    if (invalidated) return TGridBoolFuturePtr(new GridBoolFailFutureImpl<GridClientClosedException>());

    GridBoolFutureImpl* fut = new GridBoolFutureImpl(threadPool);
    TGridBoolFuturePtr res(fut);

    boost::packaged_task<bool> pt(boost::bind(&GridClientDataProjectionImpl::remove, this, key));

    fut->task(pt);

    return res;
}

bool GridClientDataProjectionImpl::removeAll(const TGridClientVariantSet& keys) {
    if (invalidated) throw GridClientClosedException();

    GridCacheRequestCommand::TKeyValueMap keyValues;
    GridCacheRequestCommand cmd(GridCacheRequestCommand::RMV_ALL);

    for (size_t i = 0; i < keys.size(); i++)
        keyValues[keys[i]] = GridClientVariant();

    cmd.setCacheName(prjCacheName);
    cmd.setValues(keyValues);
    cmd.setFlags(prjFlags);

    CacheRequestProjectionClosure c(clientUniqueId(), cmd);

    if (keyValues.size() > 0)
        this->withReconnectHandling(c, prjCacheName, GridClientVariantHasheableObject(keyValues.begin()->first));
    else
        this->withReconnectHandling(c);

    return c.getResult().isSuccess();
}

TGridBoolFuturePtr GridClientDataProjectionImpl::removeAllAsync(const TGridClientVariantSet& keys) {
    if (invalidated) return TGridBoolFuturePtr(new GridBoolFailFutureImpl<GridClientClosedException>());

    GridBoolFutureImpl* fut = new GridBoolFutureImpl(threadPool);
    TGridBoolFuturePtr res(fut);

    boost::packaged_task<bool> pt(boost::bind(&GridClientDataProjectionImpl::removeAll, this, keys));

    fut->task(pt);

    return res;
}

bool GridClientDataProjectionImpl::replace(const GridClientVariant& key, const GridClientVariant& val) {
    if (invalidated) throw GridClientClosedException();

    GridCacheRequestCommand cmd(GridCacheRequestCommand::REPLACE);

    cmd.setCacheName(prjCacheName);
    cmd.setKey(key);
    cmd.setValue(val);
    cmd.setFlags(prjFlags);

    CacheRequestProjectionClosure c(clientUniqueId(), cmd);

    withReconnectHandling(c, prjCacheName, GridClientVariantHasheableObject(key));

    if (!c.getResult().isSuccess())
        return false;

    return c.getResult().getOperationResult();
}

TGridBoolFuturePtr GridClientDataProjectionImpl::replaceAsync(const GridClientVariant& key,
        const GridClientVariant& val) {
    if (invalidated) return TGridBoolFuturePtr(new GridBoolFailFutureImpl<GridClientClosedException>());

    GridBoolFutureImpl* fut = new GridBoolFutureImpl(threadPool);
    TGridBoolFuturePtr res(fut);

    boost::packaged_task<bool> pt(boost::bind(&GridClientDataProjectionImpl::replace, this, key, val));

    fut->task(pt);

    return res;
}

bool GridClientDataProjectionImpl::cas(const GridClientVariant& key, const GridClientVariant& val1,
        const GridClientVariant& val2) {
    if (invalidated) throw GridClientClosedException();

    GridCacheRequestCommand cmd(GridCacheRequestCommand::CAS);

    cmd.setCacheName(prjCacheName);
    cmd.setKey(key);
    cmd.setValue(val1);
    cmd.setValue2(val2);
    cmd.setFlags(prjFlags);

    CacheRequestProjectionClosure c(clientUniqueId(), cmd);

    this->withReconnectHandling(c, prjCacheName, GridClientVariantHasheableObject(key));

    if (!c.getResult().isSuccess())
        return false;

    return c.getResult().getOperationResult();
}

TGridBoolFuturePtr GridClientDataProjectionImpl::casAsync(const GridClientVariant& key, const GridClientVariant& val1,
        const GridClientVariant& val2) {
    if (invalidated) return TGridBoolFuturePtr(new GridBoolFailFutureImpl<GridClientClosedException>());

    GridBoolFutureImpl* fut = new GridBoolFutureImpl(threadPool);
    TGridBoolFuturePtr res(fut);

    boost::packaged_task<bool> pt(boost::bind(&GridClientDataProjectionImpl::cas, this, key, val1, val2));

    fut->task(pt);

    return res;
}

GridClientVariant GridClientDataProjectionImpl::get(const GridClientVariant& key) {
    if (invalidated) throw GridClientClosedException();

    GridCacheRequestCommand cmd(GridCacheRequestCommand::GET);

    cmd.setKey(key);
    cmd.setCacheName(prjCacheName);
    cmd.setFlags(prjFlags);

    CacheGetProjectionClosure c(clientUniqueId(), cmd);

    this->withReconnectHandling(c, prjCacheName, GridClientVariantHasheableObject(key));

    const TCacheValuesMap& res = c.getResult().getCacheValue();

    if (res.size() == 0)
        return GridClientVariant();

    TCacheValuesMap::const_iterator iter = res.begin();

    return iter->second;
}

TGridClientFutureVariant GridClientDataProjectionImpl::getAsync(const GridClientVariant& key) {
    if (invalidated) return TGridClientFutureVariant(
            new GridFailFutureImpl<GridClientVariant, GridClientClosedException>());

    GridFutureImpl<GridClientVariant>* fut = new GridFutureImpl<GridClientVariant>(threadPool);
    TGridClientFutureVariant res(fut);

    boost::packaged_task<GridClientVariant> pt(boost::bind(&GridClientDataProjectionImpl::get, this, key));

    fut->task(pt);

    return res;
}

TGridClientVariantMap GridClientDataProjectionImpl::getAll(const TGridClientVariantSet& keys) {
    if (invalidated) throw GridClientClosedException();

    TGridClientVariantMap ret;

    GridCacheRequestCommand cmd(GridCacheRequestCommand::GET_ALL);
    GridCacheRequestCommand::TKeyValueMap keyValues;

    for (size_t i = 0; i < keys.size(); i++)
        keyValues[keys[i]] = GridClientVariant();

    cmd.setCacheName(prjCacheName);
    cmd.setValues(keyValues);
    cmd.setFlags(prjFlags);

    CacheGetProjectionClosure c(clientUniqueId(), cmd);

    if (keyValues.size() > 0)
        this->withReconnectHandling(c, prjCacheName, GridClientVariantHasheableObject(keyValues.begin()->first));
    else
        this->withReconnectHandling(c);

    const TCacheValuesMap& res = c.getResult().getCacheValue();

    GG_LOG_DEBUG("Get ALL result size: %d", res.size());

    for (auto iter = res.begin(); iter != res.end(); ++iter)
        ret[iter->first] = iter->second;

    return ret;
}

TGridClientFutureVariantMap GridClientDataProjectionImpl::getAllAsync(const TGridClientVariantSet& keys) {
    if (invalidated) 
        return TGridClientFutureVariantMap(new GridFailFutureImpl<TGridClientVariantMap, GridClientClosedException>());

    GridFutureImpl<TGridClientVariantMap>* fut = new GridFutureImpl<TGridClientVariantMap>(threadPool);
    TGridClientFutureVariantMap res(fut);

    boost::packaged_task<TGridClientVariantMap> pt(boost::bind(&GridClientDataProjectionImpl::getAll, this, keys));

    fut->task(pt);

    return res;
}

static int64_t doGetValue(const TCacheMetrics& metricsMap, std::string name) {
    GridClientVariant v(name);

    TCacheMetrics::const_iterator it = metricsMap.find(name);

    assert(it != metricsMap.end());

    int64_t res = 0;

    if (it != metricsMap.end()) {
        GridClientVariant var = it->second;

        assert(var.hasInt() || var.hasLong());

        if (var.hasInt())
            res = var.getInt();
        else
            res = var.getLong();
    }

    return res;
}

static void fillMetricsBean(const TCacheMetrics& metricsMap, GridClientDataMetrics& metrics) {
    metrics.createTime(doGetValue(metricsMap, "createTime"));
    metrics.readTime(doGetValue(metricsMap, "readTime"));
    metrics.writeTime(doGetValue(metricsMap, "writeTime"));
    metrics.reads((int32_t)doGetValue(metricsMap, "reads"));
    metrics.writes((int32_t)doGetValue(metricsMap, "writes"));
    metrics.hits((int32_t)doGetValue(metricsMap, "hits"));
    metrics.misses((int32_t)doGetValue(metricsMap, "misses"));
}

GridClientDataMetrics GridClientDataProjectionImpl::metrics() {
    if (invalidated) 
        throw GridClientClosedException();

    GridCacheRequestCommand cmd(GridCacheRequestCommand::METRICS);

    cmd.setCacheName(prjCacheName);
    cmd.setFlags(prjFlags);

    CacheMetricProjectionClosure c(clientUniqueId(), cmd);

    this->withReconnectHandling(c);

    GridClientDataMetrics metrics;

    fillMetricsBean(c.getResult().getCacheMetrics(), metrics);

    return metrics;
}

TGridClientFutureDataMetrics GridClientDataProjectionImpl::metricsAsync() {
    if (invalidated) 
        return TGridClientFutureDataMetrics(new GridFailFutureImpl<GridClientDataMetrics, GridClientClosedException>());

    GridFutureImpl<GridClientDataMetrics>* fut = new GridFutureImpl<GridClientDataMetrics>(threadPool);
    TGridClientFutureDataMetrics res(fut);

    boost::packaged_task<GridClientDataMetrics> pt(
            boost::bind(
                    static_cast<GridClientDataMetrics (GridClientDataProjectionImpl::*)(
                            void)> (&GridClientDataProjectionImpl::metrics), this));

    fut->task(pt);

    return res;
}

GridClientUuid GridClientDataProjectionImpl::affinity(const GridClientVariant& key) {
    return affinityNode(prjCacheName, GridClientVariantHasheableObject(key))->getNodeId();
}

std::set<GridClientCacheFlag> GridClientDataProjectionImpl::flags() {
    return prjFlags;
}

TGridClientDataPtr GridClientDataProjectionImpl::flagsOn(const std::set<GridClientCacheFlag>& flags) {
    std::set<GridClientCacheFlag> flags0 = prjFlags;

    flags0.insert(flags.begin(), flags.end());

    GridClientData* gcdp =
            new GridClientDataProjectionImpl(sharedData, prjLsnr, prjCacheName, filter, threadPool, flags0);

    TGridClientDataPtr clientDataPtr(gcdp);

    subProjections.push_back(clientDataPtr);

    return clientDataPtr;
}

TGridClientDataPtr GridClientDataProjectionImpl::flagsOff(const std::set<GridClientCacheFlag>& flags) {
    std::set<GridClientCacheFlag> flags0 = prjFlags;

    for (auto i = flags.begin(); i != flags.end(); i++)
        flags0.erase(*i);

    GridClientData* gcdp =
            new GridClientDataProjectionImpl(sharedData, prjLsnr, prjCacheName, filter, threadPool, flags0);

    TGridClientDataPtr clientDataPtr(gcdp);

    subProjections.push_back(clientDataPtr);

    return clientDataPtr;
}


/**
 * Invalidates this data instance. This is done by the client to indicate
 * that is has been stopped. After this call, all interface methods
 * will throw GridClientClosedException.
 */
void GridClientDataProjectionImpl::invalidate() {
    invalidated = true;
}
