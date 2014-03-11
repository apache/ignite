/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include <set>
#include <string>
#include <time.h>

#include "gridgain/gridclientuuid.hpp"
#include "gridgain/gridclientexception.hpp"
#include "gridgain/impl/hash/gridclientvarianthasheableobject.hpp"
#include "gridgain/impl/gridclientcomputeprojection.hpp"
#include "gridgain/impl/filter/gridclientnodefilter.hpp"
#include "gridgain/impl/projectionclosure/gridclientmessageprojectionclosure.hpp"
#include "gridgain/impl/filter/gridclientfilter.hpp"
#include "gridgain/impl/utils/gridfutureimpl.hpp"
#include "gridgain/impl/cmd/gridclientmessagetaskrequestcommand.hpp"
#include "gridgain/impl/cmd/gridclientmessagetaskresult.hpp"
#include "gridgain/impl/cmd/gridclientcommandexecutor.hpp"

using namespace std;

class TaskProjectionClosure: public ClientMessageProjectionClosure {
public:
    TaskProjectionClosure(const string& clientId, const string& taskName, const GridTaskRequestCommand& command)
            : ClientMessageProjectionClosure(clientId), cmd(command) {
    }

    virtual void apply(TGridClientNodePtr node, GridClientSocketAddress connParams, GridClientCommandExecutor& cmdExecutor) {
        fillRequestHeader(cmd, node);

        cmdExecutor.executeTaskCmd(connParams, cmd, rslt);
    }

    const GridClientMessageTaskResult& getResult() const {
        return rslt;
    }

private:
    GridTaskRequestCommand cmd;
    GridClientMessageTaskResult rslt;
};

class LogProjectionClosure: public ClientMessageProjectionClosure {
public:
    LogProjectionClosure(string clientId, const GridLogRequestCommand& command)
            : ClientMessageProjectionClosure(clientId), cmd(command) {
    }

    virtual void apply(TGridClientNodePtr node, GridClientSocketAddress connParams, GridClientCommandExecutor& cmdExecutor) {
        fillRequestHeader(cmd, node);

        cmdExecutor.executeLogCmd(connParams, cmd, rslt);
    }

    const GridClientMessageLogResult& getResult() const {
        return rslt;
    }

private:
    GridLogRequestCommand cmd;
    GridClientMessageLogResult rslt;
};

template <class T> class GridClientPredicateLambdaWrapper: public GridClientPredicate<T> {
public:
    GridClientPredicateLambdaWrapper(std::function<bool (const T&)> & lambda): lambda(lambda) {}

    virtual ~GridClientPredicateLambdaWrapper() {}

    virtual bool apply(const T& e) const {
        return lambda(e);
    }

private:
    std::function<bool (const T&)> lambda;
};

GridClientComputeProjectionImpl::GridClientComputeProjectionImpl(TGridClientSharedDataPtr pData,
        GridClientProjectionListener& prjLsnr,
        TGridClientNodePredicatePtr filter, TGridClientLoadBalancerPtr balancer, TGridThreadPoolPtr& threadPool)
        : GridClientProjectionImpl(pData, prjLsnr, filter), invalidated(false), threadPool(threadPool) {

    loadBalancer_ = balancer;
}

/**
 * Creates a projection that will communicate only with specified remote node.
 * <p>
 * If current projection is dynamic projection, then this method will check is passed node is in topology.
 * If any filters were specified in current topology, this method will check if passed node is accepted by
 * the filter. If current projection was restricted to any subset of nodes, this method will check if
 * passed node is in that subset. If any of the checks fails an exception will be thrown.
 *
 * @param node Single node to which this projection will be restricted.
 * @return Resulting static projection that is bound to a given node.
 * @throw GridClientException If resulting projection is empty.
 */
TGridClientComputePtr GridClientComputeProjectionImpl::projection(const GridClientNode& node) {
    TGridClientNodePredicatePtr filter(new GridClientNodeUuidFilter(node));

    return makeProjection(filter, TGridClientLoadBalancerPtr());
}

/**
 * Creates a projection that will communicate only with specified remote nodes. For any particular call
 * a node to communicate will be selected with passed balancer..
 * <p>
 * If current projection is dynamic projection, then this method will check is passed nodes are in topology.
 * If any filters were specified in current topology, this method will check if passed nodes are accepted by
 * the filter. If current projection was restricted to any subset of nodes, this method will check if
 * passed nodes are in that subset (i.e. calculate the intersection of two collections).
 * If any of the checks fails an exception will be thrown.
 *
 * @param nodes Collection of nodes to which this projection will be restricted.
 * @return Resulting static projection that is bound to a given nodes.
 * @throw GridClientException If resulting projection is empty.
 */
TGridClientComputePtr GridClientComputeProjectionImpl::projection(const TGridClientNodeList& nodes) {
    TGridClientNodePredicatePtr filter(new GridClientNodeUuidFilter(nodes));

    return makeProjection(filter, TGridClientLoadBalancerPtr());
}

/**
 * Creates a projection that will communicate only with nodes that are accepted by the passed filter.
 * <p>
 * If current projection is dynamic projection, then filter will be applied to the most relevant
 * topology snapshot every time a node to communicate is selected. If current projection is a static projection,
 * then resulting projection will only be restricted to nodes that were in parent projection and were
 * accepted by the passed filter. If any of the checks fails an exception will be thrown.
 *
 * @param filter Filter that will select nodes for projection.
 * @return Resulting projection (static or dynamic, depending in parent projection type).
 * @throws GridClientException If resulting projection is empty.
 */
TGridClientComputePtr GridClientComputeProjectionImpl::projection(TGridClientNodePredicatePtr filter) {
    return makeProjection(filter, TGridClientLoadBalancerPtr());
}

TGridClientComputePtr GridClientComputeProjectionImpl::projection(std::function<bool(const GridClientNode&)> & filter) {
    return makeProjection(
        TGridClientNodePredicatePtr(
            new GridClientPredicateLambdaWrapper<GridClientNode>(filter)),
        TGridClientLoadBalancerPtr());
}

TGridClientComputePtr GridClientComputeProjectionImpl::projection(const TGridClientNodePredicatePtr filter,
        TGridClientLoadBalancerPtr balancer) {
    return makeProjection(filter, balancer);
}

TGridClientComputePtr GridClientComputeProjectionImpl::projection(std::function<bool (const GridClientNode&)> & filter,
        TGridClientLoadBalancerPtr balancer) {
    return makeProjection(
        TGridClientNodePredicatePtr(
            new GridClientPredicateLambdaWrapper<GridClientNode>(filter)),
        balancer);
}

TGridClientComputePtr GridClientComputeProjectionImpl::projection(const TGridClientNodeList& nodes,
        TGridClientLoadBalancerPtr balancer) {
    TGridClientNodePredicatePtr filter(new GridClientNodeUuidFilter(nodes));

    return makeProjection(filter, balancer);
}

/**
 * Gets balancer used by projection.
 *
 * @return Instance of {@link GridClientLoadBalancer}.
 */
TGridClientLoadBalancerPtr GridClientComputeProjectionImpl::balancer() const {
    return loadBalancer();
}

/**
 * Executes task.
 *
 * @param taskName Task name or task class name.
 * @param taskArg Optional task argument.
 * @return Task execution result.
 * @throw GridClientException In case of error.
 * @throw GridClientClosedException If client was closed manually.
 * @throw GridServerUnreachableException If none of the servers can be reached.
 */
GridClientVariant GridClientComputeProjectionImpl::execute(const string& taskName,
        const GridClientVariant& taskArg) {
    if (invalidated)
        throw GridClientClosedException();

    GridTaskRequestCommand cmd;

    cmd.setTaskName(taskName);
    cmd.setArg(taskArg);

    TaskProjectionClosure c(clientUniqueId(), taskName, cmd);

    this->withReconnectHandling(c);

    GridClientMessageTaskResult res = c.getResult();

    return GridClientVariant(res.getTaskResult());
}

/**
 * Asynchronously executes task.
 *
 * @param taskName Task name or task class name.
 * @param taskArg Optional task argument.
 * @return Future.
 */
TGridClientFutureVariant GridClientComputeProjectionImpl::executeAsync(const string& taskName,
        const GridClientVariant& taskArg) {
    if (invalidated)
        return TGridClientFutureVariant(
            new GridFailFutureImpl<GridClientVariant, GridClientClosedException>());

    GridFutureImpl<GridClientVariant>* fut = new GridFutureImpl<GridClientVariant>(threadPool);
    TGridClientFutureVariant res(fut);

    boost::packaged_task<GridClientVariant> pt(
            boost::bind(&GridClientComputeProjectionImpl::execute, this, taskName, taskArg));

    fut->task(pt);

    return res;
}

/**
 * Executes task using cache affinity key for routing. This way the task will start executing
 * exactly on the node where this affinity key is cached.
 *
 * @param taskName Task name or task class name.
 * @param cacheName Name of the cache on which affinity should be calculated.
 * @param affKey Affinity key.
 * @param taskArg Optional task argument.
 * @return Task execution result.
 * @throw GridClientException In case of error.
 * @throw GridServerUnreachableException If none of the servers can be reached.
 * @throw GridClientClosedException If client was closed manually.
 */
GridClientVariant GridClientComputeProjectionImpl::affinityExecute(const string& taskName, const string& cacheName,
        const GridClientVariant& affKey, const GridClientVariant& taskArg) {
    if (invalidated)
        throw GridClientClosedException();

    GridTaskRequestCommand cmd;

    cmd.setTaskName(taskName);
    cmd.setArg(taskArg);

    TaskProjectionClosure c(clientUniqueId(), taskName, cmd);

    GridClientVariantHasheableObject hashObj(affKey);

    this->withReconnectHandling(c, cacheName, hashObj);

    GridClientMessageTaskResult res = c.getResult();

    return GridClientVariant(res.getTaskResult());
}

/**
 * Asynchronously executes task using cache affinity key for routing. This way
 * the task will start executing exactly on the node where this affinity key is cached.
 *
 * @param taskName Task name or task class name.
 * @param cacheName Name of the cache on which affinity should be calculated.
 * @param affKey Affinity key.
 * @param taskArg Optional task argument.
 * @return Future.
 */
TGridClientFutureVariant GridClientComputeProjectionImpl::affinityExecuteAsync(const string& taskName,
        const string& cacheName, const GridClientVariant& affKey, const GridClientVariant& taskArg) {
    if (invalidated)
        return TGridClientFutureVariant(
            new GridFailFutureImpl<GridClientVariant, GridClientClosedException>());

    GridFutureImpl<GridClientVariant>* fut = new GridFutureImpl<GridClientVariant>(threadPool);
    TGridClientFutureVariant res(fut);

    boost::packaged_task<GridClientVariant> pt(
            boost::bind(&GridClientComputeProjectionImpl::affinityExecute, this,
                    taskName, cacheName, affKey, taskArg));

    fut->task(pt);

    return res;
}

/**
 * Gets node with given id from most recently refreshed topology.
 *
 * @param id Node ID.
 * @return Node for given ID or {@code null} if node with given id was not found.
 */
TGridClientNodePtr GridClientComputeProjectionImpl::node(const GridClientUuid& uuid) const {
    return GridClientProjectionImpl::node(uuid);
}

/**
 * Gets nodes that passes the filter. If this compute instance is a projection, then only
 * nodes that passes projection criteria will be passed to the filter.
 *
 * @param filter Node filter.
 * @return Collection of nodes that satisfy provided filter.
 */
TGridClientNodeList GridClientComputeProjectionImpl::nodes(TGridClientNodePredicatePtr pred) const {
    if (pred.get() != NULL) {
        std::function <bool(const GridClientNode&)> filter = [&pred](const GridClientNode& n) { return pred->apply(n); };
        return nodes(filter);
    }
    else {
        std::function <bool(const GridClientNode&)> filter = [](const GridClientNode& n) { return true; };
        return nodes(filter);
    }
}

TGridClientNodeList GridClientComputeProjectionImpl::nodes(std::function<bool(const GridClientNode&)> & filter) const {
    TGridClientNodeList nodes;

    TNodesSet ns;

    subProjectionNodes(ns);

    for (auto it = ns.begin(); it != ns.end(); ++it) {
        GridClientNode* gcn = new GridClientNode(*it);
        TGridClientNodePtr p = TGridClientNodePtr(gcn);

        if (filter(*p))
            nodes.push_back(p);
    }

    return nodes;
}

/**
 * Gets all nodes in the projection.
 *
 * @param filter Node filter.
 * @return Collection of nodes that satisfy provided filter.
 */
TGridClientNodeList GridClientComputeProjectionImpl::nodes() const {
    TGridClientNodePredicatePtr filter = TGridClientNodePredicatePtr((GridClientPredicate<GridClientNode> *) NULL);

    return nodes(filter);
}

/**
 * Gets nodes that passes the filter. If this compute instance is a projection, then only
 * nodes that passes projection criteria will be passed to the filter.
 *
 * @param filter Node filter.
 * @return Collection of nodes that satisfy provided filter.
 */
TGridClientNodeList GridClientComputeProjectionImpl::nodes(const std::vector<GridClientUuid>& ids) const {
    TGridClientNodeList nodes;

    std::set<GridClientUuid> nodeIds(ids.begin(), ids.end());

    TNodesSet ns;

    subProjectionNodes(ns);

    for (auto it = ns.begin(); it != ns.end(); ++it) {
        if (nodeIds.find(it->getNodeId()) == nodeIds.end())
            continue;

        nodes.push_back(TGridClientNodePtr(new GridClientNode(*it)));
    }

    return nodes;
}

TGridClientNodeList GridClientComputeProjectionImpl::refreshTopology(GridTopologyRequestCommand& cmd) {
    class RefreshTopologyProjectionClosure: public ClientMessageProjectionClosure {
    public:
        RefreshTopologyProjectionClosure(std::string clientId, GridTopologyRequestCommand& topReqCmd,
                GridClientMessageTopologyResult& rslt)
                : ClientMessageProjectionClosure(clientId), cmd(topReqCmd), topRslt(rslt) {
        }

        virtual void apply(TGridClientNodePtr node, GridClientSocketAddress connParams, GridClientCommandExecutor& cmdExecutor) {
            fillRequestHeader(cmd, node);

            cmdExecutor.executeTopologyCmd(connParams, cmd, topRslt);
        }

    private:
        GridTopologyRequestCommand& cmd;
        GridClientMessageTopologyResult& topRslt;
    };

    GridClientMessageTopologyResult topRslt;

    RefreshTopologyProjectionClosure topClosure(clientUniqueId(), cmd, topRslt);

    TGridClientNodeList nodes;

    withReconnectHandling(topClosure);

    TNodesList nodesList = topRslt.getNodes();

    for (auto iter = nodesList.begin(); iter != nodesList.end(); ++iter)
        nodes.push_back(TGridClientNodePtr(new GridClientNode(*iter)));

    return nodes;
}

/**
 * Gets node by its ID.
 *
 * @param id Node ID.
 * @param includeAttrs Whether to include node attributes.
 * @param includeMetrics Whether to include node metrics.
 * @return Node descriptor or {@code null} if node doesn't exist.
 * @throw GridClientException In case of error.
 * @throw GridServerUnreachableException If none of the servers can be reached.
 * @throw GridClientClosedException If client was closed manually.
 */
TGridClientNodePtr GridClientComputeProjectionImpl::refreshNode(const GridClientUuid& id, bool includeAttrs,
        bool includeMetrics) {
    if (invalidated)
        throw GridClientClosedException();

    GridTopologyRequestCommand cmd;

    cmd.setIncludeAttributes(includeAttrs);
    cmd.setIncludeMetrics(includeMetrics);
    cmd.setNodeId(id.uuid());

    TGridClientNodeList nodes = refreshTopology(cmd);

    if (nodes.size() == 0)
        return TGridClientNodePtr((GridClientNode *) NULL);

    return nodes[0];
}

/**
 * Asynchronously gets node by its ID.
 *
 * @param id Node ID.
 * @param includeAttrs Whether to include node attributes.
 * @param includeMetrics Whether to include node metrics.
 * @return Future.
 */
TGridClientNodeFuturePtr GridClientComputeProjectionImpl::refreshNodeAsync(const GridClientUuid& id, bool includeAttrs,
        bool includeMetrics) {
    if (invalidated)
        return TGridClientNodeFuturePtr(
            new GridFailFutureImpl<TGridClientNodePtr, GridClientClosedException>());

    GridFutureImpl<TGridClientNodePtr>* fut = new GridFutureImpl<TGridClientNodePtr>(threadPool);
    TGridClientNodeFuturePtr res(fut);

    boost::packaged_task<TGridClientNodePtr> pt(
            boost::bind(
                    static_cast<TGridClientNodePtr (GridClientComputeProjectionImpl::*)(const GridClientUuid&, bool, bool)>
                    (&GridClientComputeProjectionImpl::refreshNode), this, id, includeAttrs, includeMetrics));

    fut->task(pt);

    return res;
}

/**
 * Gets node by IP address.
 *
 * @param ip IP address.
 * @param includeAttrs Whether to include node attributes.
 * @param includeMetrics Whether to include node metrics.
 * @return Node descriptor or {@code null} if node doesn't exist.
 * @throw GridClientException In case of error.
 * @throw GridServerUnreachableException If none of the servers can be reached.
 * @throw GridClientClosedException If client was closed manually.
 */
TGridClientNodePtr GridClientComputeProjectionImpl::refreshNode(const string& ip, bool includeAttrs,
        bool includeMetrics) {
    if (invalidated)
        throw GridClientClosedException();

    GridTopologyRequestCommand cmd;

    cmd.setIncludeAttributes(includeAttrs);

    cmd.setIncludeMetrics(includeMetrics);

    cmd.setNodeIp(ip);

    TGridClientNodeList nodes = refreshTopology(cmd);

    if (nodes.size() == 0)
        return TGridClientNodePtr((GridClientNode *) NULL);

    return nodes[0];
}

/**
 * Asynchronously gets node by IP address.
 *
 * @param ip IP address.
 * @param includeAttrs Whether to include node attributes.
 * @param includeMetrics Whether to include node metrics.
 * @return Future.
 */
TGridClientNodeFuturePtr GridClientComputeProjectionImpl::refreshNodeAsync(const string& ip, bool includeAttrs,
        bool includeMetrics) {
    if (invalidated)
        return TGridClientNodeFuturePtr(
            new GridFailFutureImpl<TGridClientNodePtr, GridClientClosedException>());

    GridFutureImpl<TGridClientNodePtr>* fut = new GridFutureImpl<TGridClientNodePtr>(threadPool);
    TGridClientNodeFuturePtr res(fut);

    boost::packaged_task<TGridClientNodePtr> pt(
            boost::bind(static_cast<TGridClientNodePtr (GridClientComputeProjectionImpl::*)(const string&, bool, bool)>
            (&GridClientComputeProjectionImpl::refreshNode), this, ip, includeAttrs, includeMetrics));

    fut->task(pt);

    return res;
}

/**
 * Gets current topology.
 *
 * @param includeAttrs Whether to include node attributes.
 * @param includeMetrics Whether to include node metrics.
 * @return Node descriptors.
 * @throw GridClientException In case of error.
 * @throw GridServerUnreachableException If none of the servers can be reached.
 * @throw GridClientClosedException If client was closed manually.
 */
TGridClientNodeList GridClientComputeProjectionImpl::refreshTopology(bool includeAttrs, bool includeMetrics) {
    if (invalidated)
        throw GridClientClosedException();

    GridTopologyRequestCommand cmd;

    cmd.setIncludeAttributes(includeAttrs);
    cmd.setIncludeMetrics(includeMetrics);

    return refreshTopology(cmd);
}

/**
 * Asynchronously gets current topology.
 *
 * @param includeAttrs Whether to include node attributes.
 * @param includeMetrics Whether to include node metrics.
 * @return Future.
 */
TGridClientNodeFutureList GridClientComputeProjectionImpl::refreshTopologyAsync(bool includeAttrs,
        bool includeMetrics) {
    if (invalidated)
        return TGridClientNodeFutureList(
            new GridFailFutureImpl<TGridClientNodeList, GridClientClosedException>());

    GridFutureImpl<TGridClientNodeList>* fut = new GridFutureImpl<TGridClientNodeList>(threadPool);
    TGridClientNodeFutureList res(fut);

    boost::packaged_task<TGridClientNodeList> pt(
            boost::bind(static_cast<TGridClientNodeList (GridClientComputeProjectionImpl::*)(bool, bool)>
            (&GridClientComputeProjectionImpl::refreshTopology), this, includeAttrs, includeMetrics));

    fut->task(pt);

    return res;
}

/**
 * Gets contents of default log file ({@code GRIDGAIN_HOME/work/log/gridgain.log}).
 *
 * @param lineFrom Index of line from which log is get, inclusive (starting from 0).
 * @param lineTo Index of line to which log is get, inclusive (starting from 0).
 * @return Log contents.
 * @throw GridClientException In case of error.
 * @throw GridServerUnreachableException If none of the servers can be reached.
 * @throw GridClientClosedException If client was closed manually.
 */
vector<string> GridClientComputeProjectionImpl::log(int lineFrom, int lineTo) {
    if (invalidated)
        throw GridClientClosedException();

    return log("work/log/gridgain.log", lineFrom, lineTo);
}

/**
 * Asynchronously gets contents of default log file
 * ({@code GRIDGAIN_HOME/work/log/gridgain.log}).
 *
 * @param lineFrom Index of line from which log is get, inclusive (starting from 0).
 * @param lineTo Index of line to which log is get, inclusive (starting from 0).
 * @return Future.
 */
TGridFutureStringList GridClientComputeProjectionImpl::logAsync(int lineFrom, int lineTo) {
    if (invalidated)
        return TGridFutureStringList(
            new GridFailFutureImpl<vector<string>, GridClientClosedException>());

    GridFutureImpl<vector<string> >* fut = new GridFutureImpl<vector<string> >(threadPool);
    TGridFutureStringList res(fut);

    boost::packaged_task<vector<string> > pt(
            boost::bind(static_cast<vector<string> (GridClientComputeProjectionImpl::*)(int, int)>
            (&GridClientComputeProjectionImpl::log), this, lineFrom, lineTo));

    fut->task(pt);

    return res;
}

/**
 * Gets contents of custom log file.
 *
 * @param path Log file path. Can be absolute or relative to GRIDGAIN_HOME.
 * @param lineFrom Index of line from which log is get, inclusive (starting from 0).
 * @param lineTo Index of line to which log is get, inclusive (starting from 0).
 * @return Log contents.
 * @throw GridClientException In case of error.
 * @throw GridServerUnreachableException If none of the servers can be reached.
 * @throw GridClientClosedException If client was closed manually.
 */
vector<string> GridClientComputeProjectionImpl::log(const string& path, int lineFrom, int lineTo) {
    if (invalidated)
        throw GridClientClosedException();

    vector<string> v;

    GridLogRequestCommand cmd;

    cmd.path(path);
    cmd.from(lineFrom);
    cmd.to(lineTo);

    LogProjectionClosure c(clientUniqueId(), cmd);

    this->withReconnectHandling(c);

    GridClientMessageLogResult res = c.getResult();

    return res.lines();
}

/**
 * Asynchronously gets contents of custom log file.
 *
 * @param path Log file path. Can be absolute or relative to GRIDGAIN_HOME.
 * @param lineFrom Index of line from which log is get, inclusive (starting from 0).
 * @param lineTo Index of line to which log is get, inclusive (starting from 0).
 * @return Future.
 */
TGridFutureStringList GridClientComputeProjectionImpl::logAsync(const string& path, int lineFrom, int lineTo) {
    if (invalidated)
        return TGridFutureStringList(
            new GridFailFutureImpl<vector<string>, GridClientClosedException>());

    GridFutureImpl<vector<string> >* fut = new GridFutureImpl<vector<string> >(threadPool);
    TGridFutureStringList res(fut);

    boost::packaged_task<vector<string> > pt(
            boost::bind(static_cast<vector<string> (GridClientComputeProjectionImpl::*)(const string&, int, int)>
            (&GridClientComputeProjectionImpl::log), this, path, lineFrom, lineTo));

    fut->task(pt);

    return res;
}

TGridClientComputePtr GridClientComputeProjectionImpl::makeProjection(TGridClientNodePredicatePtr filter,
        TGridClientLoadBalancerPtr balancer) {
    TGridClientComputePtr subProjection(
            new GridClientComputeProjectionImpl(sharedData, prjLsnr, filter, balancer, threadPool));

    subProjections.push_back(subProjection);

    return subProjection;
}

/**
 * Invalidates this data instance. This is done by the client to indicate
 * that is has been stopped. After this call, all interface methods
 * will throw GridClientClosedException.
 */
void GridClientComputeProjectionImpl::invalidate() {
	invalidated = true;
}
