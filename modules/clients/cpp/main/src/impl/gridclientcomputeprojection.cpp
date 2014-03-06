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

    virtual void apply(TGridClientNodePtr node, GridSocketAddress connParams, GridClientCommandExecutor& cmdExecutor) {
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

    virtual void apply(TGridClientNodePtr node, GridSocketAddress connParams, GridClientCommandExecutor& cmdExecutor) {
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
    GridClientPredicateLambdaWrapper(std::function<bool (const T&)> lambda): lambda(lambda) {}

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

TGridClientComputePtr GridClientComputeProjectionImpl::projection(const GridClientNode& node) {
    TGridClientNodePredicatePtr filter(new GridClientNodeUuidFilter(node));

    return makeProjection(filter, TGridClientLoadBalancerPtr());
}

TGridClientComputePtr GridClientComputeProjectionImpl::projection(const TGridClientNodeList& nodes) {
    TGridClientNodePredicatePtr filter(new GridClientNodeUuidFilter(nodes));

    return makeProjection(filter, TGridClientLoadBalancerPtr());
}

TGridClientComputePtr GridClientComputeProjectionImpl::projection(TGridClientNodePredicatePtr filter) {
    return makeProjection(filter, TGridClientLoadBalancerPtr());
}

TGridClientComputePtr GridClientComputeProjectionImpl::projection(std::function<bool(const GridClientNode&)> filter) {
    return makeProjection(
        TGridClientNodePredicatePtr(
            new GridClientPredicateLambdaWrapper<GridClientNode>(filter)),
        TGridClientLoadBalancerPtr());
}

TGridClientComputePtr GridClientComputeProjectionImpl::projection(const TGridClientNodePredicatePtr filter,
        TGridClientLoadBalancerPtr balancer) {
    return makeProjection(filter, balancer);
}

TGridClientComputePtr GridClientComputeProjectionImpl::projection(std::function<bool (const GridClientNode&)> filter,
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

TGridClientLoadBalancerPtr GridClientComputeProjectionImpl::balancer() const {
    return loadBalancer();
}

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

TGridClientNodePtr GridClientComputeProjectionImpl::node(const GridUuid& uuid) const {
    return GridClientProjectionImpl::node(uuid);
}

TGridClientNodeList GridClientComputeProjectionImpl::nodes(TGridClientNodePredicatePtr pred) const {
    if (pred.get() != NULL) {
        return nodes([&pred](const GridClientNode& n) {
            return pred->apply(n);
        });
    }
    else {
        return nodes([](const GridClientNode& n) {
            return true;
        });
    }
}

TGridClientNodeList GridClientComputeProjectionImpl::nodes(std::function<bool(const GridClientNode&)> filter) const {
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

TGridClientNodeList GridClientComputeProjectionImpl::nodes() const {
    TGridClientNodePredicatePtr filter = TGridClientNodePredicatePtr((GridClientPredicate<GridClientNode> *) NULL);

    return nodes(filter);
}

TGridClientNodeList GridClientComputeProjectionImpl::nodes(const std::vector<GridUuid>& ids) const {
    TGridClientNodeList nodes;

    std::set<GridUuid> nodeIds(ids.begin(), ids.end());

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

        virtual void apply(TGridClientNodePtr node, GridSocketAddress connParams, GridClientCommandExecutor& cmdExecutor) {
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

TGridClientNodePtr GridClientComputeProjectionImpl::refreshNode(const GridUuid& id, bool includeAttrs,
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

TGridClientNodeFuturePtr GridClientComputeProjectionImpl::refreshNodeAsync(const GridUuid& id, bool includeAttrs,
        bool includeMetrics) {
    if (invalidated)
        return TGridClientNodeFuturePtr(
            new GridFailFutureImpl<TGridClientNodePtr, GridClientClosedException>());

    GridFutureImpl<TGridClientNodePtr>* fut = new GridFutureImpl<TGridClientNodePtr>(threadPool);
    TGridClientNodeFuturePtr res(fut);

    boost::packaged_task<TGridClientNodePtr> pt(
            boost::bind(
                    static_cast<TGridClientNodePtr (GridClientComputeProjectionImpl::*)(const GridUuid&, bool, bool)>
                    (&GridClientComputeProjectionImpl::refreshNode), this, id, includeAttrs, includeMetrics));

    fut->task(pt);

    return res;
}

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

TGridClientNodeList GridClientComputeProjectionImpl::refreshTopology(bool includeAttrs, bool includeMetrics) {
    if (invalidated)
        throw GridClientClosedException();

    GridTopologyRequestCommand cmd;

    cmd.setIncludeAttributes(includeAttrs);
    cmd.setIncludeMetrics(includeMetrics);

    return refreshTopology(cmd);
}

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

vector<string> GridClientComputeProjectionImpl::log(int lineFrom, int lineTo) {
    if (invalidated)
        throw GridClientClosedException();

    return log("work/log/gridgain.log", lineFrom, lineTo);
}

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

void GridClientComputeProjectionImpl::invalidate() {
	invalidated = true;
}
