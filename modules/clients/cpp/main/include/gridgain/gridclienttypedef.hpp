// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_TYPEDEF_HPP_INCLUDED
#define GRID_CLIENT_TYPEDEF_HPP_INCLUDED

#include <map>
#include <vector>
#include <memory>

class GridClient;
class GridClientNode;
class GridClientData;
class GridClientSharedData;
class GridClientCompute;
class GridClientTopologyListener;
class GridClientLoadBalancer;
class GridClientVariant;
class GridClientDataMetrics;
class GridSocketAddress;
class GridBoolFuture;
class GridClientRouterBalancer;
template <class T> class GridFuture;
template<class T> class GridClientPredicate;

typedef GridClientPredicate<GridClientNode> TGridClientNodePredicate;

typedef std::shared_ptr<GridBoolFuture> TGridBoolFuturePtr;
typedef std::shared_ptr<GridClient> TGridClientPtr;
typedef std::shared_ptr<GridClientNode> TGridClientNodePtr;
typedef std::shared_ptr<GridClientData> TGridClientDataPtr;
typedef std::shared_ptr<GridClientSharedData> TGridClientSharedDataPtr;
typedef std::shared_ptr<GridClientCompute> TGridClientComputePtr;
typedef std::shared_ptr<GridClientLoadBalancer> TGridClientLoadBalancerPtr;
typedef std::shared_ptr<GridClientTopologyListener> TGridClientTopologyListenerPtr;
typedef std::shared_ptr<TGridClientNodePredicate> TGridClientNodePredicatePtr;
typedef std::shared_ptr<GridClientRouterBalancer> TGridClientRouterBalancerPtr;

typedef std::map<GridClientVariant, GridClientVariant> TGridClientVariantMap;
typedef std::vector<GridClientVariant> TGridClientVariantSet;
typedef std::vector<TGridClientNodePtr> TGridClientNodeList;
typedef std::vector<GridSocketAddress> TGridSocketAddressList;

typedef std::shared_ptr<GridFuture<TGridClientVariantMap> > TGridClientFutureVariantMap;
typedef std::shared_ptr<GridFuture<GridClientDataMetrics> > TGridClientFutureDataMetrics;
typedef std::shared_ptr<GridFuture<GridClientVariant> > TGridClientFutureVariant;
typedef std::shared_ptr<GridFuture<TGridClientNodePtr> > TGridClientNodeFuturePtr;
typedef std::shared_ptr<GridFuture<TGridClientNodeList> > TGridClientNodeFutureList;
typedef std::shared_ptr<GridFuture<std::vector<std::string> > > TGridFutureStringList;

typedef std::vector<TGridClientTopologyListenerPtr> TGridClientTopologyListenerList;

typedef std::shared_ptr<TGridClientTopologyListenerList> TGridClientTopologyListenerListPtr;

#ifdef _MSC_VER
#include <boost/atomic.hpp>

typedef boost::atomic_bool TGridAtomicBool;
typedef boost::atomic_int TGridAtomicInt;
#else
#include <atomic>

typedef std::atomic_bool TGridAtomicBool;
typedef std::atomic_int TGridAtomicInt;
#endif

#endif // end of header define
