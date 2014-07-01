/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDQUERIES_IMPL_INCLUDED
#define GRIDQUERIES_IMPL_INCLUDED

#include "gridgain/gridclientqueries.hpp"
#include "gridgain/gridclientvariant.hpp"

enum GridQueryType {
    SQL = 0,
    SQL_FIELD,
    FULL_TEXT,
    SCAN
};

class GridClientDataProjectionImpl;

template<typename T>
class GridClientDataQueryFutureImpl : public GridClientDataQueryFuture<T> {
public:
    virtual int32_t available() {
    }

    virtual T next() {
    }

    virtual bool success() {
    }
    
    virtual std::vector<T> result() const {
    }

    virtual std::vector<T> get() {
    }
    
    virtual std::vector<T> get(const boost::posix_time::time_duration& timeout) {
    }

private:
    std::vector<T> res;
};

struct GridDataQueryBean {
    GridDataQueryBean() : pageSize(0), timeout(0, 0, 0, 0), keepAll(false), incBackups(false), dedup(false), type(type) {
    }

    int32_t pageSize;

    boost::posix_time::time_duration timeout;

    bool keepAll;

    bool incBackups;

    bool dedup;

    std::string reducerClsName;

    std::string transformerClsName;

    std::vector<GridClientVariant> args;

    GridQueryType type;
    
    void remoteReducer(const std::string& clsName, const std::vector<GridClientVariant>& args) {
        if (!transformerClsName.empty())
            transformerClsName = "";

        reducerClsName = clsName;
        
        this->args = args;
    }

    void remoteTransformer(const std::string& clsName, const std::vector<GridClientVariant>& args) {
        if (!reducerClsName.empty())
            reducerClsName = "";

        transformerClsName = clsName;
        
        this->args = args;
    }
};

template<typename T>
class GridClientDataQueryImpl : public GridClientDataQuery<T> {
public:
    GridClientDataQueryImpl(GridClientDataProjectionImpl* prj, GridQueryType type) : prj(prj) {
    }

    int32_t pageSize() {
        return bean.pageSize;            
    }

    void pageSize(int32_t pageSize) {
        bean.pageSize = pageSize;
    }

    const boost::posix_time::time_duration& timeout() {
        return bean.timeout;
    }

    void timeout(const boost::posix_time::time_duration& timeout) {
        bean.timeout = timeout;
    }

    bool keepAll() {
        return bean.keepAll;
    }

    void keepAll(bool keepAll) {
        bean.keepAll = keepAll;
    }

    bool includeBackups() {
        return bean.incBackups;
    }

    void includeBackups(bool includeBackups) {
        bean.incBackups = includeBackups;
    }

    bool enableDedup() {
        return bean.dedup;
    }

    void enableDedup(bool enableDedup) {
        bean.dedup = enableDedup;
    }

    void remoteReducer(const std::string& clsName, const std::vector<GridClientVariant>& args) {
        bean.remoteReducer(clsName, args);
    }

    void remoteTransformer(const std::string& clsName, const std::vector<GridClientVariant>& args) {
        bean.remoteTransformer(clsName, args);
    }
    
    std::shared_ptr<GridClientDataQueryFuture<T>> execute() {
        // GridClientDataQueryFutureImpl<T> fut = new GridClientDataQueryFutureImpl<T>();
        // return std::shared_ptr<GridClientDataQueryFuture<T>>(fut);
        return std::shared_ptr<GridClientDataQueryFuture<T>>();
    }

    std::shared_ptr<GridClientDataQueryFuture<T>> execute(const std::vector<GridClientVariant>& args) {
        return std::shared_ptr<GridClientDataQueryFuture<T>>();
    }

private:
    GridClientDataProjectionImpl* prj;

    GridDataQueryBean bean;
};

class GridClientDataQueriesImpl : public GridClientDataQueries {
public:
    GridClientDataQueriesImpl(GridClientDataProjectionImpl* prj) : prj(prj) {
    }

    TGridClientPairQueryPtr createSqlQuery(const std::string& clsName, const std::string& clause);

    TGridClientVectorQueryPtr createSqlFieldsQuery(const std::string& clause);

    TGridClientPairQueryPtr createFullTextQuery(const std::string& clsName, const std::string& clause);

    TGridClientPairQueryPtr createScanQuery(const std::string& clsName, const std::vector<GridClientVariant>& args);

    TGridBoolFuturePtr rebuildIndexes(std::string clsName);

    TGridBoolFuturePtr rebuildAllIndexes();

private:
    GridClientDataProjectionImpl* prj;
};

#endif
