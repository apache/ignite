/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLIENTQUERIES_HPP_INCLUDED
#define GRIDCLIENTQUERIES_HPP_INCLUDED

#include <gridgain/gridclienttypedef.hpp>
#include <gridgain/gridconf.hpp>
#include <gridgain/gridfuture.hpp>

#include <boost/date_time.hpp>

template<typename T>
class GridClientDataQueryFuture : public GridClientFuture<std::vector<T>> {
public:
    virtual int32_t available() = 0;

    virtual T next() = 0;
};

template<typename T>
class GridClientDataQuery {
public:
    virtual ~GridClientDataQuery() {
    }

    virtual int32_t pageSize() = 0;

    virtual void pageSize(int32_t pageSize) = 0;

    virtual const boost::posix_time::time_duration& timeout() = 0;

    virtual void timeout(const boost::posix_time::time_duration& timeout) = 0;

    virtual bool keepAll() = 0;

    virtual void keepAll(bool keepAll) = 0;

    virtual bool includeBackups() = 0;

    virtual void includeBackups(bool includeBackups) = 0;

    virtual bool enableDedup() = 0;

    virtual void enableDedup(bool enableDedup) = 0;

    virtual void remoteReducer(const std::string& clsName, const std::vector<GridClientVariant>& args) = 0;

    virtual void remoteTransformer(const std::string& clsName, const std::vector<GridClientVariant>& args) = 0;

    virtual std::shared_ptr<GridClientDataQueryFuture<T>> execute() = 0;

    virtual std::shared_ptr<GridClientDataQueryFuture<T>> execute(const std::vector<GridClientVariant>& args) = 0;
};

/**
 * A queries projection of grid client. Contains various methods for cache query operations.
 */
class GRIDGAIN_API GridClientDataQueries {
public:
    virtual ~GridClientDataQueries() {
    }

    virtual TGridClientPairQueryPtr createSqlQuery(const std::string& clsName, const std::string& clause) = 0;

    virtual TGridClientVectorQueryPtr createSqlFieldsQuery(const std::string& clause) = 0;

    virtual TGridClientPairQueryPtr createFullTextQuery(const std::string& clsName, const std::string& clause) = 0;

    virtual TGridClientPairQueryPtr createScanQuery(const std::string& clsName, const std::vector<GridClientVariant>& args) = 0;

    virtual TGridBoolFuturePtr rebuildIndexes(std::string clsName) = 0;

    virtual TGridBoolFuturePtr rebuildAllIndexes() = 0;
};

#endif
