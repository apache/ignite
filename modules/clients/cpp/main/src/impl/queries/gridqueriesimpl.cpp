/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include "gridgain/impl/queries/gridqueriesimpl.hpp"

TGridClientPairQueryPtr GridClientDataQueriesImpl::createSqlQuery(const std::string& clsName, const std::string& clause) {
    GridClientDataQueryImpl<std::pair<GridClientVariant, GridClientVariant>>* qry = 
        new GridClientDataQueryImpl<std::pair<GridClientVariant, GridClientVariant>>(prj, GridQueryType::SQL);

    return TGridClientPairQueryPtr(qry);
}

TGridClientVectorQueryPtr GridClientDataQueriesImpl::createSqlFieldsQuery(const std::string& clause) {
    return TGridClientVectorQueryPtr();
}

TGridClientPairQueryPtr GridClientDataQueriesImpl::createFullTextQuery(const std::string& clsName, const std::string& clause) {
    return TGridClientPairQueryPtr();
}

TGridClientPairQueryPtr GridClientDataQueriesImpl::createScanQuery(const std::string& clsName, const std::vector<GridClientVariant>& args) {
    return TGridClientPairQueryPtr();
}

TGridBoolFuturePtr GridClientDataQueriesImpl::rebuildIndexes(std::string clsName) {
    return TGridBoolFuturePtr();
}

TGridBoolFuturePtr GridClientDataQueriesImpl::rebuildAllIndexes() {
    return TGridBoolFuturePtr();
}
