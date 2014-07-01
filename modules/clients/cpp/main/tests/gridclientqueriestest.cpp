/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#ifndef _MSC_VER
#define BOOST_TEST_DYN_LINK
#endif

#include <boost/test/unit_test.hpp>

#include <gridgain/gridgain.hpp>

#include "gridclientfactoryfixture.hpp"
#include "gridtestcommon.hpp"

using namespace std;

BOOST_AUTO_TEST_SUITE(GridClientQueriesSuite)

BOOST_FIXTURE_TEST_CASE(testSqlQuery, GridClientFactoryFixture1<clientConfig>) {
    // TODO 8491
    /*
    TGridClientDataPtr data = client->data(CACHE_NAME);

    TGridClientDataQueriesPtr qrys = data->queries();

    TGridClientPairQueryPtr qry =  qrys->createSqlQuery("class", "where");

    qry->keepAll(true);

    vector<GridClientVariant> args;

    args.push_back(1);

    TGridClientPairQueryFutPtr fut = qry->execute(args);

    vector<pair<GridClientVariant, GridClientVariant>> res = fut->get();

    for (auto iter = res.begin(); iter != res.end(); ++iter) {
        pair<GridClientVariant, GridClientVariant>& res = *iter;
    }
    */
}

BOOST_FIXTURE_TEST_CASE(testSqlQueryIterate, GridClientFactoryFixture1<clientConfig>) {
    // TODO 8491

    /*
    TGridClientDataPtr data = client->data(CACHE_NAME);

    TGridClientDataQueriesPtr qrys = data->queries();

    TGridClientPairQueryPtr qry =  qrys->createSqlQuery("class", "where");

    qry->keepAll(true);
    qry->pageSize(10);

    vector<GridClientVariant> args;

    args.push_back(1);

    TGridClientPairQueryFutPtr fut = qry->execute(args);

    while (true) {
        pair<GridClientVariant, GridClientVariant> next = fut->next();

        if (!next.second.hasAnyValue())
            break;
    }
    */
}

BOOST_AUTO_TEST_SUITE_END()