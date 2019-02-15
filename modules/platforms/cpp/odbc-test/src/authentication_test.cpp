/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

#include "ignite/odbc/system/odbc_constants.h"

#include <vector>
#include <string>

#include <boost/regex.hpp>
#include <boost/test/unit_test.hpp>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

#include "test_utils.h"
#include "odbc_test_suite.h"
#include "test_type.h"

using namespace ignite;
using namespace ignite::common;
using namespace ignite_test;

using namespace boost::unit_test;

namespace
{
    /** Default login. */
    const std::string defaultUser = "ignite";

    /** Default pass. */
    const std::string defaultPass = "ignite";
}

/**
 * Test setup fixture.
 */
struct AuthenticationTestSuiteFixture : odbc::OdbcTestSuite
{
    static Ignite StartAdditionalNode(const char* name)
    {
        return StartPlatformNode("queries-auth.xml", name);
    }

    /**
     * Constructor.
     */
    AuthenticationTestSuiteFixture() :
        OdbcTestSuite()
    {
        ClearLfs();

        grid = StartAdditionalNode("NodeMain");

        grid.SetActive(true);
    }

    /**
     * Destructor.
     */
    virtual ~AuthenticationTestSuiteFixture()
    {
        // No-op.
    }

    /**
     * Create connection string.
     *
     * @param user User.
     * @param pass Password.
     * @return Connection string.
     */
    static std::string MakeConnectionString(const std::string& user, const std::string& pass)
    {
        std::string cfgDirPath = GetTestConfigDir();

        std::stringstream connectString;

        connectString <<
            "DRIVER={Apache Ignite};"
            "ADDRESS=127.0.0.1:11110;"
            "SCHEMA=cache;"
            "USER=" << user << ";"
            "PASSWORD=" << pass << ";";

        return connectString.str();
    }

    /**
     * Create default connection string.
     * @return Default connection string.
     */
    static std::string MakeDefaultConnectionString()
    {
        return MakeConnectionString(defaultUser, defaultPass);
    }

    /** Node started during the test. */
    Ignite grid;
};

BOOST_FIXTURE_TEST_SUITE(AuthenticationTestSuite, AuthenticationTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestConnectionDefaultAuthSuccess)
{
    Connect(MakeDefaultConnectionString());

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestConnectionAuthReject)
{
    std::string state = ExpectConnectionReject(MakeConnectionString("unknown", "unknown"));

    BOOST_CHECK_EQUAL(std::string("08004"), state);
}

BOOST_AUTO_TEST_CASE(TestConnectionUserOperationsQuery)
{
    Connect(MakeDefaultConnectionString());

    SQLRETURN ret = ExecQuery("CREATE USER \"test\" WITH PASSWORD 'somePass'");

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    Disconnect();

    Connect(MakeConnectionString("test", "somePass"));

    ret = ExecQuery("ALTER USER \"test\" WITH PASSWORD 'somePass42'");

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    Disconnect();

    Connect(MakeConnectionString("test", "somePass42"));

    Disconnect();

    Connect(MakeDefaultConnectionString());

    ret = ExecQuery("DROP USER \"test\"");

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    Disconnect();

    std::string state = ExpectConnectionReject(MakeConnectionString("test", "somePass"));

    BOOST_CHECK_EQUAL(std::string("08004"), state);
}

BOOST_AUTO_TEST_SUITE_END()
