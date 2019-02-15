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

#ifdef _WIN32
#   include <windows.h>
#endif

#include <sql.h>
#include <sqlext.h>

#include <string>

#include <boost/test/unit_test.hpp>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

#include "test_type.h"
#include "test_utils.h"
#include "odbc_test_suite.h"

using namespace ignite;
using namespace ignite::common;
using namespace ignite_test;

using namespace boost::unit_test;

/**
 * Test setup fixture.
 */
struct ConnectionTestSuiteFixture: odbc::OdbcTestSuite
{
    /**
     * Constructor.
     */
    ConnectionTestSuiteFixture() :
        OdbcTestSuite()
    {
        StartNode();
    }

    /**
     * Start a node.
     */
    void StartNode()
    {
        StartTestNode("queries-test.xml", "NodeMain");
    }

    /**
     * Execute the query and return an error code.
     */
    std::string ExecQueryAndReturnError()
    {
        SQLCHAR selectReq[] = "select count(*) from TestType";

        SQLRETURN ret = SQLExecDirect(stmt, selectReq, sizeof(selectReq));

        std::string err;

        if (!SQL_SUCCEEDED(ret))
            err = ExtractErrorCode(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        return err;
    }

    /**
     * Extract code from ODBC error message.
     *
     * @param err Error message.
     * @return Error code.
     */
    static std::string ExtractErrorCode(const std::string& err)
    {
        std::string code;

        size_t idx = err.find(':');

        if ((idx != std::string::npos) && (idx > 0))
            code = err.substr(0, idx);

        return code;
    }

    /**
     * Destructor.
     */
    ~ConnectionTestSuiteFixture()
    {
        // No-op.
    }
};

BOOST_FIXTURE_TEST_SUITE(ConnectionTestSuite, ConnectionTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestConnectionRestore)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    // Check that query was successfully executed.
    BOOST_CHECK_EQUAL(ExecQueryAndReturnError(), "");

    // Stop node.
    Ignition::StopAll(true);

    // Query execution should throw ODBC error.
    BOOST_CHECK_EQUAL(ExecQueryAndReturnError(), "08S01");

    // Reusing a closed connection should not crash an application.
    BOOST_CHECK_EQUAL(ExecQueryAndReturnError(), "08001");

    StartNode();

    // Check that connection was restored.
    BOOST_CHECK_EQUAL(ExecQueryAndReturnError(), "");
}

BOOST_AUTO_TEST_SUITE_END()
