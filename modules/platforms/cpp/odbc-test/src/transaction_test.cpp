/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifdef _WIN32
#   include <windows.h>
#endif

#include <sql.h>
#include <sqlext.h>

#include <string>

#include <boost/test/unit_test.hpp>

#include "ignite/ignition.h"

#include "test_utils.h"
#include "odbc_test_suite.h"

using namespace ignite;
using namespace ignite_test;

using namespace boost::unit_test;

/**
 * Test setup fixture.
 */
struct TransactionTestSuiteFixture : public odbc::OdbcTestSuite
{
    static Ignite StartAdditionalNode(const char* name)
    {
        return StartTestNode("queries-transaction.xml", name);
    }

    /**
     * Constructor.
     */
    TransactionTestSuiteFixture() :
        grid(StartAdditionalNode("NodeMain"))
    {
        // No-op.
    }

    /**
     * Destructor.
     */
    ~TransactionTestSuiteFixture()
    {
        // No-op.
    }

    /**
     * Insert test string value in cache and make all the neccessary checks.
     *
     * @param key Key.
     * @param value Value.
     */
    void InsertTestValue(int64_t key, const std::string& value)
    {
        InsertTestValue(stmt, key, value);
    }

    /**
     * Insert test string value in cache and make all the neccessary checks.
     *
     * @param stmt Statement.
     * @param key Key.
     * @param value Value.
     */
    static void InsertTestValue(SQLHSTMT stmt, int64_t key, const std::string& value)
    {
        SQLCHAR insertReq[] = "INSERT INTO TestType(_key, strField) VALUES(?, ?)";

        SQLRETURN ret = SQLPrepare(stmt, insertReq, SQL_NTS);

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        char strField[1024] = { 0 };
        SQLLEN strFieldLen = 0;

        ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key, 0, 0);

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        ret = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, sizeof(strField),
            sizeof(strField), &strField, sizeof(strField), &strFieldLen);

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        CopyStringToBuffer(strField, value, sizeof(strField));
        strFieldLen = SQL_NTS;

        ret = SQLExecute(stmt);

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        SQLLEN affected = 0;
        ret = SQLRowCount(stmt, &affected);

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        BOOST_CHECK_EQUAL(affected, 1);

        ret = SQLMoreResults(stmt);

        if (ret != SQL_NO_DATA)
            ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        ResetStatement(stmt);
    }

    /**
     * Update test string value in cache and make all the neccessary checks.
     *
     * @param key Key.
     * @param value Value.
     */
    void UpdateTestValue(int64_t key, const std::string& value)
    {
        UpdateTestValue(stmt, key, value);
    }

    /**
     * Update test string value in cache and make all the neccessary checks.
     *
     * @param stmt Statement.
     * @param key Key.
     * @param value Value.
     */
    static void UpdateTestValue(SQLHSTMT stmt, int64_t key, const std::string& value)
    {
        SQLCHAR insertReq[] = "UPDATE TestType SET strField=? WHERE _key=?";

        SQLRETURN ret = SQLPrepare(stmt, insertReq, SQL_NTS);

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        char strField[1024] = { 0 };
        SQLLEN strFieldLen = 0;

        ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, sizeof(strField),
            sizeof(strField), &strField, sizeof(strField), &strFieldLen);

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        ret = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key, 0, 0);

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        CopyStringToBuffer(strField, value, sizeof(strField));
        strFieldLen = SQL_NTS;

        ret = SQLExecute(stmt);

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        SQLLEN affected = 0;
        ret = SQLRowCount(stmt, &affected);

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        BOOST_CHECK_EQUAL(affected, 1);

        ret = SQLMoreResults(stmt);

        if (ret != SQL_NO_DATA)
            ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        ResetStatement(stmt);
    }

    /**
     * Delete test string value.
     *
     * @param key Key.
     */
    void DeleteTestValue(int64_t key)
    {
        DeleteTestValue(stmt, key);
    }

    /**
     * Delete test string value.
     *
     * @param stmt Statement.
     * @param key Key.
     */
    static void DeleteTestValue(SQLHSTMT stmt, int64_t key)
    {
        SQLCHAR insertReq[] = "DELETE FROM TestType WHERE _key=?";

        SQLRETURN ret = SQLPrepare(stmt, insertReq, SQL_NTS);

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key, 0, 0);

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        ret = SQLExecute(stmt);

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        SQLLEN affected = 0;
        ret = SQLRowCount(stmt, &affected);

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        BOOST_CHECK_EQUAL(affected, 1);

        ret = SQLMoreResults(stmt);

        if (ret != SQL_NO_DATA)
            ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        ResetStatement(stmt);
    }
    /**
     * Selects and checks the value.
     *
     * @param key Key.
     * @param expect Expected value.
     */
    void CheckTestValue(int64_t key, const std::string& expect)
    {
        CheckTestValue(stmt, key, expect);
    }

    /**
     * Selects and checks the value.
     *
     * @param stmt Statement.
     * @param key Key.
     * @param expect Expected value.
     */
    static void CheckTestValue(SQLHSTMT stmt, int64_t key, const std::string& expect)
    {
        // Just selecting everything to make sure everything is OK
        SQLCHAR selectReq[] = "SELECT strField FROM TestType WHERE _key = ?";

        char strField[1024] = { 0 };
        SQLLEN strFieldLen = 0;

        SQLRETURN ret = SQLBindCol(stmt, 1, SQL_C_CHAR, &strField, sizeof(strField), &strFieldLen);

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key, 0, 0);

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        ret = SQLExecDirect(stmt, selectReq, sizeof(selectReq));

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        ret = SQLFetch(stmt);

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        BOOST_CHECK_EQUAL(std::string(strField, strFieldLen), expect);

        ret = SQLFetch(stmt);

        BOOST_CHECK_EQUAL(ret, SQL_NO_DATA);

        ret = SQLMoreResults(stmt);

        if (ret != SQL_NO_DATA)
            ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        ResetStatement(stmt);
    }

    /**
     * Selects and checks that value is absent.
     *
     * @param key Key.
     */
    void CheckNoTestValue(int64_t key)
    {
        CheckNoTestValue(stmt, key);
    }

    /**
     * Selects and checks that value is absent.
     *
     * @param stmt Statement.
     * @param key Key.
     */
    static void CheckNoTestValue(SQLHSTMT stmt, int64_t key)
    {
        // Just selecting everything to make sure everything is OK
        SQLCHAR selectReq[] = "SELECT strField FROM TestType WHERE _key = ?";

        char strField[1024] = { 0 };
        SQLLEN strFieldLen = 0;

        SQLRETURN ret = SQLBindCol(stmt, 1, SQL_C_CHAR, &strField, sizeof(strField), &strFieldLen);

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key, 0, 0);

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        ret = SQLExecDirect(stmt, selectReq, sizeof(selectReq));

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        ret = SQLFetch(stmt);

        BOOST_CHECK_EQUAL(ret, SQL_NO_DATA);

        if (ret != SQL_NO_DATA)
            ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        ret = SQLMoreResults(stmt);

        BOOST_CHECK_EQUAL(ret, SQL_NO_DATA);

        if (ret != SQL_NO_DATA)
            ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        ResetStatement(stmt);
    }

    /**
     * Reset statement state.
     */
    void ResetStatement()
    {
        ResetStatement(stmt);
    }

    /**
     * Reset statement state.
     *
     * @param stmt Statement.
     */
    static void ResetStatement(SQLHSTMT stmt)
    {
        SQLRETURN ret = SQLFreeStmt(stmt, SQL_RESET_PARAMS);

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        ret = SQLFreeStmt(stmt, SQL_UNBIND);

        ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);
    }

    /** Node started during the test. */
    Ignite grid;
};

BOOST_FIXTURE_TEST_SUITE(TransactionTestSuite, TransactionTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TransactionConnectionCommit)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache");

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestValue(42, "Some");

    CheckTestValue(42, "Some");

    ret = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_COMMIT);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    CheckTestValue(42, "Some");
}

BOOST_AUTO_TEST_CASE(TransactionConnectionRollbackInsert)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache");

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestValue(42, "Some");

    CheckTestValue(42, "Some");

    ret = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    CheckNoTestValue(42);
}

BOOST_AUTO_TEST_CASE(TransactionConnectionRollbackUpdate1)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache");

    InsertTestValue(42, "Some");

    CheckTestValue(42, "Some");

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    UpdateTestValue(42, "Other");

    CheckTestValue(42, "Other");

    ret = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    CheckTestValue(42, "Some");
}

BOOST_AUTO_TEST_CASE(TransactionConnectionRollbackUpdate2)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache");

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestValue(42, "Some");

    CheckTestValue(42, "Some");

    ret = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_COMMIT);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    CheckTestValue(42, "Some");

    UpdateTestValue(42, "Other");

    CheckTestValue(42, "Other");

    ret = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    CheckTestValue(42, "Some");
}

BOOST_AUTO_TEST_CASE(TransactionConnectionRollbackDelete1)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache");

    InsertTestValue(42, "Some");

    CheckTestValue(42, "Some");

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    DeleteTestValue(42);

    CheckNoTestValue(42);

    ret = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    CheckTestValue(42, "Some");
}

BOOST_AUTO_TEST_CASE(TransactionConnectionRollbackDelete2)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache");

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestValue(42, "Some");

    CheckTestValue(42, "Some");

    ret = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_COMMIT);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    CheckTestValue(42, "Some");

    DeleteTestValue(42);

    CheckNoTestValue(42);

    ret = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    CheckTestValue(42, "Some");
}

BOOST_AUTO_TEST_CASE(TransactionConnectionTxModeError)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache;nested_tx_mode=error");

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestValue(42, "Some");

    ret = ExecQuery("BEGIN");

    BOOST_CHECK_EQUAL(ret, SQL_ERROR);
}

BOOST_AUTO_TEST_CASE(TransactionConnectionTxModeIgnore)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache;nested_tx_mode=ignore");

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestValue(42, "Some");

    ret = ExecQuery("BEGIN");

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    ret = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    CheckNoTestValue(42);
}

BOOST_AUTO_TEST_CASE(TransactionConnectionTxModeCommit)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache;nested_tx_mode=commit");

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestValue(42, "Some");

    ret = ExecQuery("BEGIN");

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    UpdateTestValue(42, "Other");

    CheckTestValue(42, "Other");

    ret = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    CheckTestValue(42, "Some");
}

BOOST_AUTO_TEST_CASE(TransactionEnvironmentCommit)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache");

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestValue(42, "Some");

    CheckTestValue(42, "Some");

    ret = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_COMMIT);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_ENV, env);

    CheckTestValue(42, "Some");
}

BOOST_AUTO_TEST_CASE(TransactionEnvironmentRollbackInsert)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache");

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestValue(42, "Some");

    CheckTestValue(42, "Some");

    ret = SQLEndTran(SQL_HANDLE_ENV, env, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_ENV, env);

    CheckNoTestValue(42);
}

BOOST_AUTO_TEST_CASE(TransactionEnvironmentRollbackUpdate1)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache");

    InsertTestValue(42, "Some");

    CheckTestValue(42, "Some");

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    UpdateTestValue(42, "Other");

    CheckTestValue(42, "Other");

    ret = SQLEndTran(SQL_HANDLE_ENV, env, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_ENV, env);

    CheckTestValue(42, "Some");
}

BOOST_AUTO_TEST_CASE(TransactionEnvironmentRollbackUpdate2)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache");

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestValue(42, "Some");

    CheckTestValue(42, "Some");

    ret = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_COMMIT);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_ENV, env);

    CheckTestValue(42, "Some");

    UpdateTestValue(42, "Other");

    CheckTestValue(42, "Other");

    ret = SQLEndTran(SQL_HANDLE_ENV, env, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_ENV, env);

    CheckTestValue(42, "Some");
}

BOOST_AUTO_TEST_CASE(TransactionEnvironmentRollbackDelete1)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache");

    InsertTestValue(42, "Some");

    CheckTestValue(42, "Some");

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    DeleteTestValue(42);

    CheckNoTestValue(42);

    ret = SQLEndTran(SQL_HANDLE_ENV, env, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_ENV, env);

    CheckTestValue(42, "Some");
}

BOOST_AUTO_TEST_CASE(TransactionEnvironmentRollbackDelete2)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache");

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestValue(42, "Some");

    CheckTestValue(42, "Some");

    ret = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_COMMIT);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_ENV, env);

    CheckTestValue(42, "Some");

    DeleteTestValue(42);

    CheckNoTestValue(42);

    ret = SQLEndTran(SQL_HANDLE_ENV, env, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_ENV, env);

    CheckTestValue(42, "Some");
}

BOOST_AUTO_TEST_CASE(TransactionEnvironmentTxModeError)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache;nested_tx_mode=error");

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestValue(42, "Some");

    ret = ExecQuery("BEGIN");

    BOOST_CHECK_EQUAL(ret, SQL_ERROR);
}

BOOST_AUTO_TEST_CASE(TransactionEnvironmentTxModeIgnore)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache;nested_tx_mode=ignore");

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestValue(42, "Some");

    ret = ExecQuery("BEGIN");

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    ret = SQLEndTran(SQL_HANDLE_ENV, env, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_ENV, env);

    CheckNoTestValue(42);
}

BOOST_AUTO_TEST_CASE(TransactionEnvironmentTxModeCommit)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache;nested_tx_mode=commit");

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestValue(42, "Some");

    ret = ExecQuery("BEGIN");

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    UpdateTestValue(42, "Other");

    CheckTestValue(42, "Other");

    ret = SQLEndTran(SQL_HANDLE_ENV, env, SQL_ROLLBACK);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_ENV, env);

    CheckTestValue(42, "Some");
}

BOOST_AUTO_TEST_CASE(TransactionVersionMismatchError)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache");

    InsertTestValue(1, "test_1");

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    CheckTestValue(1, "test_1");

    SQLHDBC dbc2;
    SQLHSTMT stmt2;

    Connect(dbc2, stmt2, "DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache");

    ret = SQLSetConnectAttr(dbc2, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc2);

    InsertTestValue(stmt2, 2, "test_2");

    ret = SQLEndTran(SQL_HANDLE_DBC, dbc2, SQL_COMMIT);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc2);

    CheckTestValue(stmt2, 1, "test_1");
    CheckTestValue(stmt2, 2, "test_2");

    try
    {
        InsertTestValue(2, "test_2");

        BOOST_FAIL("Exception is expected");
    }
    catch (OdbcClientError& err)
    {
        BOOST_CHECK(err.message.find("Cannot serialize transaction due to write conflict") != err.message.npos);
        BOOST_CHECK_EQUAL(err.sqlstate, "40001");

        ResetStatement(stmt);
    }

    try
    {
        CheckTestValue(1, "test_1");

        BOOST_FAIL("Exception is expected");
    }
    catch (OdbcClientError& err)
    {
        BOOST_CHECK(err.message.find("Transaction is already completed") != err.message.npos);
        BOOST_CHECK_EQUAL(err.sqlstate, "25000");

        ResetStatement(stmt);
    }

    ret = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_ROLLBACK);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt2);

    SQLDisconnect(dbc2);

    SQLFreeHandle(SQL_HANDLE_DBC, dbc2);
}

BOOST_AUTO_TEST_SUITE_END()
