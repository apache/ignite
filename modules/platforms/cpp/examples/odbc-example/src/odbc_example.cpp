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
#include <windows.h>
#endif

#include <sql.h>
#include <sqlext.h>

#include <iostream>
#include <iomanip>
#include <vector>
#include <string>
#include <sstream>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

#include "ignite/examples/person.h"
#include "ignite/examples/organization.h"

using namespace ignite;

using namespace examples;

/**
 * This example populates cache with sample data and runs several SQL queries
 * over this data using system ODBC API and Apache Ignite ODBC driver.
 *
 * To run this example you should first install ODBC driver as described in
 * README file for the ODBC driver project.
 *
 * After all pre-requirements are fulfilled just build project as described
 * in README and run resulting file.
 *
 * Note, that all fields which used in queries must be listed in config file
 * under queryEntities property of the caches. You can find config file in
 * config directory: cpp/examples/odbc-example/config/example-odbc.xml
 *
 * In addition to all the fields listed under QueryEntity bean, each table
 * have two special predefined fields: _key and _val, which represent links
 * to whole key and value objects. In some queries _key column is used. Key
 * in our case works like an ID for the row and it should always present in
 * INSERT statements.
 */

/** Read buffer size. */
enum { ODBC_BUFFER_SIZE = 1024 };

/**
 * Represents simple string buffer.
 */
struct OdbcStringBuffer
{
    SQLCHAR buffer[ODBC_BUFFER_SIZE];
    SQLLEN reallen;
};

/**
 * Print result set returned by query.
 *
 * @param stmt Statement.
 */
void PrintOdbcResultSet(SQLHSTMT stmt)
{
    SQLSMALLINT columnsCnt = 0;

    // Getting number of columns in result set.
    SQLNumResultCols(stmt, &columnsCnt);

    std::vector<OdbcStringBuffer> columns(columnsCnt);

    // Binding colums.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
        SQLBindCol(stmt, i + 1, SQL_CHAR, columns[i].buffer, ODBC_BUFFER_SIZE, &columns[i].reallen);

    while (true)
    {
        SQLRETURN ret = SQLFetch(stmt);

        if (!SQL_SUCCEEDED(ret))
            break;

        std::cout << ">>> ";

        for (size_t i = 0; i < columns.size(); ++i)
            std::cout << std::setw(16) << std::left << columns[i].buffer << " ";

        std::cout << std::endl;
    }
}

/**
 * Extract error message.
 *
 * @param handleType Type of the handle.
 * @param handle Handle.
 * @return Error message.
 */
std::string GetOdbcErrorMessage(SQLSMALLINT handleType, SQLHANDLE handle)
{
    SQLCHAR sqlstate[7] = {};
    SQLINTEGER nativeCode;

    SQLCHAR message[ODBC_BUFFER_SIZE];
    SQLSMALLINT reallen = 0;

    SQLGetDiagRec(handleType, handle, 1, sqlstate, &nativeCode, message, ODBC_BUFFER_SIZE, &reallen);

    return std::string(reinterpret_cast<char*>(sqlstate)) + ": " +
        std::string(reinterpret_cast<char*>(message), reallen);
}

/**
 * Extract error from ODBC handle and throw it as IgniteError.
 *
 * @param handleType Type of the handle.
 * @param handle Handle.
 * @param msg Error message.
 */
void ThrowOdbcError(SQLSMALLINT handleType, SQLHANDLE handle, std::string msg)
{
    std::stringstream builder;

    builder << msg << ": " << GetOdbcErrorMessage(handleType, handle);

    std::string errorMsg = builder.str();

    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, errorMsg.c_str());
}

/**
 * Fetch cache data using ODBC interface.
 */
void GetDataWithOdbc(SQLHDBC dbc, const std::string& query)
{
    SQLHSTMT stmt;

    // Allocate a statement handle
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

    std::vector<SQLCHAR> buf(query.begin(), query.end());

    SQLRETURN ret = SQLExecDirect(stmt, &buf[0], static_cast<SQLSMALLINT>(buf.size()));

    if (SQL_SUCCEEDED(ret))
        PrintOdbcResultSet(stmt);
    else
        std::cerr << "Failed to execute query: " << GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt) << std::endl;

    // Releasing statement handle.
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/**
 * Populate Person cache with sample data.
 * 
 * @param dbc Database connection.
 */
void PopulatePerson(SQLHDBC dbc)
{
    SQLHSTMT stmt;

    // Allocate a statement handle
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

    if (!SQL_SUCCEEDED(ret))
        ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to allocate statement handle");

    try
    {
        SQLCHAR query[] =
            "INSERT INTO Person (_key, orgId, firstName, lastName, resume, salary) "
            "VALUES (?, ?, ?, ?, ?, ?)";

        ret = SQLPrepare(stmt, query, static_cast<SQLSMALLINT>(sizeof(query)));

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to prepare query");

        // Binding columns.

        int64_t key = 0;
        int64_t orgId = 0;
        char firstName[1024] = { 0 };
        SQLLEN firstNameLen = SQL_NTS;
        char lastName[1024] = { 0 };
        SQLLEN lastNameLen = SQL_NTS;
        char resume[1024] = { 0 };
        SQLLEN resumeLen = SQL_NTS;
        double salary = 0.0;

        ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key, 0, 0);

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to bind parameter");

        ret = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &orgId, 0, 0);

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to bind parameter");

        ret = SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
            sizeof(firstName), sizeof(firstName), firstName, 0, &firstNameLen);

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to bind parameter");

        ret = SQLBindParameter(stmt, 4, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
            sizeof(lastName), sizeof(lastName), lastName, 0, &lastNameLen);

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to bind parameter");

        ret = SQLBindParameter(stmt, 5, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
            sizeof(resume), sizeof(resume), resume, 0, &resumeLen);

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to bind parameter");

        ret = SQLBindParameter(stmt, 6, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, &salary, 0, 0);

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to bind parameter");

        // Filling cache.

        key = 1;
        orgId = 1;
        strncpy(firstName, "John", sizeof(firstName));
        strncpy(lastName, "Doe", sizeof(lastName));
        strncpy(resume, "Master Degree.", sizeof(resume));
        salary = 2200.0;

        ret = SQLExecute(stmt);

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to execute prepared statement");

        ret = SQLMoreResults(stmt);

        if (ret != SQL_NO_DATA)
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "No more data expected");

        ++key;
        orgId = 1;
        strncpy(firstName, "Jane", sizeof(firstName));
        strncpy(lastName, "Doe", sizeof(lastName));
        strncpy(resume, "Bachelor Degree.", sizeof(resume));
        salary = 1300.0;

        ret = SQLExecute(stmt);

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to execute prepared statement");


        ret = SQLMoreResults(stmt);

        if (ret != SQL_NO_DATA)
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "No more data expected");

        ++key;
        orgId = 2;
        strncpy(firstName, "John", sizeof(firstName));
        strncpy(lastName, "Smith", sizeof(lastName));
        strncpy(resume, "Bachelor Degree.", sizeof(resume));
        salary = 1700.0;

        ret = SQLExecute(stmt);

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to execute prepared statement");

        ret = SQLMoreResults(stmt);

        if (ret != SQL_NO_DATA)
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "No more data expected");

        ++key;
        orgId = 2;
        strncpy(firstName, "Jane", sizeof(firstName));
        strncpy(lastName, "Smith", sizeof(lastName));
        strncpy(resume, "Master Degree.", sizeof(resume));
        salary = 2500.0;

        ret = SQLExecute(stmt);

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to execute prepared statement");

        ret = SQLMoreResults(stmt);

        if (ret != SQL_NO_DATA)
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "No more data expected");

        ++key;
        orgId = 2;
        strncpy(firstName, "John", sizeof(firstName));
        strncpy(lastName, "Roe", sizeof(lastName));
        strncpy(resume, "Bachelor Degree.", sizeof(resume));
        salary = 1500.0;

        ret = SQLExecute(stmt);

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to execute prepared statement");

        ret = SQLMoreResults(stmt);

        if (ret != SQL_NO_DATA)
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "No more data expected");

        ++key;
        orgId = 2;
        strncpy(firstName, "Jane", sizeof(firstName));
        strncpy(lastName, "Roe", sizeof(lastName));
        strncpy(resume, "Bachelor Degree.", sizeof(resume));
        salary = 1000.0;

        ret = SQLExecute(stmt);

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to execute prepared statement");

        ret = SQLMoreResults(stmt);

        if (ret != SQL_NO_DATA)
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "No more data expected");

        ++key;
        orgId = 1;
        strncpy(firstName, "Richard", sizeof(firstName));
        strncpy(lastName, "Miles", sizeof(lastName));
        strncpy(resume, "Master Degree.", sizeof(resume));
        salary = 2400.0;

        ret = SQLExecute(stmt);

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to execute prepared statement");

        ret = SQLMoreResults(stmt);

        if (ret != SQL_NO_DATA)
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "No more data expected");

        ++key;
        orgId = 2;
        strncpy(firstName, "Mary", sizeof(firstName));
        strncpy(lastName, "Major", sizeof(lastName));
        strncpy(resume, "Bachelor Degree.", sizeof(resume));
        salary = 900.0;

        ret = SQLExecute(stmt);

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to execute prepared statement");
    }
    catch(...)
    {
        // Releasing statement handle.
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);

        // Re-throwing expection.
        throw;
    }

    // Releasing statement handle.
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/**
 * Populate Organization cache with sample data.
 * 
 * @param dbc Database connection.
 */
void PopulateOrganization(SQLHDBC dbc)
{
    SQLHSTMT stmt;

    // Allocate a statement handle
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

    if (!SQL_SUCCEEDED(ret))
        ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to allocate statement handle");

    try
    {
        SQLCHAR query1[] = "INSERT INTO \"Organization\".Organization (_key, name) VALUES (1L, 'Microsoft')";

        ret = SQLExecDirect(stmt, query1, static_cast<SQLSMALLINT>(sizeof(query1)));

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to execute query");

        SQLFreeStmt(stmt, SQL_CLOSE);

        SQLCHAR query2[] = "INSERT INTO \"Organization\".Organization (_key, name) VALUES (2L, 'Red Cross')";

        ret = SQLExecDirect(stmt, query2, static_cast<SQLSMALLINT>(sizeof(query2)));

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to execute query");
    }
    catch (...)
    {
        // Releasing statement handle.
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);

        // Re-throwing expection.
        throw;
    }

    // Releasing statement handle.
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/**
 * Adjust salary for specified employee.
 *
 * @param dbc Database connection.
 * @param key Person key.
 * @param salary New salary.
 */
void AdjustSalary(SQLHDBC dbc, int64_t key, double salary)
{
    SQLHSTMT stmt;

    // Allocate a statement handle
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

    if (!SQL_SUCCEEDED(ret))
        ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to allocate statement handle");

    try
    {
        SQLCHAR query[] = "UPDATE Person SET salary=? WHERE _key=?";

        ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, &salary, 0, 0);

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to bind parameter");

        ret = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key, 0, 0);

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to bind parameter");

        ret = SQLExecDirect(stmt, query, static_cast<SQLSMALLINT>(sizeof(query)));

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to execute query");
    }
    catch (...)
    {
        // Releasing statement handle.
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);

        // Re-throwing expection.
        throw;
    }

    // Releasing statement handle.
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/**
 * Remove specified person.
 *
 * @param dbc Database connection.
 * @param key Person key.
 */
void DeletePerson(SQLHDBC dbc, int64_t key)
{
    SQLHSTMT stmt;

    // Allocate a statement handle
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

    if (!SQL_SUCCEEDED(ret))
        ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to allocate statement handle");

    try
    {
        SQLCHAR query[] = "DELETE FROM Person WHERE _key=?";

        ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key, 0, 0);

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to bind parameter");

        ret = SQLExecDirect(stmt, query, static_cast<SQLSMALLINT>(sizeof(query)));

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt, "Failed to execute query");
    }
    catch (...)
    {
        // Releasing statement handle.
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);

        // Re-throwing expection.
        throw;
    }

    // Releasing statement handle.
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}

/**
 * Query tables.
 * 
 * @param dbc Database connection.
 */
void QueryData(SQLHDBC dbc)
{
    std::cout << std::endl;
    std::cout << ">>> Getting list of persons:" << std::endl;

    GetDataWithOdbc(dbc, "SELECT firstName, lastName, resume, salary FROM Person");

    std::cout << std::endl;
    std::cout << ">>> Getting average salary by degree:" << std::endl;

    GetDataWithOdbc(dbc, "SELECT resume, AVG(salary) FROM Person GROUP BY resume");

    std::cout << std::endl;
    std::cout << ">>> Getting people with organizations:" << std::endl;

    GetDataWithOdbc(dbc, "SELECT firstName, lastName, Organization.name FROM Person "
        "INNER JOIN \"Organization\".Organization ON Person.orgId = Organization._KEY");
}

/**
 * Program entry point.
 *
 * @return Exit code.
 */
int main() 
{
    IgniteConfiguration cfg;

    cfg.springCfgPath = "platforms/cpp/examples/odbc-example/config/example-odbc.xml";

    try
    {
        // Start a node.
        Ignite grid = Ignition::Start(cfg);

        SQLHENV env;

        // Allocate an environment handle
        SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

        // We want ODBC 3 support
        SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(SQL_OV_ODBC3), 0);

        SQLHDBC dbc;

        // Allocate a connection handle
        SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

        // Combining connect string
        std::string connectStr = "DRIVER={Apache Ignite};SERVER=localhost;PORT=10800;SCHEMA=Person;";

        SQLCHAR outstr[ODBC_BUFFER_SIZE];
        SQLSMALLINT outstrlen;

        // Connecting to ODBC server.
        SQLRETURN ret = SQLDriverConnect(dbc, NULL, reinterpret_cast<SQLCHAR*>(&connectStr[0]),
            static_cast<SQLSMALLINT>(connectStr.size()), outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_DBC, dbc, "Failed to connect");

        std::cout << std::endl;
        std::cout << ">>> Cache ODBC example started." << std::endl;
        std::cout << std::endl;

        // Populate caches.
        PopulatePerson(dbc);
        PopulateOrganization(dbc);

        QueryData(dbc);

        std::cout << std::endl;
        std::cout << std::endl;
        std::cout << ">>> Adjusted salary for Mary Major. Querying again." << std::endl;

        AdjustSalary(dbc, 8, 1200.0);

        QueryData(dbc);

        std::cout << std::endl;
        std::cout << std::endl;
        std::cout << ">>> Removing several employees. Querying again." << std::endl;

        DeletePerson(dbc, 4);
        DeletePerson(dbc, 5);

        QueryData(dbc);

        // Disconneting from the server.
        SQLDisconnect(dbc);

        // Releasing allocated handles.
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        SQLFreeHandle(SQL_HANDLE_ENV, env);

        // Stop node.
        Ignition::StopAll(false);
    }
    catch (IgniteError& err)
    {
        std::cout << "An error occurred: " << err.GetText() << std::endl;

        return err.GetCode();
    }

    std::cout << std::endl;
    std::cout << ">>> Example finished, press 'Enter' to exit ..." << std::endl;
    std::cout << std::endl;

    std::cin.get();

    return 0;
}
