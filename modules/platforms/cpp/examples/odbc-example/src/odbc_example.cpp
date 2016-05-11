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
using namespace cache;

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
 * Fetch cache data using ODBC interface.
 */
void GetDataWithOdbc(const std::string& query)
{
    SQLHENV env;

    // Allocate an environment handle
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

    // We want ODBC 3 support
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(SQL_OV_ODBC3), 0);

    SQLHDBC dbc;

    // Allocate a connection handle
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    // Combining connect string
    std::string connectStr = "DRIVER={Apache Ignite};SERVER=localhost;PORT=10800;CACHE=Person;";

    SQLCHAR outstr[ODBC_BUFFER_SIZE];
    SQLSMALLINT outstrlen;

    // Connecting to ODBC server.
    SQLRETURN ret = SQLDriverConnect(dbc, NULL, reinterpret_cast<SQLCHAR*>(&connectStr[0]),
        static_cast<SQLSMALLINT>(connectStr.size()), outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

    if (!SQL_SUCCEEDED(ret))
    {
        std::cerr << "Failed to connect: " << GetOdbcErrorMessage(SQL_HANDLE_DBC, dbc) << std::endl;

        return;
    }

    SQLHSTMT stmt;

    // Allocate a statement handle
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

    std::vector<SQLCHAR> buf(query.begin(), query.end());

    ret = SQLExecDirect(stmt, &buf[0], static_cast<SQLSMALLINT>(buf.size()));

    if (SQL_SUCCEEDED(ret))
        PrintOdbcResultSet(stmt);
    else
        std::cerr << "Failed to execute query: " << GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt) << std::endl;

    // Releasing statement handle.
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    // Disconneting from the server.
    SQLDisconnect(dbc);

    // Releasing allocated handles.
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

/**
 * Populate Person cache with sample data.
 * 
 * @param cache Cache instance.
 */
void Populate(Cache<int64_t, Person>& cache)
{
    std::map<int64_t, Person> persons;

    int64_t key = 0;
    persons[++key] = Person(1, "John", "Doe", "Master Degree.", 2200.0);
    persons[++key] = Person(1, "Jane", "Doe", "Bachelor Degree.", 1300.0);
    persons[++key] = Person(2, "John", "Smith", "Bachelor Degree.", 1700.0);
    persons[++key] = Person(2, "Jane", "Smith", "Master Degree.", 2500.0);
    persons[++key] = Person(2, "John", "Roe", "Bachelor Degree.", 1500.0);
    persons[++key] = Person(2, "Jane", "Roe", "Bachelor Degree.", 1000.0);
    persons[++key] = Person(1, "Richard", "Miles", "Master Degree.", 2400.0);
    persons[++key] = Person(2, "Mary", "Major", "Bachelor Degree.", 900.0);

    cache.PutAll(persons);
}

/**
 * Populate Organization cache with sample data.
 * 
 * @param cache Cache instance.
 */
void Populate(Cache<int64_t, Organization>& cache)
{
    std::map<int64_t, Organization> orgs;

    int64_t key = 0;
    orgs[++key] = Organization("Microsoft", Address("1096 Eddy Street, San Francisco, CA", 94109));
    orgs[++key] = Organization("Red Cross", Address("184 Fidler Drive, San Antonio, TX", 78205));

    cache.PutAll(orgs);
}

/**
 * Program entry point.
 *
 * @return Exit code.
 */
int main() 
{
    IgniteConfiguration cfg;

    cfg.jvmInitMem = 512;
    cfg.jvmMaxMem = 512;

    cfg.springCfgPath = "platforms/cpp/examples/odbc-example/config/example-odbc.xml";

    try
    {
        // Start a node.
        Ignite grid = Ignition::Start(cfg);

        std::cout << std::endl;
        std::cout << ">>> Cache ODBC example started." << std::endl;
        std::cout << std::endl;

        // Get Person cache instance.
        Cache<int64_t, Person> personCache = grid.GetCache<int64_t, Person>("Person");

        // Get Organization cache instance.
        Cache<int64_t, Organization> orgCache = grid.GetCache<int64_t, Organization>("Organization");

        // Clear caches.
        personCache.Clear();
        orgCache.Clear();

        // Populate caches.
        Populate(personCache);
        Populate(orgCache);

        std::cout << std::endl;
        std::cout << ">>> Getting list of persons:" << std::endl;

        GetDataWithOdbc("SELECT firstName, lastName, resume, salary FROM Person");

        std::cout << std::endl;
        std::cout << ">>> Getting average salary by degree:" << std::endl;

        GetDataWithOdbc("SELECT resume, AVG(salary) FROM Person GROUP BY resume");

        std::cout << std::endl;
        std::cout << ">>> Getting people with organizations:" << std::endl;

        GetDataWithOdbc("SELECT firstName, lastName, Organization.name FROM Person "
            "INNER JOIN \"Organization\".Organization ON Person.orgId = Organization._KEY");

        // Stop node.
        Ignition::StopAll(false);
    }
    catch (IgniteError& err)
    {
        std::cout << "An error occurred: " << err.GetText() << std::endl;
    }

    std::cout << std::endl;
    std::cout << ">>> Example finished, press any key to exit ..." << std::endl;
    std::cout << std::endl;

    std::cin.get();

    return 0;
}
