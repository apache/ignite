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

/** Read buffer size. */
enum { BUFFER_SIZE = 1024 };

/**
 * Represents simple string buffer.
 */
struct StringBuffer
{
    SQLCHAR buffer[BUFFER_SIZE];
    SQLLEN reallen;
};

/**
 * Print result set returned by query.
 *
 * @param stmt Statement.
 */
void PrintResultSet(SQLHSTMT stmt)
{
    SQLSMALLINT columnsCnt = 0;

    // Getting number of columns in result set.
    SQLNumResultCols(stmt, &columnsCnt);

    std::vector<StringBuffer> columns(columnsCnt);

    // Binding colums.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
        SQLBindCol(stmt, i + 1, SQL_CHAR, columns[i].buffer, BUFFER_SIZE, &columns[i].reallen);

    std::cout << "Result set:" << std::endl;

    while (true)
    {
        SQLRETURN ret = SQLFetch(stmt);

        if (!SQL_SUCCEEDED(ret))
            break;

        for (int i = 0; i < columns.size(); ++i)
            std::cout << std::setw(16) << columns[i].buffer << " ";

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
std::string GetErrorMessage(SQLSMALLINT handleType, SQLHANDLE handle)
{
    SQLCHAR sqlstate[7] = {};
    SQLINTEGER nativeCode;

    SQLCHAR message[BUFFER_SIZE];
    SQLSMALLINT reallen = 0;

    SQLGetDiagRec(handleType, handle, 1, sqlstate, &nativeCode, message, BUFFER_SIZE, &reallen);

    return std::string(reinterpret_cast<char*>(sqlstate)) + ": " + std::string(reinterpret_cast<char*>(message), reallen);
}

/**
 * Program entry point.
 *
 * @return Exit code.
 */
int main() 
{
    SQLHENV env;

    // Allocate an environment handle
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

    // We want ODBC 3 support
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(SQL_OV_ODBC3), 0);

    SQLHDBC dbc;

    // Allocate a connection handle
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    std::string cacheName;
    std::string hostName;
    std::string port;

    // Reading hostname.
    std::cout << "Hostname [localhost]: ";
    std::getline(std::cin, hostName);

    // Fallback to default value.
    if (hostName.empty())
        hostName = "localhost";

    // Reading port.
    std::cout << "Port [11443]: ";
    std::getline(std::cin, port);

    // Fallback to default value.
    if (port.empty())
        port = "11443";

    // Reading cache name.
    std::cout << "Cache name: ";
    std::getline(std::cin, cacheName);

    // Combining connect string
    std::string connectStr = "DRIVER={Apache Ignite};"
                             "SERVER=" + hostName + ";"
                             "PORT=" + port + ";"
                             "CACHE=" + cacheName;

    SQLCHAR outstr[BUFFER_SIZE];
    SQLSMALLINT outstrlen;

    std::cout << "Trying to connect using following connection string: \""
              << connectStr << "\"" << std::endl;

    SQLRETURN ret = SQLDriverConnect(dbc, NULL, reinterpret_cast<SQLCHAR*>(&connectStr[0]),
        static_cast<SQLSMALLINT>(connectStr.size()), outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

    if (!SQL_SUCCEEDED(ret))
    {
        std::cerr << "Failed to connect: " << GetErrorMessage(SQL_HANDLE_DBC, dbc) << std::endl;

        return -1;
    }

    std::cout << "Connected" << std::endl;
    std::cout << "Returned connection string was: " << outstr << std::endl;

    SQLHSTMT stmt;

    // Allocate a statement handle
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

    while (true)
    {
        std::string query;

        // Reading query.
        std::cout << "SQL query (Empty input to exit) : ";
        std::getline(std::cin, query);

        if (query.empty())
            break;

        ret = SQLExecDirect(stmt, reinterpret_cast<SQLCHAR*>(&query[0]), static_cast<SQLSMALLINT>(query.size()));

        if (!SQL_SUCCEEDED(ret))
        {
            std::cerr << "Failed to execute query: " << GetErrorMessage(SQL_HANDLE_STMT, stmt) << std::endl;

            continue;
        }

        // Printing result.
        PrintResultSet(stmt);
    }

    std::cout << "Disconnecting" << std::endl;

    // Releasing statement handle.
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    // Disconneting from the server.
    SQLDisconnect(dbc);

    // Releasing allocated handles.
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}