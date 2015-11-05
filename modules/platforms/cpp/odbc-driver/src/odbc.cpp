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
#   define _WINSOCKAPI_
#   include <windows.h>
#endif //_WIN32

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <algorithm>

#include <sqlext.h>
#include <odbcinst.h>

#include "utility.h"
#include "configuration.h"
#include "environment.h"
#include "connection.h"

#undef min

FILE* log_file = NULL;

void logInit(const char* path)
{
    if (!log_file)
    {
        log_file = fopen(path, "w");
    }
}


SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE* result)
{
    LOG_MSG("SQLAllocHandle called\n");
    switch (type)
    {
        case SQL_HANDLE_ENV:
            return SQLAllocEnv(result);

        case SQL_HANDLE_DBC:
            return SQLAllocConnect(parent, result);

        case SQL_HANDLE_STMT:
            return SQLAllocStmt(parent, result);

        case SQL_HANDLE_DESC:
        default:
            break;
    }

    *result = 0;
    return SQL_ERROR;
}

SQLRETURN SQL_API SQLAllocEnv(SQLHENV *env)
{
    using ignite::odbc::Environment;

    LOG_MSG("SQLAllocEnv called\n");

    *env = reinterpret_cast<SQLHENV>(new Environment());

    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLAllocConnect(SQLHENV env, SQLHDBC *conn)
{
    using ignite::odbc::Environment;
    using ignite::odbc::Connection;

    LOG_MSG("SQLAllocConnect called\n");

    *conn = SQL_NULL_HDBC;

    Environment *environment = reinterpret_cast<Environment*>(env);

    if (!environment)
        return SQL_INVALID_HANDLE;
    
    Connection *connection = environment->CreateConnection();

    if (!connection)
        return SQL_ERROR;

    *conn = reinterpret_cast<SQLHDBC>(connection);

    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLAllocStmt(SQLHDBC conn, SQLHSTMT *stmt)
{
    LOG_MSG("SQLAllocStmt called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT type, SQLHANDLE handle)
{
    LOG_MSG("SQLFreeHandle called\n");

    switch (type)
    {
        case SQL_HANDLE_ENV:
            return SQLFreeEnv(handle);

        case SQL_HANDLE_DBC:
            return SQLFreeConnect(handle);

        case SQL_HANDLE_STMT:
            return SQLFreeStmt(handle, SQL_DROP);

        case SQL_HANDLE_DESC:
        default:
            break;
    }

    return SQL_ERROR;
}

SQLRETURN SQL_API SQLFreeEnv(SQLHENV env)
{
    using ignite::odbc::Environment;

    LOG_MSG("SQLFreeEnv called\n");

    Environment *environment = reinterpret_cast<Environment*>(env);

    if (!environment)
        return SQL_INVALID_HANDLE;

    delete environment;

    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLFreeConnect(SQLHDBC conn)
{
    using ignite::odbc::Connection;

    Connection *connection = reinterpret_cast<Connection*>(conn);

    if (!connection)
        return SQL_INVALID_HANDLE;

    delete connection;

    LOG_MSG("SQLFreeConnect called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT stmt, SQLUSMALLINT option)
{
    LOG_MSG("SQLFreeStmt called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLDriverConnect(
    SQLHDBC         ConnectionHandle,
    SQLHWND         WindowHandle,
    SQLCHAR*        InConnectionString,
    SQLSMALLINT     StringLength1,
    SQLCHAR*        OutConnectionString,
    SQLSMALLINT     BufferLength,
    SQLSMALLINT*    StringLength2Ptr,
    SQLUSMALLINT    DriverCompletion)
{
    using ignite::odbc::Connection;

    UNREFERENCED_PARAMETER(WindowHandle);

    LOG_MSG("SQLDriverConnect called\n");

    Connection *connection = reinterpret_cast<Connection*>(ConnectionHandle);

    if (!connection)
        return SQL_INVALID_HANDLE;

    if (StringLength1 == SQL_NTS && InConnectionString)
        StringLength1 = static_cast<SQLSMALLINT>(strlen((char*)InConnectionString));

    ignite::odbc::Configuration config;

    config.FillFromConnectString(reinterpret_cast<const char*>(InConnectionString), StringLength1);

    bool connected = connection->Establish(config.GetServetHost(), config.GetServetPort());

    if (!connected)
        return SQL_ERROR;

    if (OutConnectionString && BufferLength > 0)
    {
        std::string out_connection_str = config.ToConnectString();

        strncpy((char*)OutConnectionString, out_connection_str.c_str(), BufferLength - 1);

        OutConnectionString[BufferLength - 1] = '\0';

        if (StringLength2Ptr)
            *StringLength2Ptr = std::min(BufferLength - 1, static_cast<int>(out_connection_str.size()));
    }

    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLDisconnect(SQLHDBC conn)
{
    using ignite::odbc::Connection;

    LOG_MSG("SQLDisconnect called\n");

    Connection *connection = reinterpret_cast<Connection*>(conn);

    if (!connection)
        return SQL_INVALID_HANDLE;

    bool success = connection->Release();

    return success ? SQL_SUCCESS : SQL_ERROR;
}

SQLRETURN SQL_API SQLExecDirect(
    SQLHSTMT		hStmt,
    UCHAR*		sStmtText,
    SDWORD		iStmtLen)
{
    LOG_MSG("SQLExecDirect called\n");
    return SQL_SUCCESS;
}

///// SQLBindCol /////

SQLRETURN SQL_API SQLBindCol(SQLHSTMT arg0,
    UWORD arg1,
    SWORD arg2,
    PTR arg3,
    SDWORD arg4,
    UNALIGNED SDWORD * arg5)
{
    LOG_MSG("SQLBindCol called\n");
    return SQL_SUCCESS;
}

///// SQLCancel /////

SQLRETURN SQL_API SQLCancel(SQLHSTMT arg0)
{
    LOG_MSG("SQLCancel called\n");
    return SQL_SUCCESS;
}

///// SQLColAttributes /////

SQLRETURN SQL_API SQLColAttributes(SQLHSTMT arg0,
    UWORD arg1,
    UWORD arg2,
    PTR arg3,
    SWORD arg4,
    UNALIGNED SWORD * arg5,
    UNALIGNED SDWORD * arg6)
{
    LOG_MSG("SQLColAttributes called\n");
    return SQL_SUCCESS;
}

///// SQLConnect /////

SQLRETURN SQL_API SQLConnect(SQLHDBC arg0,
    UCHAR * arg1,
    SWORD arg2,
    UCHAR * arg3,
    SWORD arg4,
    UCHAR * arg5,
    SWORD arg6)
{
    LOG_MSG("SQLConnect called\n");
    return SQL_SUCCESS;
}

///// SQLDescribeCol /////

SQLRETURN SQL_API SQLDescribeCol(SQLHSTMT arg0,
    UWORD arg1,
    UCHAR * arg2,
    SWORD arg3,
    SWORD * arg4,
    UNALIGNED SWORD * arg5,
    UNALIGNED UDWORD * arg6,
    UNALIGNED SWORD * arg7,
    UNALIGNED SWORD * arg8)
{
    LOG_MSG("SQLDescribeCol called\n");
    return SQL_SUCCESS;
}

///// SQLError /////

SQLRETURN SQL_API SQLError(HENV arg0,
    SQLHDBC arg1,
    SQLHSTMT arg2,
    UCHAR * arg3,
    UNALIGNED SDWORD * arg4,
    UCHAR * arg5,
    SWORD arg6,
    UNALIGNED SWORD * arg7)
{
    LOG_MSG("SQLError called\n");
    return(SQL_NO_DATA_FOUND);
}


///// SQLExecute /////

SQLRETURN SQL_API SQLExecute(SQLHSTMT arg0)
{
    LOG_MSG("SQLExecute called\n");
    return SQL_SUCCESS;
}

///// SQLFetch /////

SQLRETURN SQL_API SQLFetch(SQLHSTMT arg0)
{
    LOG_MSG("SQLFetch called\n");
    return SQL_SUCCESS;
}

///// SQLGetCursorName /////

SQLRETURN SQL_API SQLGetCursorName(SQLHSTMT arg0,
    UCHAR * arg1,
    SWORD arg2,
    UNALIGNED SWORD * arg3)
{
    LOG_MSG("SQLGetCursorName called\n");
    return SQL_SUCCESS;
}

///// SQLNumResultCols /////

SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT arg0,
    UNALIGNED SWORD * arg1)
{
    LOG_MSG("SQLNumResultCols called\n");
    return SQL_SUCCESS;
}

///// SQLPrepare /////

SQLRETURN SQL_API SQLPrepare(SQLHSTMT arg0,
    UCHAR * arg1,
    SDWORD arg2)
{
    LOG_MSG("SQLPrepare called\n");
    return SQL_SUCCESS;
}

///// SQLRowCount /////

SQLRETURN SQL_API SQLRowCount(SQLHSTMT arg0,
    UNALIGNED SDWORD * arg1)
{
    LOG_MSG("SQLRowCount called\n");
    return SQL_SUCCESS;
}

///// SQLSetCursorName /////

SQLRETURN SQL_API SQLSetCursorName(SQLHSTMT arg0,
    UCHAR * arg1,
    SWORD arg2)
{
    LOG_MSG("SQLSetCursorName called\n");
    return SQL_SUCCESS;
}

///// SQLSetParam /////

SQLRETURN SQL_API SQLSetParam(SQLHSTMT arg0,
    UWORD arg1,
    SWORD arg2,
    SWORD arg3,
    UDWORD arg4,
    SWORD arg5,
    PTR arg6,
    UNALIGNED SDWORD * arg7)
{
    LOG_MSG("SQLSetParam called\n");
    return SQL_SUCCESS;
}

///// SQLTransact /////

SQLRETURN SQL_API SQLTransact(HENV arg0,
    SQLHDBC arg1,
    UWORD arg2)
{
    LOG_MSG("SQLTransact called\n");
    return SQL_SUCCESS;
}

///// SQLColumns /////

SQLRETURN SQL_API SQLColumns(SQLHSTMT arg0,
    UCHAR * arg1,
    SWORD arg2,
    UCHAR * arg3,
    SWORD arg4,
    UCHAR * arg5,
    SWORD arg6,
    UCHAR * arg7,
    SWORD arg8)
{
    LOG_MSG("SQLColumns called\n");
    return SQL_SUCCESS;
}

///// SQLGetConnectOption /////

SQLRETURN SQL_API SQLGetConnectOption(SQLHDBC arg0,
    UWORD arg1,
    PTR arg2)
{
    LOG_MSG("SQLGetConnectOption called\n");
    return SQL_SUCCESS;
}

///// SQLGetData /////

SQLRETURN SQL_API SQLGetData(SQLHSTMT arg0,
    UWORD arg1,
    SWORD arg2,
    PTR arg3,
    SDWORD arg4,
    UNALIGNED SDWORD * arg5)
{
    LOG_MSG("SQLGetData called\n");
    return SQL_SUCCESS;
}

///// SQLGetFunctions /////

SQLRETURN SQL_API SQLGetFunctions(SQLHDBC arg0,
    UWORD arg1,
    UWORD  * arg2)
{
    LOG_MSG("SQLGetFunctions called\n");
    return SQL_SUCCESS;
}

///// SQLGetInfo /////

SQLRETURN SQL_API SQLGetInfo(SQLHDBC arg0,
    UWORD arg1,
    PTR arg2,
    SWORD arg3,
    UNALIGNED SWORD * arg4)
{
    LOG_MSG("SQLGetInfo called\n");
    return SQL_SUCCESS;
}

///// SQLGetStmtOption /////

SQLRETURN SQL_API SQLGetStmtOption(SQLHSTMT arg0,
    UWORD arg1,
    PTR arg2)
{
    LOG_MSG("SQLGetStmtOption called\n");
    return SQL_SUCCESS;
}

///// SQLGetTypeInfo /////

SQLRETURN SQL_API SQLGetTypeInfo(SQLHSTMT arg0,
    SWORD arg1)
{
    LOG_MSG("SQLGetTypeInfo called\n");
    return SQL_SUCCESS;
}

///// SQLParamData /////

SQLRETURN SQL_API SQLParamData(SQLHSTMT arg0,
    PTR * arg1)
{
    LOG_MSG("SQLParamData called\n");
    return SQL_SUCCESS;
}

///// SQLPutData /////

SQLRETURN SQL_API SQLPutData(SQLHSTMT arg0,
    PTR arg1,
    SDWORD arg2)
{
    LOG_MSG("SQLPutData called\n");
    return SQL_SUCCESS;
}

///// SQLSetConnectOption /////

SQLRETURN SQL_API SQLSetConnectOption(SQLHDBC arg0,
    UWORD arg1,
    UDWORD arg2)
{
    LOG_MSG("SQLSetConnectOption called\n");
    return SQL_SUCCESS;
}

///// SQLSetStmtOption /////

SQLRETURN SQL_API SQLSetStmtOption(SQLHSTMT arg0,
    UWORD arg1,
    UDWORD arg2)
{
    LOG_MSG("SQLSetStmtOption called\n");
    return SQL_SUCCESS;
}

///// SQLSpecialColumns /////

SQLRETURN SQL_API SQLSpecialColumns(SQLHSTMT arg0,
    UWORD arg1,
    UCHAR * arg2,
    SWORD arg3,
    UCHAR * arg4,
    SWORD arg5,
    UCHAR * arg6,
    SWORD arg7,
    UWORD arg8,
    UWORD arg9)
{
    LOG_MSG("SQLSpecialColumns called\n");
    return SQL_SUCCESS;
}

///// SQLStatistics /////

SQLRETURN SQL_API SQLStatistics(SQLHSTMT arg0,
    UCHAR * arg1,
    SWORD arg2,
    UCHAR * arg3,
    SWORD arg4,
    UCHAR * arg5,
    SWORD arg6,
    UWORD arg7,
    UWORD arg8)
{
    LOG_MSG("SQLStatistics called\n");
    return SQL_SUCCESS;
}

///// SQLTables /////

SQLRETURN SQL_API SQLTables(SQLHSTMT arg0,
    UCHAR * arg1,
    SWORD arg2,
    UCHAR * arg3,
    SWORD arg4,
    UCHAR * arg5,
    SWORD arg6,
    UCHAR * arg7,
    SWORD arg8)
{
    LOG_MSG("SQLTables called\n");
    return SQL_SUCCESS;
}

///// SQLBrowseConnect /////

SQLRETURN SQL_API SQLBrowseConnect(SQLHDBC arg0,
    UCHAR * arg1,
    SWORD arg2,
    UCHAR * arg3,
    SWORD arg4,
    UNALIGNED SWORD * arg5)
{
    LOG_MSG("SQLBrowseConnect called\n");
    return SQL_SUCCESS;
}

///// SQLColumnPrivileges /////

SQLRETURN SQL_API SQLColumnPrivileges(SQLHSTMT arg0,
    UCHAR * arg1,
    SWORD arg2,
    UCHAR * arg3,
    SWORD arg4,
    UCHAR * arg5,
    SWORD arg6,
    UCHAR * arg7,
    SWORD arg8)
{
    LOG_MSG("SQLColumnPrivileges called\n");
    return SQL_SUCCESS;
}

///// SQLDataSources /////

SQLRETURN SQL_API SQLDataSources(HENV arg0,
    UWORD arg1,
    UCHAR * arg2,
    SWORD arg3,
    SWORD * arg4,
    UCHAR * arg5,
    SWORD arg6,
    SWORD * arg7)
{
    LOG_MSG("SQLDataSources called\n");
    return SQL_SUCCESS;
}

///// SQLDescribeParam /////

SQLRETURN SQL_API SQLDescribeParam(SQLHSTMT arg0,
    UWORD arg1,
    UNALIGNED SWORD * arg2,
    UNALIGNED UDWORD * arg3,
    UNALIGNED SWORD * arg4,
    UNALIGNED SWORD * arg5)
{
    LOG_MSG("SQLDescribeParam called\n");
    return SQL_SUCCESS;
}

///// SQLExtendedFetch /////

SQLRETURN SQL_API SQLExtendedFetch(SQLHSTMT arg0,
    UWORD arg1,
    SDWORD arg2,
    UNALIGNED UDWORD * arg3,
    UNALIGNED UWORD * arg4)
{
    LOG_MSG("SQLExtendedFetch called\n");
    return SQL_SUCCESS;
}

///// SQLForeignKeys /////

SQLRETURN SQL_API SQLForeignKeys(SQLHSTMT arg0,
    UCHAR * arg1,
    SWORD arg2,
    UCHAR * arg3,
    SWORD arg4,
    UCHAR * arg5,
    SWORD arg6,
    UCHAR * arg7,
    SWORD arg8,
    UCHAR * arg9,
    SWORD arg10,
    UCHAR * arg11,
    SWORD arg12)
{
    LOG_MSG("SQLForeignKeys called\n");
    return SQL_SUCCESS;
}

///// SQLMoreResults /////

SQLRETURN SQL_API SQLMoreResults(SQLHSTMT arg0)
{
    LOG_MSG("SQLMoreResults called\n");
    return SQL_SUCCESS;
}

///// SQLNativeSql /////

SQLRETURN SQL_API SQLNativeSql(SQLHDBC arg0,
    UCHAR * arg1,
    SDWORD arg2,
    UCHAR * arg3,
    SDWORD arg4,
    UNALIGNED SDWORD * arg5)
{
    LOG_MSG("SQLNativeSql called\n");
    return SQL_SUCCESS;
}

///// SQLNumParams /////

SQLRETURN SQL_API SQLNumParams(SQLHSTMT arg0,
    UNALIGNED SWORD * arg1)
{
    LOG_MSG("SQLNumParams called\n");
    return SQL_SUCCESS;
}

///// SQLParamOptions /////

SQLRETURN SQL_API SQLParamOptions(SQLHSTMT arg0,
    UDWORD arg1,
    UNALIGNED UDWORD * arg2)
{
    LOG_MSG("SQLParamOptions called\n");
    return SQL_SUCCESS;
}

///// SQLPrimaryKeys /////

SQLRETURN SQL_API SQLPrimaryKeys(SQLHSTMT arg0,
    UCHAR * arg1,
    SWORD arg2,
    UCHAR * arg3,
    SWORD arg4,
    UCHAR * arg5,
    SWORD arg6)
{
    LOG_MSG("SQLPrimaryKeys called\n");
    return SQL_SUCCESS;
}

///// SQLProcedureColumns /////

SQLRETURN SQL_API SQLProcedureColumns(SQLHSTMT arg0,
    UCHAR * arg1,
    SWORD arg2,
    UCHAR * arg3,
    SWORD arg4,
    UCHAR * arg5,
    SWORD arg6,
    UCHAR * arg7,
    SWORD arg8)
{
    LOG_MSG("SQLProcedureColumns called\n");
    return SQL_SUCCESS;
}

///// SQLProcedures /////

SQLRETURN SQL_API SQLProcedures(SQLHSTMT arg0,
    UCHAR * arg1,
    SWORD arg2,
    UCHAR * arg3,
    SWORD arg4,
    UCHAR * arg5,
    SWORD arg6)
{
    LOG_MSG("SQLProcedures called\n");
    return SQL_SUCCESS;
}

///// SQLSetPos /////

SQLRETURN SQL_API SQLSetPos(SQLHSTMT arg0,
    UWORD arg1,
    UWORD arg2,
    UWORD arg3)
{
    LOG_MSG("SQLSetPos called\n");
    return SQL_SUCCESS;
}

///// SQLSetScrollOptions /////

SQLRETURN SQL_API SQLSetScrollOptions(SQLHSTMT arg0,
    UWORD arg1,
    SDWORD arg2,
    UWORD arg3)
{
    LOG_MSG("SQLSetScrollOptions called\n");
    return SQL_SUCCESS;
}

///// SQLTablePrivileges /////

SQLRETURN SQL_API SQLTablePrivileges(SQLHSTMT arg0,
    UCHAR * arg1,
    SWORD arg2,
    UCHAR * arg3,
    SWORD arg4,
    UCHAR * arg5,
    SWORD arg6)
{
    LOG_MSG("SQLTablePrivileges called\n");
    return SQL_SUCCESS;
}

///// SQLDrivers /////

SQLRETURN SQL_API SQLDrivers(HENV arg0,
    UWORD arg1,
    UCHAR * arg2,
    SWORD arg3,
    SWORD * arg4,
    UCHAR * arg5,
    SWORD arg6,
    SWORD * arg7)
{
    LOG_MSG("SQLDrivers called\n");
    return SQL_SUCCESS;
}

///// SQLBindParameter /////

SQLRETURN SQL_API SQLBindParameter(SQLHSTMT arg0,
    UWORD arg1,
    SWORD arg2,
    SWORD arg3,
    SWORD arg4,
    UDWORD arg5,
    SWORD arg6,
    PTR arg7,
    SDWORD arg8,
    UNALIGNED SDWORD * arg9)
{
    LOG_MSG("SQLBindParameter called\n");
    return SQL_SUCCESS;
}

///// SQLBindParam /////

SQLRETURN SQL_API SQLBindParam(SQLHSTMT arg0,
    SQLUSMALLINT arg1,
    SQLSMALLINT arg2,
    SQLSMALLINT arg3,
    SQLUINTEGER arg4,
    SQLSMALLINT arg5,
    SQLPOINTER arg6,
    SQLINTEGER * arg7)
{
    LOG_MSG("SQLBindParam called\n");
    return SQL_SUCCESS;
}

///// SQLCloseCursor /////

SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT arg0)
{
    LOG_MSG("SQLCloseCursor called\n");
    return SQL_SUCCESS;
}

///// SQLColAttribute /////

SQLRETURN SQL_API SQLColAttribute(SQLHSTMT arg0,
    SQLUSMALLINT arg1,
    SQLUSMALLINT arg2,
    SQLPOINTER arg3,
    SQLSMALLINT arg4,
    UNALIGNED SQLSMALLINT * arg5,
    SQLPOINTER arg6)
{
    LOG_MSG("SQLColAttribute called\n");
    return SQL_SUCCESS;
}

///// SQLCopyDesc /////

SQLRETURN SQL_API SQLCopyDesc(SQLHDESC arg0,
    SQLHDESC arg1)
{
    LOG_MSG("SQLCopyDesc called\n");
    return SQL_SUCCESS;
}

///// SQLEndTran /////

SQLRETURN SQL_API SQLEndTran(SQLSMALLINT arg0,
    SQLHANDLE arg1,
    SQLSMALLINT arg2)
{
    LOG_MSG("SQLEndTran called\n");
    return SQL_SUCCESS;
}

///// SQLFetchScroll /////

SQLRETURN SQL_API SQLFetchScroll(SQLHSTMT arg0,
    SQLSMALLINT arg1,
    SQLINTEGER arg2)
{
    LOG_MSG("SQLFetchScroll called\n");
    return SQL_SUCCESS;
}

///// SQLGetConnectAttr /////

SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC arg0,
    SQLINTEGER arg1,
    SQLPOINTER arg2,
    SQLINTEGER arg3,
    UNALIGNED SQLINTEGER * arg4)
{
    LOG_MSG("SQLGetConnectAttr called\n");
    return SQL_SUCCESS;
}

///// SQLGetDescField /////

SQLRETURN SQL_API SQLGetDescField(SQLHDESC arg0,
    SQLSMALLINT arg1,
    SQLSMALLINT arg2,
    SQLPOINTER arg3,
    SQLINTEGER arg4,
    UNALIGNED SQLINTEGER * arg5)
{
    LOG_MSG("SQLGetDescField called\n");
    return SQL_SUCCESS;
}

///// SQLGetDescRec /////

SQLRETURN SQL_API SQLGetDescRec(SQLHDESC arg0,
    SQLSMALLINT arg1,
    SQLCHAR * arg2,
    SQLSMALLINT arg3,
    UNALIGNED SQLSMALLINT * arg4,
    UNALIGNED SQLSMALLINT * arg5,
    UNALIGNED SQLSMALLINT * arg6,
    UNALIGNED SQLINTEGER  * arg7,
    UNALIGNED SQLSMALLINT * arg8,
    UNALIGNED SQLSMALLINT * arg9,
    UNALIGNED SQLSMALLINT * arg10)
{
    LOG_MSG("SQLGetDescRec called\n");
    return SQL_SUCCESS;
}

///// SQLGetDiagField /////

SQLRETURN SQL_API SQLGetDiagField(SQLSMALLINT arg0,
    SQLHANDLE arg1,
    SQLSMALLINT arg2,
    SQLSMALLINT arg3,
    SQLPOINTER arg4,
    SQLSMALLINT arg5,
    UNALIGNED SQLSMALLINT * arg6)
{
    LOG_MSG("SQLGetDiagField called\n");
    return(SQL_NO_DATA_FOUND);
}

///// SQLGetDiagRec /////

SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT arg0,
    SQLHANDLE arg1,
    SQLSMALLINT arg2,
    SQLCHAR * arg3,
    UNALIGNED SQLINTEGER * arg4,
    SQLCHAR * arg5,
    SQLSMALLINT arg6,
    UNALIGNED SQLSMALLINT * arg7)
{
    LOG_MSG("SQLGetDiagRec called\n");
    return(SQL_NO_DATA_FOUND);
}

///// SQLGetEnvAttr /////

SQLRETURN SQL_API SQLGetEnvAttr(SQLHENV arg0,
    SQLINTEGER arg1,
    SQLPOINTER arg2,
    SQLINTEGER arg3,
    UNALIGNED SQLINTEGER * arg4)
{
    LOG_MSG("SQLGetEnvAttr called\n");
    return SQL_SUCCESS;
}

///// SQLGetStmtAttr /////

SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT arg0,
    SQLINTEGER arg1,
    SQLPOINTER arg2,
    SQLINTEGER arg3,
    UNALIGNED SQLINTEGER * arg4)
{
    LOG_MSG("SQLGetStmtAttr called\n");
    return SQL_SUCCESS;
}

///// SQLSetConnectAttr /////

SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC arg0,
    SQLINTEGER arg1,
    SQLPOINTER arg2,
    SQLINTEGER arg3)
{
    LOG_MSG("SQLSetConnectAttr called\n");
    return SQL_SUCCESS;
}

///// SQLSetDescField /////

SQLRETURN SQL_API SQLSetDescField(SQLHDESC arg0,
    SQLSMALLINT arg1,
    SQLSMALLINT arg2,
    SQLPOINTER arg3,
    SQLINTEGER arg4)
{
    LOG_MSG("SQLSetDescField called\n");
    return SQL_SUCCESS;
}

///// SQLSetDescRec /////

SQLRETURN SQL_API SQLSetDescRec(SQLHDESC arg0,
    SQLSMALLINT arg1,
    SQLSMALLINT arg2,
    SQLSMALLINT arg3,
    SQLINTEGER arg4,
    SQLSMALLINT arg5,
    SQLSMALLINT arg6,
    SQLPOINTER arg7,
    UNALIGNED SQLINTEGER * arg8,
    UNALIGNED SQLINTEGER * arg9)
{
    LOG_MSG("SQLSetDescRec called\n");
    return SQL_SUCCESS;
}

///// SQLSetEnvAttr /////

SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV arg0,
    SQLINTEGER arg1,
    SQLPOINTER arg2,
    SQLINTEGER arg3)
{
    LOG_MSG("SQLSetEnvAttr called\n");
    return SQL_SUCCESS;
}

///// SQLSetStmtAttr /////

SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT arg0,
    SQLINTEGER arg1,
    SQLPOINTER arg2,
    SQLINTEGER arg3)
{
    LOG_MSG("SQLSetStmtAttr called\n");
    return SQL_SUCCESS;
}


///// SQLBulkOperations /////

SQLRETURN SQL_API SQLBulkOperations(SQLHSTMT arg0,
    SQLSMALLINT arg1)
{
    LOG_MSG("SQLBulkOperations called\n");
    return SQL_SUCCESS;
}