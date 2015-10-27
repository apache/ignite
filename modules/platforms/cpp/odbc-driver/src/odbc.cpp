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
#   define _CRT_SECURE_NO_WARNINGS 
#   include <windows.h>
#   define IGNITE_EXPORT __declspec(dllexport)
#   define IGNITE_IMPORT __declspec(dllimport)
#   define IGNITE_CALL __stdcall
#else
#   define IGNITE_EXPORT
#   define IGNITE_IMPORT
#   define IGNITE_CALL
#endif //_WIN32


#include <cstdlib>
#include <cstring>

#include <sqlext.h>
#include <odbcinst.h>

#include <cstdio>

FILE* log_file = NULL;

void logInit(const char* path)
{
    if (!log_file)
    {
        log_file = fopen(path, "w");
    }
}

#define LOG_MSG(fmt, ...) \
    logInit("D:\\odbc.log"); \
    fprintf(log_file, "%s: " fmt, __FUNCTION__);


///// SQLDriverConnect /////

RETCODE SQL_API SQLDriverConnect(
    HDBC        hDBC, HWND        hWnd,
    UCHAR*      szConnStrIn, SWORD       cbConnStrIn,
    UCHAR*      szConnStrOut, SWORD       cbConnStrOut,
    SWORD*      pcbConnStrOut, UWORD       uwMode)
{
    LOG_MSG("SQLDriverConnect called\n");

    if (cbConnStrIn == SQL_NTS && szConnStrIn)
        cbConnStrIn = strlen((char*)szConnStrIn);

    if (szConnStrOut && cbConnStrOut > 0)
    {
        strncpy((char*)szConnStrOut, (char*)szConnStrIn,
                (cbConnStrIn == SQL_NTS) ? cbConnStrOut - 1 :
                min(cbConnStrOut, cbConnStrIn));

        szConnStrOut[cbConnStrOut - 1] = '\0';
    }

    if (pcbConnStrOut)
        *pcbConnStrOut = cbConnStrIn;

    return SQL_SUCCESS;
}

///// SQLExecDirect /////

RETCODE SQL_API SQLExecDirect(
    HSTMT		hStmt,
    UCHAR*		sStmtText,
    SDWORD		iStmtLen)
{
    LOG_MSG("SQLExecDirect called\n");
    return SQL_SUCCESS;
}

///// SQLAllocHandle /////

RETCODE SQL_API SQLAllocHandle(
    SQLSMALLINT	HandleType,
    SQLHANDLE		HandleParent,
    SQLHANDLE*		NewHandlePointer)
{
    LOG_MSG("SQLAllocHandle called\n");
    return SQL_SUCCESS;
}
///// SQLAllocConnect /////

RETCODE SQL_API SQLAllocConnect(HENV arg0,
    HDBC * arg1)
{
    LOG_MSG("SQLAllocConnect called\n");
    return(SQL_SUCCESS);
}

///// SQLAllocEnv /////

RETCODE SQL_API SQLAllocEnv(HENV * arg0)
{
    LOG_MSG("SQLAllocEnv called\n");
    return(SQL_SUCCESS);
}

///// SQLAllocStmt /////

RETCODE SQL_API SQLAllocStmt(HDBC arg0,
    HSTMT * arg1)
{
    LOG_MSG("SQLAllocStmt called\n");
    return(SQL_SUCCESS);
}

///// SQLBindCol /////

RETCODE SQL_API SQLBindCol(HSTMT arg0,
    UWORD arg1,
    SWORD arg2,
    PTR arg3,
    SDWORD arg4,
    UNALIGNED SDWORD * arg5)
{
    LOG_MSG("SQLBindCol called\n");
    return(SQL_SUCCESS);
}

///// SQLCancel /////

RETCODE SQL_API SQLCancel(HSTMT arg0)
{
    LOG_MSG("SQLCancel called\n");
    return(SQL_SUCCESS);
}

///// SQLColAttributes /////

RETCODE SQL_API SQLColAttributes(HSTMT arg0,
    UWORD arg1,
    UWORD arg2,
    PTR arg3,
    SWORD arg4,
    UNALIGNED SWORD * arg5,
    UNALIGNED SDWORD * arg6)
{
    LOG_MSG("SQLColAttributes called\n");
    return(SQL_SUCCESS);
}

///// SQLConnect /////

RETCODE SQL_API SQLConnect(HDBC arg0,
    UCHAR * arg1,
    SWORD arg2,
    UCHAR * arg3,
    SWORD arg4,
    UCHAR * arg5,
    SWORD arg6)
{
    LOG_MSG("SQLConnect called\n");
    return(SQL_SUCCESS);
}

///// SQLDescribeCol /////

RETCODE SQL_API SQLDescribeCol(HSTMT arg0,
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
    return(SQL_SUCCESS);
}

///// SQLDisconnect /////

RETCODE SQL_API SQLDisconnect(HDBC arg0)
{
    LOG_MSG("SQLDisconnect called\n");
    return(SQL_SUCCESS);
}

///// SQLError /////

RETCODE SQL_API SQLError(HENV arg0,
    HDBC arg1,
    HSTMT arg2,
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

RETCODE SQL_API SQLExecute(HSTMT arg0)
{
    LOG_MSG("SQLExecute called\n");
    return(SQL_SUCCESS);
}

///// SQLFetch /////

RETCODE SQL_API SQLFetch(HSTMT arg0)
{
    LOG_MSG("SQLFetch called\n");
    return(SQL_SUCCESS);
}

///// SQLFreeConnect /////

RETCODE SQL_API SQLFreeConnect(HDBC arg0)
{
    LOG_MSG("SQLFreeConnect called\n");
    return(SQL_SUCCESS);
}

///// SQLFreeEnv /////

RETCODE SQL_API SQLFreeEnv(HENV arg0)
{
    LOG_MSG("SQLFreeEnv called\n");
    return(SQL_SUCCESS);
}

///// SQLFreeStmt /////

RETCODE SQL_API SQLFreeStmt(HSTMT arg0,
    UWORD arg1)
{
    LOG_MSG("SQLFreeStmt called\n");
    return(SQL_SUCCESS);
}

///// SQLGetCursorName /////

RETCODE SQL_API SQLGetCursorName(HSTMT arg0,
    UCHAR * arg1,
    SWORD arg2,
    UNALIGNED SWORD * arg3)
{
    LOG_MSG("SQLGetCursorName called\n");
    return(SQL_SUCCESS);
}

///// SQLNumResultCols /////

RETCODE SQL_API SQLNumResultCols(HSTMT arg0,
    UNALIGNED SWORD * arg1)
{
    LOG_MSG("SQLNumResultCols called\n");
    return(SQL_SUCCESS);
}

///// SQLPrepare /////

RETCODE SQL_API SQLPrepare(HSTMT arg0,
    UCHAR * arg1,
    SDWORD arg2)
{
    LOG_MSG("SQLPrepare called\n");
    return(SQL_SUCCESS);
}

///// SQLRowCount /////

RETCODE SQL_API SQLRowCount(HSTMT arg0,
    UNALIGNED SDWORD * arg1)
{
    LOG_MSG("SQLRowCount called\n");
    return(SQL_SUCCESS);
}

///// SQLSetCursorName /////

RETCODE SQL_API SQLSetCursorName(HSTMT arg0,
    UCHAR * arg1,
    SWORD arg2)
{
    LOG_MSG("SQLSetCursorName called\n");
    return(SQL_SUCCESS);
}

///// SQLSetParam /////

RETCODE SQL_API SQLSetParam(HSTMT arg0,
    UWORD arg1,
    SWORD arg2,
    SWORD arg3,
    UDWORD arg4,
    SWORD arg5,
    PTR arg6,
    UNALIGNED SDWORD * arg7)
{
    LOG_MSG("SQLSetParam called\n");
    return(SQL_SUCCESS);
}

///// SQLTransact /////

RETCODE SQL_API SQLTransact(HENV arg0,
    HDBC arg1,
    UWORD arg2)
{
    LOG_MSG("SQLTransact called\n");
    return(SQL_SUCCESS);
}

///// SQLColumns /////

RETCODE SQL_API SQLColumns(HSTMT arg0,
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
    return(SQL_SUCCESS);
}

///// SQLGetConnectOption /////

RETCODE SQL_API SQLGetConnectOption(HDBC arg0,
    UWORD arg1,
    PTR arg2)
{
    LOG_MSG("SQLGetConnectOption called\n");
    return(SQL_SUCCESS);
}

///// SQLGetData /////

RETCODE SQL_API SQLGetData(HSTMT arg0,
    UWORD arg1,
    SWORD arg2,
    PTR arg3,
    SDWORD arg4,
    UNALIGNED SDWORD * arg5)
{
    LOG_MSG("SQLGetData called\n");
    return(SQL_SUCCESS);
}

///// SQLGetFunctions /////

RETCODE SQL_API SQLGetFunctions(HDBC arg0,
    UWORD arg1,
    UWORD  * arg2)
{
    LOG_MSG("SQLGetFunctions called\n");
    return(SQL_SUCCESS);
}

///// SQLGetInfo /////

RETCODE SQL_API SQLGetInfo(HDBC arg0,
    UWORD arg1,
    PTR arg2,
    SWORD arg3,
    UNALIGNED SWORD * arg4)
{
    LOG_MSG("SQLGetInfo called\n");
    return(SQL_SUCCESS);
}

///// SQLGetStmtOption /////

RETCODE SQL_API SQLGetStmtOption(HSTMT arg0,
    UWORD arg1,
    PTR arg2)
{
    LOG_MSG("SQLGetStmtOption called\n");
    return(SQL_SUCCESS);
}

///// SQLGetTypeInfo /////

RETCODE SQL_API SQLGetTypeInfo(HSTMT arg0,
    SWORD arg1)
{
    LOG_MSG("SQLGetTypeInfo called\n");
    return(SQL_SUCCESS);
}

///// SQLParamData /////

RETCODE SQL_API SQLParamData(HSTMT arg0,
    PTR * arg1)
{
    LOG_MSG("SQLParamData called\n");
    return(SQL_SUCCESS);
}

///// SQLPutData /////

RETCODE SQL_API SQLPutData(HSTMT arg0,
    PTR arg1,
    SDWORD arg2)
{
    LOG_MSG("SQLPutData called\n");
    return(SQL_SUCCESS);
}

///// SQLSetConnectOption /////

RETCODE SQL_API SQLSetConnectOption(HDBC arg0,
    UWORD arg1,
    UDWORD arg2)
{
    LOG_MSG("SQLSetConnectOption called\n");
    return(SQL_SUCCESS);
}

///// SQLSetStmtOption /////

RETCODE SQL_API SQLSetStmtOption(HSTMT arg0,
    UWORD arg1,
    UDWORD arg2)
{
    LOG_MSG("SQLSetStmtOption called\n");
    return(SQL_SUCCESS);
}

///// SQLSpecialColumns /////

RETCODE SQL_API SQLSpecialColumns(HSTMT arg0,
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
    return(SQL_SUCCESS);
}

///// SQLStatistics /////

RETCODE SQL_API SQLStatistics(HSTMT arg0,
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
    return(SQL_SUCCESS);
}

///// SQLTables /////

RETCODE SQL_API SQLTables(HSTMT arg0,
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
    return(SQL_SUCCESS);
}

///// SQLBrowseConnect /////

RETCODE SQL_API SQLBrowseConnect(HDBC arg0,
    UCHAR * arg1,
    SWORD arg2,
    UCHAR * arg3,
    SWORD arg4,
    UNALIGNED SWORD * arg5)
{
    LOG_MSG("SQLBrowseConnect called\n");
    return(SQL_SUCCESS);
}

///// SQLColumnPrivileges /////

RETCODE SQL_API SQLColumnPrivileges(HSTMT arg0,
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
    return(SQL_SUCCESS);
}

///// SQLDataSources /////

RETCODE SQL_API SQLDataSources(HENV arg0,
    UWORD arg1,
    UCHAR * arg2,
    SWORD arg3,
    SWORD * arg4,
    UCHAR * arg5,
    SWORD arg6,
    SWORD * arg7)
{
    LOG_MSG("SQLDataSources called\n");
    return(SQL_SUCCESS);
}

///// SQLDescribeParam /////

RETCODE SQL_API SQLDescribeParam(HSTMT arg0,
    UWORD arg1,
    UNALIGNED SWORD * arg2,
    UNALIGNED UDWORD * arg3,
    UNALIGNED SWORD * arg4,
    UNALIGNED SWORD * arg5)
{
    LOG_MSG("SQLDescribeParam called\n");
    return(SQL_SUCCESS);
}

///// SQLExtendedFetch /////

RETCODE SQL_API SQLExtendedFetch(HSTMT arg0,
    UWORD arg1,
    SDWORD arg2,
    UNALIGNED UDWORD * arg3,
    UNALIGNED UWORD * arg4)
{
    LOG_MSG("SQLExtendedFetch called\n");
    return(SQL_SUCCESS);
}

///// SQLForeignKeys /////

RETCODE SQL_API SQLForeignKeys(HSTMT arg0,
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
    return(SQL_SUCCESS);
}

///// SQLMoreResults /////

RETCODE SQL_API SQLMoreResults(HSTMT arg0)
{
    LOG_MSG("SQLMoreResults called\n");
    return(SQL_SUCCESS);
}

///// SQLNativeSql /////

RETCODE SQL_API SQLNativeSql(HDBC arg0,
    UCHAR * arg1,
    SDWORD arg2,
    UCHAR * arg3,
    SDWORD arg4,
    UNALIGNED SDWORD * arg5)
{
    LOG_MSG("SQLNativeSql called\n");
    return(SQL_SUCCESS);
}

///// SQLNumParams /////

RETCODE SQL_API SQLNumParams(HSTMT arg0,
    UNALIGNED SWORD * arg1)
{
    LOG_MSG("SQLNumParams called\n");
    return(SQL_SUCCESS);
}

///// SQLParamOptions /////

RETCODE SQL_API SQLParamOptions(HSTMT arg0,
    UDWORD arg1,
    UNALIGNED UDWORD * arg2)
{
    LOG_MSG("SQLParamOptions called\n");
    return(SQL_SUCCESS);
}

///// SQLPrimaryKeys /////

RETCODE SQL_API SQLPrimaryKeys(HSTMT arg0,
    UCHAR * arg1,
    SWORD arg2,
    UCHAR * arg3,
    SWORD arg4,
    UCHAR * arg5,
    SWORD arg6)
{
    LOG_MSG("SQLPrimaryKeys called\n");
    return(SQL_SUCCESS);
}

///// SQLProcedureColumns /////

RETCODE SQL_API SQLProcedureColumns(HSTMT arg0,
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
    return(SQL_SUCCESS);
}

///// SQLProcedures /////

RETCODE SQL_API SQLProcedures(HSTMT arg0,
    UCHAR * arg1,
    SWORD arg2,
    UCHAR * arg3,
    SWORD arg4,
    UCHAR * arg5,
    SWORD arg6)
{
    LOG_MSG("SQLProcedures called\n");
    return(SQL_SUCCESS);
}

///// SQLSetPos /////

RETCODE SQL_API SQLSetPos(HSTMT arg0,
    UWORD arg1,
    UWORD arg2,
    UWORD arg3)
{
    LOG_MSG("SQLSetPos called\n");
    return(SQL_SUCCESS);
}

///// SQLSetScrollOptions /////

RETCODE SQL_API SQLSetScrollOptions(HSTMT arg0,
    UWORD arg1,
    SDWORD arg2,
    UWORD arg3)
{
    LOG_MSG("SQLSetScrollOptions called\n");
    return(SQL_SUCCESS);
}

///// SQLTablePrivileges /////

RETCODE SQL_API SQLTablePrivileges(HSTMT arg0,
    UCHAR * arg1,
    SWORD arg2,
    UCHAR * arg3,
    SWORD arg4,
    UCHAR * arg5,
    SWORD arg6)
{
    LOG_MSG("SQLTablePrivileges called\n");
    return(SQL_SUCCESS);
}

///// SQLDrivers /////

RETCODE SQL_API SQLDrivers(HENV arg0,
    UWORD arg1,
    UCHAR * arg2,
    SWORD arg3,
    SWORD * arg4,
    UCHAR * arg5,
    SWORD arg6,
    SWORD * arg7)
{
    LOG_MSG("SQLDrivers called\n");
    return(SQL_SUCCESS);
}

///// SQLBindParameter /////

RETCODE SQL_API SQLBindParameter(HSTMT arg0,
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
    return(SQL_SUCCESS);
}

///// SQLBindParam /////

RETCODE SQL_API SQLBindParam(SQLHSTMT arg0,
    SQLUSMALLINT arg1,
    SQLSMALLINT arg2,
    SQLSMALLINT arg3,
    SQLUINTEGER arg4,
    SQLSMALLINT arg5,
    SQLPOINTER arg6,
    SQLINTEGER * arg7)
{
    LOG_MSG("SQLBindParam called\n");
    return(SQL_SUCCESS);
}

///// SQLCloseCursor /////

RETCODE SQL_API SQLCloseCursor(SQLHSTMT arg0)
{
    LOG_MSG("SQLCloseCursor called\n");
    return(SQL_SUCCESS);
}

///// SQLColAttribute /////

RETCODE SQL_API SQLColAttribute(SQLHSTMT arg0,
    SQLUSMALLINT arg1,
    SQLUSMALLINT arg2,
    SQLPOINTER arg3,
    SQLSMALLINT arg4,
    UNALIGNED SQLSMALLINT * arg5,
    SQLPOINTER arg6)
{
    LOG_MSG("SQLColAttribute called\n");
    return(SQL_SUCCESS);
}

///// SQLCopyDesc /////

RETCODE SQL_API SQLCopyDesc(SQLHDESC arg0,
    SQLHDESC arg1)
{
    LOG_MSG("SQLCopyDesc called\n");
    return(SQL_SUCCESS);
}

///// SQLEndTran /////

RETCODE SQL_API SQLEndTran(SQLSMALLINT arg0,
    SQLHANDLE arg1,
    SQLSMALLINT arg2)
{
    LOG_MSG("SQLEndTran called\n");
    return(SQL_SUCCESS);
}

///// SQLFetchScroll /////

RETCODE SQL_API SQLFetchScroll(SQLHSTMT arg0,
    SQLSMALLINT arg1,
    SQLINTEGER arg2)
{
    LOG_MSG("SQLFetchScroll called\n");
    return(SQL_SUCCESS);
}

///// SQLFreeHandle /////

RETCODE SQL_API SQLFreeHandle(SQLSMALLINT arg0,
    SQLHANDLE arg1)
{
    LOG_MSG("SQLFreeHandle called\n");
    return(SQL_SUCCESS);
}

///// SQLGetConnectAttr /////

RETCODE SQL_API SQLGetConnectAttr(SQLHDBC arg0,
    SQLINTEGER arg1,
    SQLPOINTER arg2,
    SQLINTEGER arg3,
    UNALIGNED SQLINTEGER * arg4)
{
    LOG_MSG("SQLGetConnectAttr called\n");
    return(SQL_SUCCESS);
}

///// SQLGetDescField /////

RETCODE SQL_API SQLGetDescField(SQLHDESC arg0,
    SQLSMALLINT arg1,
    SQLSMALLINT arg2,
    SQLPOINTER arg3,
    SQLINTEGER arg4,
    UNALIGNED SQLINTEGER * arg5)
{
    LOG_MSG("SQLGetDescField called\n");
    return(SQL_SUCCESS);
}

///// SQLGetDescRec /////

RETCODE SQL_API SQLGetDescRec(SQLHDESC arg0,
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
    return(SQL_SUCCESS);
}

///// SQLGetDiagField /////

RETCODE SQL_API SQLGetDiagField(SQLSMALLINT arg0,
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

RETCODE SQL_API SQLGetDiagRec(SQLSMALLINT arg0,
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

RETCODE SQL_API SQLGetEnvAttr(SQLHENV arg0,
    SQLINTEGER arg1,
    SQLPOINTER arg2,
    SQLINTEGER arg3,
    UNALIGNED SQLINTEGER * arg4)
{
    LOG_MSG("SQLGetEnvAttr called\n");
    return(SQL_SUCCESS);
}

///// SQLGetStmtAttr /////

RETCODE SQL_API SQLGetStmtAttr(SQLHSTMT arg0,
    SQLINTEGER arg1,
    SQLPOINTER arg2,
    SQLINTEGER arg3,
    UNALIGNED SQLINTEGER * arg4)
{
    LOG_MSG("SQLGetStmtAttr called\n");
    return(SQL_SUCCESS);
}

///// SQLSetConnectAttr /////

RETCODE SQL_API SQLSetConnectAttr(SQLHDBC arg0,
    SQLINTEGER arg1,
    SQLPOINTER arg2,
    SQLINTEGER arg3)
{
    LOG_MSG("SQLSetConnectAttr called\n");
    return(SQL_SUCCESS);
}

///// SQLSetDescField /////

RETCODE SQL_API SQLSetDescField(SQLHDESC arg0,
    SQLSMALLINT arg1,
    SQLSMALLINT arg2,
    SQLPOINTER arg3,
    SQLINTEGER arg4)
{
    LOG_MSG("SQLSetDescField called\n");
    return(SQL_SUCCESS);
}

///// SQLSetDescRec /////

RETCODE SQL_API SQLSetDescRec(SQLHDESC arg0,
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
    return(SQL_SUCCESS);
}

///// SQLSetEnvAttr /////

RETCODE SQL_API SQLSetEnvAttr(SQLHENV arg0,
    SQLINTEGER arg1,
    SQLPOINTER arg2,
    SQLINTEGER arg3)
{
    LOG_MSG("SQLSetEnvAttr called\n");
    return(SQL_SUCCESS);
}

///// SQLSetStmtAttr /////

RETCODE SQL_API SQLSetStmtAttr(SQLHSTMT arg0,
    SQLINTEGER arg1,
    SQLPOINTER arg2,
    SQLINTEGER arg3)
{
    LOG_MSG("SQLSetStmtAttr called\n");
    return(SQL_SUCCESS);
}


///// SQLBulkOperations /////

RETCODE SQL_API SQLBulkOperations(SQLHSTMT arg0,
    SQLSMALLINT arg1)
{
    LOG_MSG("SQLBulkOperations called\n");
    return(SQL_SUCCESS);
}