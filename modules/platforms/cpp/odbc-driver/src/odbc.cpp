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
#   undef min
#endif //_WIN32

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <algorithm>

#include <sqlext.h>
#include <odbcinst.h>

#include "ignite/odbc/config/configuration.h"
#include "ignite/odbc/utility.h"
#include "ignite/odbc/type_traits.h"
#include "ignite/odbc/environment.h"
#include "ignite/odbc/connection.h"


FILE* log_file = NULL;

void logInit(const char* path)
{
    if (!log_file)
    {
        log_file = fopen(path, "w");
    }
}


BOOL INSTAPI ConfigDSN(HWND     hwndParent,
                       WORD     req,
                       LPCSTR   driver,
                       LPCSTR   attributes)
{
    LOG_MSG("ConfigDSN called\n");

    ignite::odbc::config::Configuration config;

    config.FillFromConfigAttributes(attributes);

    if (!SQLValidDSN(config.GetDsn().c_str()))
        return FALSE;

    LOG_MSG("Driver: %s\n", driver);
    LOG_MSG("Attributes: %s\n", attributes);

    LOG_MSG("DSN: %s\n", config.GetDsn().c_str());

    switch (req)
    {
        case ODBC_ADD_DSN:
        {
            LOG_MSG("ODBC_ADD_DSN\n");

            return SQLWriteDSNToIni(config.GetDsn().c_str(), driver);
        }

        case ODBC_CONFIG_DSN:
        {
            LOG_MSG("ODBC_CONFIG_DSN\n");
            break;
        }

        case ODBC_REMOVE_DSN:
        {
            LOG_MSG("ODBC_REMOVE_DSN\n");

            return SQLRemoveDSNFromIni(config.GetDsn().c_str());
        }

        default:
        {
            return FALSE;
        }
    }

    return TRUE;
}

SQLRETURN SQLGetInfo(SQLHDBC        conn,
                     SQLUSMALLINT   infoType,
                     SQLPOINTER     infoValue,
                     SQLSMALLINT    infoValueMax,
                     SQLSMALLINT*   length)
{
    using ignite::odbc::Connection;
    using ignite::odbc::config::ConnectionInfo;

    LOG_MSG("SQLGetInfo called: %d (%s)\n", infoType, ConnectionInfo::InfoTypeToString(infoType));

    Connection *connection = reinterpret_cast<Connection*>(conn);

    if (!connection)
        return SQL_INVALID_HANDLE;

    const ConnectionInfo& info = connection->GetInfo();

    bool success = info.GetInfo(infoType, infoValue, infoValueMax, length);

    return success ? SQL_SUCCESS : SQL_ERROR;
}

SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE* result)
{
    //LOG_MSG("SQLAllocHandle called\n");
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

SQLRETURN SQL_API SQLAllocEnv(SQLHENV* env)
{
    using ignite::odbc::Environment;

    LOG_MSG("SQLAllocEnv called\n");

    *env = reinterpret_cast<SQLHENV>(new Environment());

    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLAllocConnect(SQLHENV env, SQLHDBC* conn)
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

SQLRETURN SQL_API SQLAllocStmt(SQLHDBC conn, SQLHSTMT* stmt)
{
    using ignite::odbc::Connection;
    using ignite::odbc::Statement;

    LOG_MSG("SQLAllocStmt called\n");

    *stmt = SQL_NULL_HDBC;

    Connection *connection = reinterpret_cast<Connection*>(conn);

    if (!connection)
        return SQL_INVALID_HANDLE;

    Statement *statement = connection->CreateStatement();

    if (!statement)
        return SQL_ERROR;

    *stmt = reinterpret_cast<SQLHSTMT>(statement);

    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT type, SQLHANDLE handle)
{
    //LOG_MSG("SQLFreeHandle called\n");

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

    LOG_MSG("SQLFreeConnect called\n");

    Connection *connection = reinterpret_cast<Connection*>(conn);

    if (!connection)
        return SQL_INVALID_HANDLE;

    delete connection;

    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT stmt, SQLUSMALLINT option)
{
    using ignite::odbc::Statement;

    LOG_MSG("SQLFreeStmt called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    switch (option)
    {
        case SQL_DROP:
        {
            delete statement;

            break;
        }

        case SQL_CLOSE:
        {
            return SQLCloseCursor(stmt);
        }

        case SQL_UNBIND:
        {
            statement->UnbindAllColumns();

            break;
        }

        case SQL_RESET_PARAMS:
        {
            // Not supported yet.
            // Falling through.
        }

        default:
            return SQL_ERROR;
    }

    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT stmt)
{
    using ignite::odbc::Statement;

    LOG_MSG("SQLCloseCursor called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    bool success = statement->Close();

    return success ? SQL_SUCCESS : SQL_ERROR;
}

SQLRETURN SQL_API SQLDriverConnect(SQLHDBC      conn,
                                   SQLHWND      windowHandle,
                                   SQLCHAR*     inConnectionString,
                                   SQLSMALLINT  inConnectionStringLen,
                                   SQLCHAR*     outConnectionString,
                                   SQLSMALLINT  outConnectionStringBufferLen,
                                   SQLSMALLINT* outConnectionStringLen,
                                   SQLUSMALLINT driverCompletion)
{
    using ignite::odbc::Connection;

    UNREFERENCED_PARAMETER(windowHandle);

    LOG_MSG("SQLDriverConnect called\n");
    LOG_MSG("Connection String: [%s]\n", inConnectionString);

    Connection *connection = reinterpret_cast<Connection*>(conn);

    if (!connection)
        return SQL_INVALID_HANDLE;

    if (inConnectionStringLen == SQL_NTS && inConnectionString)
        inConnectionStringLen = static_cast<SQLSMALLINT>(strlen((char*)inConnectionString));

    ignite::odbc::config::Configuration config;

    config.FillFromConnectString(reinterpret_cast<const char*>(inConnectionString), inConnectionStringLen);

    bool connected = connection->Establish(config.GetHost(), config.GetPort(), config.GetCache());

    if (!connected)
        return SQL_ERROR;

    if (outConnectionString && outConnectionStringBufferLen > 0)
    {
        std::string out_connection_str = config.ToConnectString();

        LOG_MSG("%s\n", out_connection_str.c_str());

        strncpy((char*)outConnectionString, out_connection_str.c_str(), outConnectionStringBufferLen - 1);

        outConnectionString[outConnectionStringBufferLen - 1] = '\0';

        if (outConnectionStringLen)
            *outConnectionStringLen = static_cast<SQLSMALLINT>(strlen(reinterpret_cast<char*>(outConnectionString)));
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

SQLRETURN SQL_API SQLPrepare(SQLHSTMT stmt, SQLCHAR* query, SQLINTEGER queryLen)
{
    using ignite::odbc::Statement;

    LOG_MSG("SQLPrepare called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    const char* sql = reinterpret_cast<char*>(query);
    size_t len = queryLen == SQL_NTS ? strlen(sql) : static_cast<size_t>(queryLen);

    LOG_MSG("SQL: %s\n", sql);
    LOG_MSG("Length: %u\n", len);

    statement->PrepareSqlQuery(sql, len);

    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLExecute(SQLHSTMT stmt)
{
    using ignite::odbc::Statement;

    LOG_MSG("SQLExecute called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    bool success = statement->ExecuteSqlQuery();

    return success ? SQL_SUCCESS : SQL_ERROR;
}

SQLRETURN SQL_API SQLExecDirect(SQLHSTMT stmt, SQLCHAR* query, SQLINTEGER queryLen)
{
    using ignite::odbc::Statement;

    LOG_MSG("SQLExecDirect called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    const char* sql = reinterpret_cast<char*>(query);
    size_t len = queryLen == SQL_NTS ? strlen(sql) : static_cast<size_t>(queryLen);

    LOG_MSG("SQL: %s\n", sql);
    LOG_MSG("Length: %u\n", len);

    bool success = statement->ExecuteSqlQuery(sql, len);

    return success ? SQL_SUCCESS : SQL_ERROR;
}

SQLRETURN SQL_API SQLBindCol(SQLHSTMT       stmt,
                             SQLUSMALLINT   colNum,
                             SQLSMALLINT    targetType,
                             SQLPOINTER     targetValue,
                             SQLLEN         bufferLength,
                             SQLLEN*        strLengthOrIndicator)
{
    using namespace ignite::odbc::type_traits;

    using ignite::odbc::Statement;
    using ignite::odbc::app::ApplicationDataBuffer;

    LOG_MSG("SQLBindCol called: index=%d, type=%d\n", colNum, targetType);

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    IgniteSqlType driverType = ToDriverType(targetType);

    if (driverType == IGNITE_ODBC_C_TYPE_UNSUPPORTED)
        return SQL_ERROR;

    if (bufferLength < 0)
        return SQL_ERROR;

    if (targetValue || strLengthOrIndicator)
    {
        ApplicationDataBuffer dataBuffer(driverType, targetValue, bufferLength, strLengthOrIndicator);

        statement->BindColumn(colNum, dataBuffer);
    }
    else
        statement->UnbindColumn(colNum);

    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLFetch(SQLHSTMT stmt)
{
    using ignite::odbc::Statement;

    LOG_MSG("SQLFetch called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    return SqlResultToReturnCode(statement->FetchRow());
}

SQLRETURN SQL_API SQLFetchScroll(SQLHSTMT       stmt,
                                 SQLSMALLINT    orientation,
                                 SQLLEN         offset)
{
    LOG_MSG("SQLFetchScroll called\n");
    LOG_MSG("Orientation: %d, Offset: %d\n", orientation, offset);

    if (orientation != SQL_FETCH_NEXT)
        return SQL_ERROR;

    return SQLFetch(stmt);
}

SQLRETURN SQL_API SQLExtendedFetch(SQLHSTMT         stmt,
                                   SQLUSMALLINT     orientation,
                                   SQLLEN           offset,
                                   SQLULEN*         rowCount,
                                   SQLUSMALLINT*    rowStatusArray)
{
    LOG_MSG("SQLExtendedFetch called\n");

    SQLRETURN res = SQLFetchScroll(stmt, orientation, offset);

    if (res == SQL_SUCCESS || res == SQL_NO_DATA)
    {
        if (rowCount)
            *rowCount = 1;

        if (rowStatusArray)
            rowStatusArray[0] = SQL_ROW_SUCCESS;
    }

    return res;
}

SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT stmt, SQLSMALLINT *columnNum)
{
    using ignite::odbc::Statement;
    using ignite::odbc::meta::ColumnMetaVector;

    LOG_MSG("SQLNumResultCols called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    const ColumnMetaVector* meta = statement->GetMeta();

    if (!meta)
        return SQL_ERROR;

    *columnNum = static_cast<SQLSMALLINT>(meta->size());

    LOG_MSG("columnNum: %d\n", *columnNum);

    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLTables(SQLHSTMT    stmt,
                            SQLCHAR*    catalogName,
                            SQLSMALLINT catalogNameLen,
                            SQLCHAR*    schemaName,
                            SQLSMALLINT schemaNameLen,
                            SQLCHAR*    tableName,
                            SQLSMALLINT tableNameLen,
                            SQLCHAR*    tableType,
                            SQLSMALLINT tableTypeLen)
{
    using ignite::odbc::Statement;
    using ignite::utility::SqlStringToString;

    LOG_MSG("SQLTables called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    std::string catalog = SqlStringToString(catalogName, catalogNameLen);
    std::string schema = SqlStringToString(schemaName, schemaNameLen);
    std::string table = SqlStringToString(tableName, tableNameLen);
    std::string tableTypeStr = SqlStringToString(tableType, tableTypeLen);

    LOG_MSG("catalog: %s\n", catalog.c_str());
    LOG_MSG("schema: %s\n", schema.c_str());
    LOG_MSG("table: %s\n", table.c_str());
    LOG_MSG("tableType: %s\n", tableTypeStr.c_str());

    bool success = statement->ExecuteGetTablesMetaQuery(catalog, schema, table, tableTypeStr);

    return success ? SQL_SUCCESS : SQL_ERROR;
}

SQLRETURN SQL_API SQLColumns(SQLHSTMT       stmt,
                             SQLCHAR*       catalogName,
                             SQLSMALLINT    catalogNameLen,
                             SQLCHAR*       schemaName,
                             SQLSMALLINT    schemaNameLen,
                             SQLCHAR*       tableName,
                             SQLSMALLINT    tableNameLen,
                             SQLCHAR*       columnName,
                             SQLSMALLINT    columnNameLen)
{
    using ignite::odbc::Statement;
    using ignite::utility::SqlStringToString;

    LOG_MSG("SQLColumns called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    std::string catalog = SqlStringToString(catalogName, catalogNameLen);
    std::string schema = SqlStringToString(schemaName, schemaNameLen);
    std::string table = SqlStringToString(tableName, tableNameLen);
    std::string column = SqlStringToString(columnName, columnNameLen);

    LOG_MSG("catalog: %s\n", catalog.c_str());
    LOG_MSG("schema: %s\n", schema.c_str());
    LOG_MSG("table: %s\n", table.c_str());
    LOG_MSG("column: %s\n", column.c_str());

    bool success = statement->ExecuteGetColumnsMetaQuery(schema, table, column);

    return success ? SQL_SUCCESS : SQL_ERROR;
}

SQLRETURN SQL_API SQLMoreResults(SQLHSTMT stmt)
{
    using ignite::odbc::Statement;

    LOG_MSG("SQLMoreResults called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    return statement->DataAvailable() ? SQL_SUCCESS : SQL_NO_DATA;
}

SQLRETURN SQL_API SQLBindParameter(SQLHSTMT     stmt,
                                   SQLUSMALLINT paramIdx,
                                   SQLSMALLINT  ioType,
                                   SQLSMALLINT  bufferType,
                                   SQLSMALLINT  paramSqlType,
                                   SQLULEN      columnSize,
                                   SQLSMALLINT  decDigits,
                                   SQLPOINTER   buffer,
                                   SQLLEN       bufferLen,
                                   SQLLEN*      resLen)
{
    using ignite::odbc::Statement;
    using ignite::odbc::type_traits::IsSqlTypeSupported;

    LOG_MSG("SQLBindParameter called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    if (ioType != SQL_PARAM_INPUT)
        return SQL_ERROR;

    if (*resLen == SQL_DATA_AT_EXEC || *resLen <= SQL_LEN_DATA_AT_EXEC_OFFSET)
        return SQL_ERROR;

    if (!IsSqlTypeSupported(paramSqlType))
        return SQL_ERROR;

    return SQL_SUCCESS;
}

//
// ==== Not implemented ====
//

SQLRETURN SQL_API SQLCancel(SQLHSTMT stmt)
{
    LOG_MSG("SQLCancel called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLColAttributes(SQLHSTMT     stmt,
                                   SQLUSMALLINT colNum,
                                   SQLUSMALLINT fieldId,
                                   SQLPOINTER   strAttrBuf,
                                   SQLSMALLINT  strAttrBufLen,
                                   SQLSMALLINT* strAttrResLen,
                                   SQLLEN*      numAttrBuf)
{
    LOG_MSG("SQLColAttributes called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLConnect(SQLHDBC        conn,
                             SQLCHAR*       serverName,
                             SQLSMALLINT    serverNameLen,
                             SQLCHAR*       userName,
                             SQLSMALLINT    userNameLen,
                             SQLCHAR*       auth,
                             SQLSMALLINT    authLen)
{
    LOG_MSG("SQLConnect called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLDescribeCol(SQLHSTMT       stmt,
                                 SQLUSMALLINT   columnNumber, 
                                 SQLCHAR*       columnName,
                                 SQLSMALLINT    bufferLength, 
                                 SQLSMALLINT*   nameLength,
                                 SQLSMALLINT*   dataType, 
                                 SQLULEN*       columnSize,
                                 SQLSMALLINT*   decimalDigits, 
                                 SQLSMALLINT*   nullable)
{
    LOG_MSG("SQLDescribeCol called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLError(SQLHENV      env,
                           SQLHDBC      conn,
                           SQLHSTMT     stmt,
                           SQLCHAR*     state,
                           SQLINTEGER*  error,
                           SQLCHAR*     msgBuf,
                           SQLSMALLINT  msgBufLen,
                           SQLSMALLINT* msgResLen)
{
    LOG_MSG("SQLError called\n");
    return(SQL_NO_DATA_FOUND);
}

SQLRETURN SQL_API SQLGetCursorName(SQLHSTMT     stmt,
                                   SQLCHAR*     nameBuf,
                                   SQLSMALLINT  nameBufLen,
                                   SQLSMALLINT* nameResLen)
{
    LOG_MSG("SQLGetCursorName called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLRowCount(SQLHSTMT  stmt,
                              SQLLEN*   rowCnt)
{
    LOG_MSG("SQLRowCount called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetCursorName(SQLHSTMT     stmt,
                                   SQLCHAR*     name,
                                   SQLSMALLINT  nameLen)
{
    LOG_MSG("SQLSetCursorName called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetConnectOption(SQLHDBC       conn,
                                      SQLUSMALLINT  option,
                                      SQLPOINTER    value)
{
    LOG_MSG("SQLGetConnectOption called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetData(SQLHSTMT       stmt,
                             SQLUSMALLINT   colNum,
                             SQLSMALLINT    targetType,
                             SQLPOINTER     targetValue,
                             SQLLEN         bufferLength,
                             SQLLEN*        strLengthOrIndicator)
{
    LOG_MSG("SQLGetData called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetFunctions(SQLHDBC       conn,
                                  SQLUSMALLINT  funcId,
                                  SQLUSMALLINT* supported)
{
    LOG_MSG("SQLGetFunctions called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetStmtOption(SQLHSTMT     stmt,
                                   SQLUSMALLINT option,
                                   SQLPOINTER   value)
{
    LOG_MSG("SQLGetStmtOption called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetTypeInfo(SQLHSTMT       stmt,
                                 SQLSMALLINT    type)
{
    LOG_MSG("SQLGetTypeInfo called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLParamData(SQLHSTMT    stmt,
                               SQLPOINTER* value)
{
    LOG_MSG("SQLParamData called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLPutData(SQLHSTMT     stmt,
                             SQLPOINTER   data,
                             SQLLEN       strLengthOrIndicator)
{
    LOG_MSG("SQLPutData called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetConnectOption(SQLHDBC       conn,
                                      SQLUSMALLINT  option,
                                      SQLULEN       value)
{
    LOG_MSG("SQLSetConnectOption called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetStmtOption(SQLHSTMT     stmt,
                                   SQLUSMALLINT option,
                                   SQLULEN      value)
{
    LOG_MSG("SQLSetStmtOption called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSpecialColumns(SQLHSTMT    stmt,
                                    SQLSMALLINT idType,
                                    SQLCHAR*    catalogName,
                                    SQLSMALLINT catalogNameLen,
                                    SQLCHAR*    schemaName,
                                    SQLSMALLINT schemaNameLen,
                                    SQLCHAR*    tableName,
                                    SQLSMALLINT tableNameLen,
                                    SQLSMALLINT scope,
                                    SQLSMALLINT nullable)
{
    LOG_MSG("SQLSpecialColumns called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLStatistics(SQLHSTMT        stmt,
                                SQLCHAR*        catalogName,
                                SQLSMALLINT     catalogNameLen,
                                SQLCHAR*        schemaName,
                                SQLSMALLINT     schemaNameLen,
                                SQLCHAR*        tableName,
                                SQLSMALLINT     tableNameLen,
                                SQLUSMALLINT    unique,
                                SQLUSMALLINT    reserved)
{
    LOG_MSG("SQLStatistics called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLBrowseConnect(SQLHDBC      conn,
                                   SQLCHAR*     inConnectionStr,
                                   SQLSMALLINT  inConnectionStrLen,
                                   SQLCHAR*     outConnectionStrBuf,
                                   SQLSMALLINT  outConnectionStrBufLen,
                                   SQLSMALLINT* outConnectionStrResLen)
{
    LOG_MSG("SQLBrowseConnect called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLProcedureColumns(SQLHSTMT      stmt,
                                      SQLCHAR *     catalogName,
                                      SQLSMALLINT   catalogNameLen,
                                      SQLCHAR *     schemaName,
                                      SQLSMALLINT   schemaNameLen,
                                      SQLCHAR *     procName,
                                      SQLSMALLINT   procNameLen,
                                      SQLCHAR *     columnName,
                                      SQLSMALLINT   columnNameLen)
{
    LOG_MSG("SQLProcedureColumns called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetPos(SQLHSTMT        stmt,
                            SQLSETPOSIROW   rowNum,
                            SQLUSMALLINT    operation,
                            SQLUSMALLINT    lockType)
{
    LOG_MSG("SQLSetPos called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetScrollOptions(SQLHSTMT      stmt,
                                      SQLUSMALLINT  concurrency,
                                      SQLLEN        crowKeyset,
                                      SQLUSMALLINT  crowRowset)
{
    LOG_MSG("SQLSetScrollOptions called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC     conn,
                                    SQLINTEGER  attr,
                                    SQLPOINTER  valueBuf,
                                    SQLINTEGER  valueBufLen,
                                    SQLINTEGER* valueResLen)
{
    LOG_MSG("SQLGetConnectAttr called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetEnvAttr(SQLHENV     env,
                                SQLINTEGER  attr,
                                SQLPOINTER  valueBuf,
                                SQLINTEGER  valueBufLen,
                                SQLINTEGER* valueResLen)
{
    LOG_MSG("SQLGetEnvAttr called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT       stmt,
                                 SQLINTEGER     attr,
                                 SQLPOINTER     valueBuf,
                                 SQLINTEGER     valueBufLen,
                                 SQLINTEGER*    valueResLen)
{
    LOG_MSG("SQLGetStmtAttr called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC     conn,
                                    SQLINTEGER  attr,
                                    SQLPOINTER  value,
                                    SQLINTEGER  valueLen)
{
    using ignite::odbc::Connection;

    LOG_MSG("SQLSetConnectAttr called\n");

    Connection *connection = reinterpret_cast<Connection*>(conn);

    if (!connection)
        return SQL_INVALID_HANDLE;

    return SQL_ERROR;
}

SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV     env,
                                SQLINTEGER  attr,
                                SQLPOINTER  value,
                                SQLINTEGER  valueLen)
{
    LOG_MSG("SQLSetEnvAttr called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT    stmt,
                                 SQLINTEGER  attr,
                                 SQLPOINTER  value,
                                 SQLINTEGER  valueLen)
{
    LOG_MSG("SQLSetStmtAttr called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLBulkOperations(SQLHSTMT       stmt,
                                    SQLUSMALLINT   operation)
{
    LOG_MSG("SQLBulkOperations called\n");
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
