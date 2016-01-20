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

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <algorithm>

#include "ignite/odbc/utility.h"
#include "ignite/odbc/system/odbc_constants.h"

#include "ignite/odbc/config/configuration.h"
#include "ignite/odbc/type_traits.h"
#include "ignite/odbc/environment.h"
#include "ignite/odbc/connection.h"
#include "ignite/odbc/statement.h"

#ifdef ODBC_DEBUG

FILE* log_file = NULL;

void logInit(const char* path)
{
    if (!log_file)
    {
        log_file = fopen(path, "w");
    }
}

#endif //ODBC_DEBUG

BOOL INSTAPI ConfigDSN(HWND     hwndParent,
                       WORD     req,
                       LPCSTR   driver,
                       LPCSTR   attributes)
{
    LOG_MSG("ConfigDSN called\n");

    ignite::odbc::config::Configuration config;

    config.FillFromConfigAttributes(attributes);

    if (!SQLValidDSN(config.GetDsn().c_str()))
        return SQL_FALSE;

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
            return SQL_FALSE;
        }
    }

    return SQL_TRUE;
}

SQLRETURN SQL_API SQLGetInfo(SQLHDBC        conn,
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

    connection->GetInfo(infoType, infoValue, infoValueMax, length);

    return connection->GetDiagnosticRecords().GetReturnCode();
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
        return environment->GetDiagnosticRecords().GetReturnCode();

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

    *stmt = reinterpret_cast<SQLHSTMT>(statement);

    return connection->GetDiagnosticRecords().GetReturnCode();
}

SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT type, SQLHANDLE handle)
{
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
            statement->UnbindAllParameters();

            break;
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

    statement->Close();

    return statement->GetDiagnosticRecords().GetReturnCode();
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
    using ignite::odbc::diagnostic::DiagnosticRecordStorage;
    using ignite::utility::SqlStringToString;
    using ignite::utility::CopyStringToBuffer;

    UNREFERENCED_PARAMETER(windowHandle);

    LOG_MSG("SQLDriverConnect called\n");
    LOG_MSG("Connection String: [%s]\n", inConnectionString);

    Connection *connection = reinterpret_cast<Connection*>(conn);

    if (!connection)
        return SQL_INVALID_HANDLE;

    std::string connectStr = SqlStringToString(inConnectionString, inConnectionStringLen);

    ignite::odbc::config::Configuration config;

    config.FillFromConnectString(connectStr);

    connection->Establish(config.GetHost(), config.GetPort(), config.GetCache());

    const DiagnosticRecordStorage& diag = connection->GetDiagnosticRecords();

    if (!diag.IsSuccessful())
        return diag.GetReturnCode();

    std::string outConnectStr = config.ToConnectString();

    size_t reslen = CopyStringToBuffer(outConnectStr,
        reinterpret_cast<char*>(outConnectionString),
        static_cast<size_t>(outConnectionStringBufferLen));

    if (outConnectionStringLen)
        *outConnectionStringLen = static_cast<SQLSMALLINT>(reslen);

    LOG_MSG("%s\n", outConnectionString);

    return diag.GetReturnCode();
}

SQLRETURN SQL_API SQLConnect(SQLHDBC        conn,
                             SQLCHAR*       serverName,
                             SQLSMALLINT    serverNameLen,
                             SQLCHAR*       userName,
                             SQLSMALLINT    userNameLen,
                             SQLCHAR*       auth,
                             SQLSMALLINT    authLen)
{
    using ignite::odbc::Connection;
    using ignite::odbc::diagnostic::DiagnosticRecordStorage;
    using ignite::utility::SqlStringToString;

    LOG_MSG("SQLConnect called\n");

    Connection *connection = reinterpret_cast<Connection*>(conn);

    if (!connection)
        return SQL_INVALID_HANDLE;

    std::string server = SqlStringToString(serverName, serverNameLen);

    connection->Establish(server);

    return connection->GetDiagnosticRecords().GetReturnCode();
}

SQLRETURN SQL_API SQLDisconnect(SQLHDBC conn)
{
    using ignite::odbc::Connection;

    LOG_MSG("SQLDisconnect called\n");

    Connection *connection = reinterpret_cast<Connection*>(conn);

    if (!connection)
        return SQL_INVALID_HANDLE;

    connection->Release();

    return connection->GetDiagnosticRecords().GetReturnCode();
}

SQLRETURN SQL_API SQLPrepare(SQLHSTMT stmt, SQLCHAR* query, SQLINTEGER queryLen)
{
    using ignite::odbc::Statement;
    using ignite::utility::SqlStringToString;

    LOG_MSG("SQLPrepare called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    std::string sql = SqlStringToString(query, queryLen);

    LOG_MSG("SQL: %s\n", sql.c_str());

    statement->PrepareSqlQuery(sql);

    return statement->GetDiagnosticRecords().GetReturnCode();
}

SQLRETURN SQL_API SQLExecute(SQLHSTMT stmt)
{
    using ignite::odbc::Statement;

    LOG_MSG("SQLExecute called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    statement->ExecuteSqlQuery();

    return statement->GetDiagnosticRecords().GetReturnCode();
}

SQLRETURN SQL_API SQLExecDirect(SQLHSTMT stmt, SQLCHAR* query, SQLINTEGER queryLen)
{
    using ignite::odbc::Statement;
    using ignite::utility::SqlStringToString;

    LOG_MSG("SQLExecDirect called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    std::string sql = SqlStringToString(query, queryLen);

    LOG_MSG("SQL: %s\n", sql.c_str());

    statement->ExecuteSqlQuery(sql);

    return statement->GetDiagnosticRecords().GetReturnCode();
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

    return statement->GetDiagnosticRecords().GetReturnCode();
}

SQLRETURN SQL_API SQLFetch(SQLHSTMT stmt)
{
    using ignite::odbc::Statement;

    LOG_MSG("SQLFetch called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    statement->FetchRow();

    return statement->GetDiagnosticRecords().GetReturnCode();
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
    
    int32_t res = statement->GetColumnNumber();

    *columnNum = static_cast<SQLSMALLINT>(res);

    LOG_MSG("columnNum: %d\n", *columnNum);

    return statement->GetDiagnosticRecords().GetReturnCode();
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

    statement->ExecuteGetTablesMetaQuery(catalog, schema, table, tableTypeStr);

    return statement->GetDiagnosticRecords().GetReturnCode();
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

    statement->ExecuteGetColumnsMetaQuery(schema, table, column);

    return statement->GetDiagnosticRecords().GetReturnCode();
}

SQLRETURN SQL_API SQLMoreResults(SQLHSTMT stmt)
{
    using ignite::odbc::Statement;

    LOG_MSG("SQLMoreResults called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    //TODO: reset diagnostic here.
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
    using namespace ignite::odbc::type_traits;

    using ignite::odbc::Statement;
    using ignite::odbc::app::ApplicationDataBuffer;
    using ignite::odbc::app::Parameter;
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

    IgniteSqlType driverType = ToDriverType(bufferType);

    if (driverType == IGNITE_ODBC_C_TYPE_UNSUPPORTED)
        return SQL_ERROR;

    if (buffer)
    {
        ApplicationDataBuffer dataBuffer(driverType, buffer, bufferLen, resLen);

        Parameter param(dataBuffer, paramSqlType, columnSize, decDigits);

        statement->BindParameter(paramIdx, param);
    }
    else
        statement->UnbindParameter(paramIdx);

    return statement->GetDiagnosticRecords().GetReturnCode();
}

SQLRETURN SQL_API SQLNativeSql(SQLHDBC      conn,
                               SQLCHAR*     inQuery,
                               SQLINTEGER   inQueryLen,
                               SQLCHAR*     outQueryBuffer,
                               SQLINTEGER   outQueryBufferLen,
                               SQLINTEGER*  outQueryLen)
{
    using namespace ignite::utility;

    LOG_MSG("SQLNativeSql called\n");

    std::string in = SqlStringToString(inQuery, inQueryLen);

    CopyStringToBuffer(in, reinterpret_cast<char*>(outQueryBuffer),
        static_cast<size_t>(outQueryBufferLen));

    *outQueryLen = std::min(outQueryBufferLen, static_cast<SQLINTEGER>(in.size()));

    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLColAttribute(SQLHSTMT        stmt,
                                  SQLUSMALLINT    columnNum,
                                  SQLUSMALLINT    fieldId,
                                  SQLPOINTER      strAttr,
                                  SQLSMALLINT     bufferLen,
                                  SQLSMALLINT*    strAttrLen,
                                  SQLLEN*         numericAttr)
{
    using ignite::odbc::Statement;
    using ignite::odbc::meta::ColumnMetaVector;
    using ignite::odbc::meta::ColumnMeta;

    LOG_MSG("SQLColAttribute called: %d (%s)\n", fieldId, ColumnMeta::AttrIdToString(fieldId));

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    // This is a special case
    if (fieldId == SQL_DESC_COUNT)
    {
        SQLSMALLINT val = 0;

        SQLRETURN res = SQLNumResultCols(stmt, &val);

        if (res == SQL_SUCCESS)
            *numericAttr = val;

        return res;
    }

    statement->GetColumnAttribute(columnNum, fieldId, reinterpret_cast<char*>(strAttr),
        bufferLen, strAttrLen, numericAttr);

    return statement->GetDiagnosticRecords().GetReturnCode();
}

SQLRETURN SQL_API SQLDescribeCol(SQLHSTMT       stmt,
                                 SQLUSMALLINT   columnNum, 
                                 SQLCHAR*       columnNameBuf,
                                 SQLSMALLINT    columnNameBufLen,
                                 SQLSMALLINT*   columnNameLen,
                                 SQLSMALLINT*   dataType, 
                                 SQLULEN*       columnSize,
                                 SQLSMALLINT*   decimalDigits, 
                                 SQLSMALLINT*   nullable)
{
    using ignite::odbc::Statement;
    using ignite::odbc::SqlLen;

    LOG_MSG("SQLDescribeCol called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    statement->GetColumnAttribute(columnNum, SQL_DESC_NAME,
        reinterpret_cast<char*>(columnNameBuf), columnNameBufLen, columnNameLen, 0);

    SqlLen dataTypeRes;
    SqlLen columnSizeRes;
    SqlLen decimalDigitsRes;
    SqlLen nullableRes;

    statement->GetColumnAttribute(columnNum, SQL_DESC_TYPE, 0, 0, 0, &dataTypeRes);
    statement->GetColumnAttribute(columnNum, SQL_DESC_PRECISION, 0, 0, 0, &columnSizeRes);
    statement->GetColumnAttribute(columnNum, SQL_DESC_SCALE, 0, 0, 0, &decimalDigitsRes);
    statement->GetColumnAttribute(columnNum, SQL_DESC_NULLABLE, 0, 0, 0, &nullableRes);

    LOG_MSG("columnNum: %lld\n", columnNum);
    LOG_MSG("dataTypeRes: %lld\n", dataTypeRes);
    LOG_MSG("columnSizeRes: %lld\n", columnSizeRes);
    LOG_MSG("decimalDigitsRes: %lld\n", decimalDigitsRes);
    LOG_MSG("nullableRes: %lld\n", nullableRes);
    LOG_MSG("columnNameBuf: %s\n", columnNameBuf);
    LOG_MSG("columnNameLen: %d\n", *columnNameLen);

    *dataType = static_cast<SQLSMALLINT>(dataTypeRes);
    *columnSize = static_cast<SQLULEN>(columnSizeRes);
    *decimalDigits = static_cast<SQLSMALLINT>(decimalDigitsRes);
    *nullable = static_cast<SQLSMALLINT>(nullableRes);

    return statement->GetDiagnosticRecords().GetReturnCode();
}


SQLRETURN SQL_API SQLRowCount(SQLHSTMT stmt, SQLLEN* rowCnt)
{
    using ignite::odbc::Statement;

    LOG_MSG("SQLRowCount called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    int64_t res = statement->AffectedRows();

    *rowCnt = static_cast<SQLLEN>(res);

    return statement->GetDiagnosticRecords().GetReturnCode();
}

SQLRETURN SQL_API SQLForeignKeys(SQLHSTMT       stmt,
                                 SQLCHAR*       primaryCatalogName,
                                 SQLSMALLINT    primaryCatalogNameLen,
                                 SQLCHAR*       primarySchemaName,
                                 SQLSMALLINT    primarySchemaNameLen,
                                 SQLCHAR*       primaryTableName,
                                 SQLSMALLINT    primaryTableNameLen,
                                 SQLCHAR*       foreignCatalogName,
                                 SQLSMALLINT    foreignCatalogNameLen,
                                 SQLCHAR*       foreignSchemaName,
                                 SQLSMALLINT    foreignSchemaNameLen,
                                 SQLCHAR*       foreignTableName,
                                 SQLSMALLINT    foreignTableNameLen)
{
    using ignite::odbc::Statement;
    using ignite::utility::SqlStringToString;

    LOG_MSG("SQLForeignKeys called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    std::string primaryCatalog = SqlStringToString(primaryCatalogName, primaryCatalogNameLen);
    std::string primarySchema = SqlStringToString(primarySchemaName, primarySchemaNameLen);
    std::string primaryTable = SqlStringToString(primaryTableName, primaryTableNameLen);
    std::string foreignCatalog = SqlStringToString(foreignCatalogName, foreignCatalogNameLen);
    std::string foreignSchema = SqlStringToString(foreignSchemaName, foreignSchemaNameLen);
    std::string foreignTable = SqlStringToString(foreignTableName, foreignTableNameLen);

    LOG_MSG("primaryCatalog: %s\n", primaryCatalog.c_str());
    LOG_MSG("primarySchema: %s\n", primarySchema.c_str());
    LOG_MSG("primaryTable: %s\n", primaryTable.c_str());
    LOG_MSG("foreignCatalog: %s\n", foreignCatalog.c_str());
    LOG_MSG("foreignSchema: %s\n", foreignSchema.c_str());
    LOG_MSG("foreignTable: %s\n", foreignTable.c_str());

    statement->ExecuteGetForeignKeysQuery(primaryCatalog, primarySchema,
        primaryTable, foreignCatalog, foreignSchema, foreignTable);

    return statement->GetDiagnosticRecords().GetReturnCode();
}

SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT       stmt,
                                 SQLINTEGER     attr,
                                 SQLPOINTER     valueBuf,
                                 SQLINTEGER     valueBufLen,
                                 SQLINTEGER*    valueResLen)
{
    using ignite::odbc::Statement;

    LOG_MSG("SQLGetStmtAttr called");

#ifdef ODBC_DEBUG
    using ignite::odbc::type_traits::StatementAttrIdToString;

    LOG_MSG("Attr: %s (%d)\n", StatementAttrIdToString(attr), attr);
#endif //ODBC_DEBUG

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    //TODO: move this logic into Statement.
    switch (attr)
    {
        case SQL_ATTR_APP_ROW_DESC:
        case SQL_ATTR_APP_PARAM_DESC:
        case SQL_ATTR_IMP_ROW_DESC:
        case SQL_ATTR_IMP_PARAM_DESC:
        {
            SQLPOINTER *val = reinterpret_cast<SQLPOINTER*>(valueBuf);

            *val = static_cast<SQLPOINTER>(stmt);

            break;
        }

        case SQL_ATTR_ROW_ARRAY_SIZE:
        {
            SQLINTEGER *val = reinterpret_cast<SQLINTEGER*>(valueBuf);

            *val = static_cast<SQLINTEGER>(1);

            break;
        }

        case SQL_ATTR_ROWS_FETCHED_PTR:
        {
            SQLULEN** val = reinterpret_cast<SQLULEN**>(valueBuf);

            *val = reinterpret_cast<SQLULEN*>(statement->GetRowsFetchedPtr());

            break;
        }

        case SQL_ATTR_ROW_STATUS_PTR:
        {
            SQLUSMALLINT** val = reinterpret_cast<SQLUSMALLINT**>(valueBuf);

            *val = reinterpret_cast<SQLUSMALLINT*>(statement->GetRowStatusesPtr());

            break;
        }

        case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
        {
            SQLULEN** val = reinterpret_cast<SQLULEN**>(valueBuf);

            *val = reinterpret_cast<SQLULEN*>(statement->GetParamBindOffsetPtr());

            break;
        }

        case SQL_ATTR_ROW_BIND_OFFSET_PTR:
        {
            SQLULEN** val = reinterpret_cast<SQLULEN**>(valueBuf);

            *val = reinterpret_cast<SQLULEN*>(statement->GetColumnBindOffsetPtr());

            break;
        }

        default:
            return SQL_ERROR;
    }

    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT    stmt,
                                 SQLINTEGER  attr,
                                 SQLPOINTER  value,
                                 SQLINTEGER  valueLen)
{
    using ignite::odbc::Statement;

    LOG_MSG("SQLSetStmtAttr called");

#ifdef ODBC_DEBUG
    using ignite::odbc::type_traits::StatementAttrIdToString;

    LOG_MSG("Attr: %s (%d)\n", StatementAttrIdToString(attr), attr);
#endif //ODBC_DEBUG

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    //TODO: move this logic into Statement.
    switch (attr)
    {
        case SQL_ATTR_ROW_ARRAY_SIZE:
        {
            SQLULEN val = reinterpret_cast<SQLULEN>(value);

            LOG_MSG("Value: %d\n", val);

            if (val != 1)
                return SQL_ERROR;

            break;
        }

        case SQL_ATTR_ROWS_FETCHED_PTR:
        {
            statement->SetRowsFetchedPtr(reinterpret_cast<size_t*>(value));

            break;
        }

        case SQL_ATTR_ROW_STATUS_PTR:
        {
            statement->SetRowStatusesPtr(reinterpret_cast<uint16_t*>(value));

            break;
        }

        case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
        {
            statement->SetParamBindOffsetPtr(reinterpret_cast<size_t*>(value));

            break;
        }

        case SQL_ATTR_ROW_BIND_OFFSET_PTR:
        {
            statement->SetColumnBindOffsetPtr(reinterpret_cast<size_t*>(value));

            break;
        }

        default:
            return SQL_ERROR;
    }

    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLPrimaryKeys(SQLHSTMT       stmt,
                                 SQLCHAR*       catalogName,
                                 SQLSMALLINT    catalogNameLen,
                                 SQLCHAR*       schemaName,
                                 SQLSMALLINT    schemaNameLen,
                                 SQLCHAR*       tableName,
                                 SQLSMALLINT    tableNameLen)
{
    using ignite::odbc::Statement;
    using ignite::utility::SqlStringToString;

    LOG_MSG("SQLPrimaryKeys called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    std::string catalog = SqlStringToString(catalogName, catalogNameLen);
    std::string schema = SqlStringToString(schemaName, schemaNameLen);
    std::string table = SqlStringToString(tableName, tableNameLen);

    LOG_MSG("catalog: %s\n", catalog.c_str());
    LOG_MSG("schema: %s\n", schema.c_str());
    LOG_MSG("table: %s\n", table.c_str());

    statement->ExecuteGetPrimaryKeysQuery(catalog, schema, table);

    return statement->GetDiagnosticRecords().GetReturnCode();
}

SQLRETURN SQL_API SQLNumParams(SQLHSTMT stmt, SQLSMALLINT* paramCnt)
{
    using ignite::odbc::Statement;

    LOG_MSG("SQLNumParams called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    *paramCnt = static_cast<SQLSMALLINT>(statement->GetParametersNumber());

    return statement->GetDiagnosticRecords().GetReturnCode();
}

SQLRETURN SQL_API SQLGetDiagField(SQLSMALLINT   handleType,
                                  SQLHANDLE     handle,
                                  SQLSMALLINT   recNum,
                                  SQLSMALLINT   diagId,
                                  SQLPOINTER    buffer,
                                  SQLSMALLINT   bufferLen,
                                  SQLSMALLINT*  resLen)
{
    using namespace ignite::odbc;
    using namespace ignite::odbc::diagnostic;
    using namespace ignite::odbc::type_traits;

    using ignite::odbc::app::ApplicationDataBuffer;

    LOG_MSG("SQLGetDiagField called: %d\n", recNum);

    SqlLen outResLen;
    ApplicationDataBuffer outBuffer(IGNITE_ODBC_C_TYPE_DEFAULT, buffer, bufferLen, &outResLen);

    SqlResult result;

    DiagnosticField field = DiagnosticFieldToInternal(diagId);

    switch (handleType)
    {
        case SQL_HANDLE_ENV:
        case SQL_HANDLE_DBC:
        case SQL_HANDLE_STMT:
        {
            Diagnosable *diag = reinterpret_cast<Diagnosable*>(handle);

            result = diag->GetDiagnosticRecords().GetField(recNum, field, outBuffer);

            break;
        }

        default:
        {
            result = SQL_RESULT_NO_DATA;
            break;
        }
    }

    if (result == SQL_RESULT_SUCCESS)
        *resLen = static_cast<SQLSMALLINT>(outResLen);

    return SqlResultToReturnCode(result);
}

SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT     handleType,
                                SQLHANDLE       handle,
                                SQLSMALLINT     recNum,
                                SQLCHAR*        sqlState,
                                SQLINTEGER*     nativeError,
                                SQLCHAR*        msgBuffer,
                                SQLSMALLINT     msgBufferLen,
                                SQLSMALLINT*    msgLen)
{
    using namespace ignite::utility;
    using namespace ignite::odbc;
    using namespace ignite::odbc::diagnostic;
    using namespace ignite::odbc::type_traits;

    using ignite::odbc::app::ApplicationDataBuffer;

    LOG_MSG("SQLGetDiagRec called\n");

    const DiagnosticRecordStorage* records = 0;
    
    switch (handleType)
    {
        case SQL_HANDLE_ENV:
        case SQL_HANDLE_DBC:
        case SQL_HANDLE_STMT:
        {
            Diagnosable *diag = reinterpret_cast<Diagnosable*>(handle);

            records = &diag->GetDiagnosticRecords();

            break;
        }

        default:
            break;
    }

    if (!records || recNum < 1 || recNum > records->GetStatusRecordsNumber())
        return SQL_NO_DATA;

    const DiagnosticRecord& record = records->GetStatusRecord(recNum);

    if (sqlState)
        CopyStringToBuffer(record.GetSqlState(), reinterpret_cast<char*>(sqlState), 6);

    if (nativeError)
        *nativeError = 0;

    SqlLen outResLen;
    ApplicationDataBuffer outBuffer(IGNITE_ODBC_C_TYPE_CHAR, msgBuffer, msgBufferLen, &outResLen);

    outBuffer.PutString(record.GetMessage());

    *msgLen = static_cast<SQLSMALLINT>(outResLen);

    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetTypeInfo(SQLHSTMT       stmt,
                                 SQLSMALLINT    type)
{
    using ignite::odbc::Statement;

    LOG_MSG("SQLGetTypeInfo called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    statement->ExecuteGetTypeInfoQuery(static_cast<int16_t>(type));

    return statement->GetDiagnosticRecords().GetReturnCode();
}

SQLRETURN SQL_API SQLEndTran(SQLSMALLINT    handleType,
                             SQLHANDLE      handle,
                             SQLSMALLINT    completionType)
{
    using namespace ignite::odbc;

    LOG_MSG("SQLEndTran called\n");

    SQLRETURN result;

    switch (handleType)
    {
        case SQL_HANDLE_ENV:
        {
            Environment *env = reinterpret_cast<Environment*>(handle);

            if (!env)
                return SQL_INVALID_HANDLE;

            if (completionType == SQL_COMMIT)
                env->TransactionCommit();
            else
                env->TransactionRollback();

            result = env->GetDiagnosticRecords().GetReturnCode();

            break;
        }

        case SQL_HANDLE_DBC:
        {
            Connection *conn = reinterpret_cast<Connection*>(handle);

            if (!conn)
                return SQL_INVALID_HANDLE;

            if (completionType == SQL_COMMIT)
                conn->TransactionCommit();
            else
                conn->TransactionRollback();

            result = conn->GetDiagnosticRecords().GetReturnCode();

            break;
        }

        default:
        {
            result = SQL_INVALID_HANDLE;

            break;
        }
    }

    return result;
}

SQLRETURN SQL_API SQLGetData(SQLHSTMT       stmt,
                             SQLUSMALLINT   colNum,
                             SQLSMALLINT    targetType,
                             SQLPOINTER     targetValue,
                             SQLLEN         bufferLength,
                             SQLLEN*        strLengthOrIndicator)
{
    using namespace ignite::odbc::type_traits;

    using ignite::odbc::Statement;
    using ignite::odbc::app::ApplicationDataBuffer;

    LOG_MSG("SQLGetData called\n");

    Statement *statement = reinterpret_cast<Statement*>(stmt);

    if (!statement)
        return SQL_INVALID_HANDLE;

    IgniteSqlType driverType = ToDriverType(targetType);

    ApplicationDataBuffer dataBuffer(driverType, targetValue, bufferLength, strLengthOrIndicator);

    //statement->GetColumnData(colNum, dataBuffer);

    return statement->GetDiagnosticRecords().GetReturnCode();
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

    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV     env,
                                SQLINTEGER  attr,
                                SQLPOINTER  value,
                                SQLINTEGER  valueLen)
{
    LOG_MSG("SQLSetEnvAttr called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLBulkOperations(SQLHSTMT       stmt,
                                    SQLUSMALLINT   operation)
{
    LOG_MSG("SQLBulkOperations called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLTablePrivileges(SQLHSTMT      stmt,
                                     SQLCHAR*      catalogName,
                                     SQLSMALLINT   catalogNameLen,
                                     SQLCHAR*      schemaName,
                                     SQLSMALLINT   schemaNameLen,
                                     SQLCHAR*      tableName,
                                     SQLSMALLINT   tableNameLen)
{
    LOG_MSG("SQLTablePrivileges called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLCopyDesc(SQLHDESC src, SQLHDESC dst)
{
    LOG_MSG("SQLCopyDesc called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetDescField(SQLHDESC      descr,
                                  SQLSMALLINT   recNum,
                                  SQLSMALLINT   fieldId,
                                  SQLPOINTER    buffer,
                                  SQLINTEGER    bufferLen,
                                  SQLINTEGER*   resLen)
{
    LOG_MSG("SQLGetDescField called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetDescRec(SQLHDESC        DescriptorHandle,
                                SQLSMALLINT     RecNumber,
                                SQLCHAR*        nameBuffer,
                                SQLSMALLINT     nameBufferLen,
                                SQLSMALLINT*    strLen,
                                SQLSMALLINT*    type,
                                SQLSMALLINT*    subType,
                                SQLLEN*         len,
                                SQLSMALLINT*    precision,
                                SQLSMALLINT*    scale,
                                SQLSMALLINT*    nullable)
{
    LOG_MSG("SQLGetDescRec called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetDescField(SQLHDESC      descr,
                                  SQLSMALLINT   recNum,
                                  SQLSMALLINT   fieldId,
                                  SQLPOINTER    buffer,
                                  SQLINTEGER    bufferLen)
{
    LOG_MSG("SQLSetDescField called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetDescRec(SQLHDESC      descr,
                                SQLSMALLINT   recNum,
                                SQLSMALLINT   type,
                                SQLSMALLINT   subType,
                                SQLLEN        len,
                                SQLSMALLINT   precision,
                                SQLSMALLINT   scale,
                                SQLPOINTER    buffer,
                                SQLLEN*       resLen,
                                SQLLEN*       id)
{
    LOG_MSG("SQLSetDescRec called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLColumnPrivileges(SQLHSTMT      stmt,
                                      SQLCHAR*      catalogName,
                                      SQLSMALLINT   catalogNameLen,
                                      SQLCHAR*      schemaName,
                                      SQLSMALLINT   schemaNameLen,
                                      SQLCHAR*      tableName,
                                      SQLSMALLINT   tableNameLen,
                                      SQLCHAR*      columnName,
                                      SQLSMALLINT   columnNameLen)
{
    LOG_MSG("SQLColumnPrivileges called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLDescribeParam(SQLHSTMT     stmt,
                                   SQLUSMALLINT paramNum,
                                   SQLSMALLINT* dataType,
                                   SQLULEN*     paramSize,
                                   SQLSMALLINT* decimalDigits,
                                   SQLSMALLINT* nullable)
{
    LOG_MSG("SQLDescribeParam called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLParamOptions(SQLHSTMT  stmt,
                                  SQLULEN   paramSetSize,
                                  SQLULEN*  paramsProcessed)
{
    LOG_MSG("SQLParamOptions called\n");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLProcedures(SQLHSTMT        stmt,
                                SQLCHAR*        catalogName,
                                SQLSMALLINT     catalogNameLen,
                                SQLCHAR*        schemaName,
                                SQLSMALLINT     schemaNameLen,
                                SQLCHAR*        tableName,
                                SQLSMALLINT     tableNameLen)
{
    LOG_MSG("SQLProcedures called\n");
    return SQL_SUCCESS;
}
