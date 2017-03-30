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

#include "ignite/odbc.h"

#include "ignite/odbc/log.h"

#include "ignite/odbc/utility.h"

SQLRETURN SQL_API SQLGetInfo(SQLHDBC        conn,
                             SQLUSMALLINT   infoType,
                             SQLPOINTER     infoValue,
                             SQLSMALLINT    infoValueMax,
                             SQLSMALLINT*   length)
{
    return ignite::SQLGetInfo(conn, infoType, infoValue, infoValueMax, length);
}

SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE* result)
{
    return ignite::SQLAllocHandle(type, parent, result);
}

SQLRETURN SQL_API SQLAllocEnv(SQLHENV* env)
{
    return ignite::SQLAllocEnv(env);
}

SQLRETURN SQL_API SQLAllocConnect(SQLHENV env, SQLHDBC* conn)
{
    return ignite::SQLAllocConnect(env, conn);
}

SQLRETURN SQL_API SQLAllocStmt(SQLHDBC conn, SQLHSTMT* stmt)
{
    return ignite::SQLAllocStmt(conn, stmt);
}

SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT type, SQLHANDLE handle)
{
    return ignite::SQLFreeHandle(type, handle);
}

SQLRETURN SQL_API SQLFreeEnv(SQLHENV env)
{
    return ignite::SQLFreeEnv(env);
}

SQLRETURN SQL_API SQLFreeConnect(SQLHDBC conn)
{
    return ignite::SQLFreeConnect(conn);
}

SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT stmt, SQLUSMALLINT option)
{
    return ignite::SQLFreeStmt(stmt, option);
}

SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT stmt)
{
    return ignite::SQLCloseCursor(stmt);
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
    return ignite::SQLDriverConnect(conn, windowHandle, inConnectionString,
        inConnectionStringLen, outConnectionString, outConnectionStringBufferLen,
        outConnectionStringLen, driverCompletion);
}

SQLRETURN SQL_API SQLConnect(SQLHDBC        conn,
                             SQLCHAR*       serverName,
                             SQLSMALLINT    serverNameLen,
                             SQLCHAR*       userName,
                             SQLSMALLINT    userNameLen,
                             SQLCHAR*       auth,
                             SQLSMALLINT    authLen)
{
    return ignite::SQLConnect(conn, serverName, serverNameLen,
        userName, userNameLen, auth, authLen);
}

SQLRETURN SQL_API SQLDisconnect(SQLHDBC conn)
{
    return ignite::SQLDisconnect(conn);
}

SQLRETURN SQL_API SQLPrepare(SQLHSTMT stmt, SQLCHAR* query, SQLINTEGER queryLen)
{
    return ignite::SQLPrepare(stmt, query, queryLen);
}

SQLRETURN SQL_API SQLExecute(SQLHSTMT stmt)
{
    return ignite::SQLExecute(stmt);
}

SQLRETURN SQL_API SQLExecDirect(SQLHSTMT stmt, SQLCHAR* query, SQLINTEGER queryLen)
{
    return ignite::SQLExecDirect(stmt, query, queryLen);
}

SQLRETURN SQL_API SQLBindCol(SQLHSTMT       stmt,
                             SQLUSMALLINT   colNum,
                             SQLSMALLINT    targetType,
                             SQLPOINTER     targetValue,
                             SQLLEN         bufferLength,
                             SQLLEN*        strLengthOrIndicator)
{
    return ignite::SQLBindCol(stmt, colNum, targetType,
        targetValue, bufferLength, strLengthOrIndicator);
}

SQLRETURN SQL_API SQLFetch(SQLHSTMT stmt)
{
    return ignite::SQLFetch(stmt);
}

SQLRETURN SQL_API SQLFetchScroll(SQLHSTMT       stmt,
                                 SQLSMALLINT    orientation,
                                 SQLLEN         offset)
{
    return ignite::SQLFetchScroll(stmt, orientation, offset);
}

SQLRETURN SQL_API SQLExtendedFetch(SQLHSTMT         stmt,
                                   SQLUSMALLINT     orientation,
                                   SQLLEN           offset,
                                   SQLULEN*         rowCount,
                                   SQLUSMALLINT*    rowStatusArray)
{
    return ignite::SQLExtendedFetch(stmt, orientation,
        offset, rowCount, rowStatusArray);
}

SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT stmt, SQLSMALLINT *columnNum)
{
    return ignite::SQLNumResultCols(stmt, columnNum);
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
    return ignite::SQLTables(stmt, catalogName, catalogNameLen,
        schemaName, schemaNameLen, tableName, tableNameLen,
        tableType, tableTypeLen);
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
    return ignite::SQLColumns(stmt, catalogName, catalogNameLen, schemaName,
        schemaNameLen, tableName, tableNameLen, columnName, columnNameLen);
}

SQLRETURN SQL_API SQLMoreResults(SQLHSTMT stmt)
{
    return ignite::SQLMoreResults(stmt);
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
    return ignite::SQLBindParameter(stmt, paramIdx, ioType,
        bufferType, paramSqlType, columnSize, decDigits,
        buffer, bufferLen, resLen);
}

SQLRETURN SQL_API SQLNativeSql(SQLHDBC      conn,
                               SQLCHAR*     inQuery,
                               SQLINTEGER   inQueryLen,
                               SQLCHAR*     outQueryBuffer,
                               SQLINTEGER   outQueryBufferLen,
                               SQLINTEGER*  outQueryLen)
{
    return ignite::SQLNativeSql(conn, inQuery, inQueryLen,
        outQueryBuffer, outQueryBufferLen, outQueryLen);
}


#if defined _WIN64 || !defined _WIN32
SQLRETURN SQL_API SQLColAttribute(SQLHSTMT        stmt,
                                  SQLUSMALLINT    columnNum,
                                  SQLUSMALLINT    fieldId,
                                  SQLPOINTER      strAttr,
                                  SQLSMALLINT     bufferLen,
                                  SQLSMALLINT*    strAttrLen,
                                  SQLLEN*         numericAttr)
#else
SQLRETURN SQL_API SQLColAttribute(SQLHSTMT       stmt,
                                  SQLUSMALLINT   columnNum,
                                  SQLUSMALLINT   fieldId,
                                  SQLPOINTER     strAttr,
                                  SQLSMALLINT    bufferLen,
                                  SQLSMALLINT*   strAttrLen,
                                  SQLPOINTER     numericAttr)
#endif
{
    return ignite::SQLColAttribute(stmt, columnNum, fieldId,
        strAttr, bufferLen, strAttrLen, (SQLLEN*)numericAttr);
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
    return ignite::SQLDescribeCol(stmt, columnNum, columnNameBuf,
        columnNameBufLen, columnNameLen, dataType, columnSize,
        decimalDigits, nullable);
}


SQLRETURN SQL_API SQLRowCount(SQLHSTMT stmt, SQLLEN* rowCnt)
{
    return ignite::SQLRowCount(stmt, rowCnt);
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
    return ignite::SQLForeignKeys(stmt, primaryCatalogName,
        primaryCatalogNameLen, primarySchemaName, primarySchemaNameLen,
        primaryTableName, primaryTableNameLen, foreignCatalogName,
        foreignCatalogNameLen, foreignSchemaName, foreignSchemaNameLen,
        foreignTableName, foreignTableNameLen);
}

SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT       stmt,
                                 SQLINTEGER     attr,
                                 SQLPOINTER     valueBuf,
                                 SQLINTEGER     valueBufLen,
                                 SQLINTEGER*    valueResLen)
{
    return ignite::SQLGetStmtAttr(stmt, attr, valueBuf, valueBufLen, valueResLen);
}

SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT    stmt,
                                 SQLINTEGER  attr,
                                 SQLPOINTER  value,
                                 SQLINTEGER  valueLen)
{
    return ignite::SQLSetStmtAttr(stmt, attr, value, valueLen);
}

SQLRETURN SQL_API SQLPrimaryKeys(SQLHSTMT       stmt,
                                 SQLCHAR*       catalogName,
                                 SQLSMALLINT    catalogNameLen,
                                 SQLCHAR*       schemaName,
                                 SQLSMALLINT    schemaNameLen,
                                 SQLCHAR*       tableName,
                                 SQLSMALLINT    tableNameLen)
{
    return ignite::SQLPrimaryKeys(stmt, catalogName, catalogNameLen,
        schemaName, schemaNameLen, tableName, tableNameLen);
}

SQLRETURN SQL_API SQLNumParams(SQLHSTMT stmt, SQLSMALLINT* paramCnt)
{
    return ignite::SQLNumParams(stmt, paramCnt);
}

SQLRETURN SQL_API SQLGetDiagField(SQLSMALLINT   handleType,
                                  SQLHANDLE     handle,
                                  SQLSMALLINT   recNum,
                                  SQLSMALLINT   diagId,
                                  SQLPOINTER    buffer,
                                  SQLSMALLINT   bufferLen,
                                  SQLSMALLINT*  resLen)
{
    return ignite::SQLGetDiagField(handleType, handle,
        recNum, diagId, buffer, bufferLen, resLen);
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
    return ignite::SQLGetDiagRec(handleType, handle, recNum,
        sqlState, nativeError, msgBuffer, msgBufferLen, msgLen);
}

SQLRETURN SQL_API SQLGetTypeInfo(SQLHSTMT stmt, SQLSMALLINT type)
{
    return ignite::SQLGetTypeInfo(stmt, type);
}

SQLRETURN SQL_API SQLEndTran(SQLSMALLINT    handleType,
                             SQLHANDLE      handle,
                             SQLSMALLINT    completionType)
{
    return ignite::SQLEndTran(handleType, handle, completionType);
}

SQLRETURN SQL_API SQLGetData(SQLHSTMT       stmt,
                             SQLUSMALLINT   colNum,
                             SQLSMALLINT    targetType,
                             SQLPOINTER     targetValue,
                             SQLLEN         bufferLength,
                             SQLLEN*        strLengthOrIndicator)
{
    return ignite::SQLGetData(stmt, colNum, targetType,
        targetValue, bufferLength, strLengthOrIndicator);
}

SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV     env,
                                SQLINTEGER  attr,
                                SQLPOINTER  value,
                                SQLINTEGER  valueLen)
{
    return ignite::SQLSetEnvAttr(env, attr, value, valueLen);
}

SQLRETURN SQL_API SQLGetEnvAttr(SQLHENV     env,
                                SQLINTEGER  attr,
                                SQLPOINTER  valueBuf,
                                SQLINTEGER  valueBufLen,
                                SQLINTEGER* valueResLen)
{
    return ignite::SQLGetEnvAttr(env, attr,
        valueBuf, valueBufLen, valueResLen);
}

SQLRETURN SQL_API SQLSpecialColumns(SQLHSTMT    stmt,
                                    SQLUSMALLINT idType,
                                    SQLCHAR*    catalogName,
                                    SQLSMALLINT catalogNameLen,
                                    SQLCHAR*    schemaName,
                                    SQLSMALLINT schemaNameLen,
                                    SQLCHAR*    tableName,
                                    SQLSMALLINT tableNameLen,
                                    SQLUSMALLINT scope,
                                    SQLUSMALLINT nullable)
{
    return ignite::SQLSpecialColumns(stmt, idType, catalogName, catalogNameLen, schemaName,
        schemaNameLen, tableName, tableNameLen, scope, nullable);
}

SQLRETURN SQL_API SQLParamData(SQLHSTMT stmt, SQLPOINTER* value)
{
    return ignite::SQLParamData(stmt, value);
}

SQLRETURN SQL_API SQLPutData(SQLHSTMT     stmt,
                             SQLPOINTER   data,
                             SQLLEN       strLengthOrIndicator)
{
    return ignite::SQLPutData(stmt, data, strLengthOrIndicator);
}

SQLRETURN SQL_API SQLDescribeParam(SQLHSTMT     stmt,
                                   SQLUSMALLINT paramNum,
                                   SQLSMALLINT* dataType,
                                   SQLULEN*     paramSize,
                                   SQLSMALLINT* decimalDigits,
                                   SQLSMALLINT* nullable)
{
    return ignite::SQLDescribeParam(stmt, paramNum, dataType,
     paramSize, decimalDigits, nullable);
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
    return ignite::SQLError(env, conn, stmt, state,
        error, msgBuf, msgBufLen, msgResLen);
}

//
// ==== Not implemented ====
//

SQLRETURN SQL_API SQLCancel(SQLHSTMT stmt)
{
    LOG_MSG("SQLCancel called");
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
    LOG_MSG("SQLColAttributes called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetCursorName(SQLHSTMT     stmt,
                                   SQLCHAR*     nameBuf,
                                   SQLSMALLINT  nameBufLen,
                                   SQLSMALLINT* nameResLen)
{
    LOG_MSG("SQLGetCursorName called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetCursorName(SQLHSTMT     stmt,
                                   SQLCHAR*     name,
                                   SQLSMALLINT  nameLen)
{
    LOG_MSG("SQLSetCursorName called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetConnectOption(SQLHDBC       conn,
                                      SQLUSMALLINT  option,
                                      SQLPOINTER    value)
{
    LOG_MSG("SQLGetConnectOption called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetStmtOption(SQLHSTMT     stmt,
                                   SQLUSMALLINT option,
                                   SQLPOINTER   value)
{
    LOG_MSG("SQLGetStmtOption called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetConnectOption(SQLHDBC       conn,
                                      SQLUSMALLINT  option,
                                      SQLULEN       value)
{
    LOG_MSG("SQLSetConnectOption called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetStmtOption(SQLHSTMT     stmt,
                                   SQLUSMALLINT option,
                                   SQLULEN      value)
{
    LOG_MSG("SQLSetStmtOption called");
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
    LOG_MSG("SQLStatistics called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLBrowseConnect(SQLHDBC      conn,
                                   SQLCHAR*     inConnectionStr,
                                   SQLSMALLINT  inConnectionStrLen,
                                   SQLCHAR*     outConnectionStrBuf,
                                   SQLSMALLINT  outConnectionStrBufLen,
                                   SQLSMALLINT* outConnectionStrResLen)
{
    LOG_MSG("SQLBrowseConnect called");
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
    LOG_MSG("SQLProcedureColumns called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetPos(SQLHSTMT        stmt,
                            SQLSETPOSIROW   rowNum,
                            SQLUSMALLINT    operation,
                            SQLUSMALLINT    lockType)
{
    LOG_MSG("SQLSetPos called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetScrollOptions(SQLHSTMT      stmt,
                                      SQLUSMALLINT  concurrency,
                                      SQLLEN        crowKeyset,
                                      SQLUSMALLINT  crowRowset)
{
    LOG_MSG("SQLSetScrollOptions called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC     conn,
                                    SQLINTEGER  attr,
                                    SQLPOINTER  valueBuf,
                                    SQLINTEGER  valueBufLen,
                                    SQLINTEGER* valueResLen)
{
    LOG_MSG("SQLGetConnectAttr called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC     conn,
                                    SQLINTEGER  attr,
                                    SQLPOINTER  value,
                                    SQLINTEGER  valueLen)
{
    LOG_MSG("SQLSetConnectAttr called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLBulkOperations(SQLHSTMT       stmt,
                                    SQLUSMALLINT   operation)
{
    LOG_MSG("SQLBulkOperations called");
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
    LOG_MSG("SQLTablePrivileges called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLCopyDesc(SQLHDESC src, SQLHDESC dst)
{
    LOG_MSG("SQLCopyDesc called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLGetDescField(SQLHDESC      descr,
                                  SQLSMALLINT   recNum,
                                  SQLSMALLINT   fieldId,
                                  SQLPOINTER    buffer,
                                  SQLINTEGER    bufferLen,
                                  SQLINTEGER*   resLen)
{
    LOG_MSG("SQLGetDescField called");
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
    LOG_MSG("SQLGetDescRec called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLSetDescField(SQLHDESC      descr,
                                  SQLSMALLINT   recNum,
                                  SQLSMALLINT   fieldId,
                                  SQLPOINTER    buffer,
                                  SQLINTEGER    bufferLen)
{
    LOG_MSG("SQLSetDescField called");
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
    LOG_MSG("SQLSetDescRec called");
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
    LOG_MSG("SQLColumnPrivileges called");
    return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLParamOptions(SQLHSTMT  stmt,
                                  SQLULEN   paramSetSize,
                                  SQLULEN*  paramsProcessed)
{
    LOG_MSG("SQLParamOptions called");
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
    LOG_MSG("SQLProcedures called");
    return SQL_SUCCESS;
}
