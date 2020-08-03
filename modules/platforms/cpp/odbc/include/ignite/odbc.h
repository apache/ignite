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

#ifndef _IGNITE_ODBC_ODBC
#define _IGNITE_ODBC_ODBC

#include "ignite/odbc/system/odbc_constants.h"

/**
 * @file odbc.h
 *
 * Functions here are placed to the ignite namespace so there are no
 * collisions with standard ODBC functions when we call driver API
 * functions from other API functions. I.e, when we call SQLAllocEnv
 * from SQLAllocHandle linker can place Driver Manager call here,
 * instead of internal driver call. On other hand if we call
 * ignite::SQLAllocEnv from ignite::SQLAllocHandle we can be sure 
 * there are no collisions.
 */

namespace ignite
{
    SQLRETURN SQLGetInfo(SQLHDBC        conn,
                         SQLUSMALLINT   infoType,
                         SQLPOINTER     infoValue,
                         SQLSMALLINT    infoValueMax,
                         SQLSMALLINT*   length);

    SQLRETURN SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE* result);

    SQLRETURN SQLAllocEnv(SQLHENV* env);

    SQLRETURN SQLAllocConnect(SQLHENV env, SQLHDBC* conn);

    SQLRETURN SQLAllocStmt(SQLHDBC conn, SQLHSTMT* stmt);

    SQLRETURN SQLFreeHandle(SQLSMALLINT type, SQLHANDLE handle);

    SQLRETURN SQLFreeEnv(SQLHENV env);

    SQLRETURN SQLFreeConnect(SQLHDBC conn);

    SQLRETURN SQLFreeStmt(SQLHSTMT stmt, SQLUSMALLINT option);

    SQLRETURN SQLCloseCursor(SQLHSTMT stmt);

    SQLRETURN SQLDriverConnect(SQLHDBC      conn,
                               SQLHWND      windowHandle,
                               SQLCHAR*     inConnectionString,
                               SQLSMALLINT  inConnectionStringLen,
                               SQLCHAR*     outConnectionString,
                               SQLSMALLINT  outConnectionStringBufferLen,
                               SQLSMALLINT* outConnectionStringLen,
                               SQLUSMALLINT driverCompletion);

    SQLRETURN SQLConnect(SQLHDBC        conn,
                         SQLCHAR*       serverName,
                         SQLSMALLINT    serverNameLen,
                         SQLCHAR*       userName,
                         SQLSMALLINT    userNameLen,
                         SQLCHAR*       auth,
                         SQLSMALLINT    authLen);

    SQLRETURN SQLDisconnect(SQLHDBC conn);

    SQLRETURN SQLPrepare(SQLHSTMT stmt, SQLCHAR* query, SQLINTEGER queryLen);

    SQLRETURN SQLExecute(SQLHSTMT stmt);

    SQLRETURN SQLExecDirect(SQLHSTMT stmt, SQLCHAR* query, SQLINTEGER queryLen);

    SQLRETURN SQLBindCol(SQLHSTMT       stmt,
                         SQLUSMALLINT   colNum,
                         SQLSMALLINT    targetType,
                         SQLPOINTER     targetValue,
                         SQLLEN         bufferLength,
                         SQLLEN*        strLengthOrIndicator);

    SQLRETURN SQLFetch(SQLHSTMT stmt);

    SQLRETURN SQLFetchScroll(SQLHSTMT stmt, SQLSMALLINT orientation, SQLLEN offset);

    SQLRETURN SQLExtendedFetch(SQLHSTMT         stmt,
                               SQLUSMALLINT     orientation,
                               SQLLEN           offset,
                               SQLULEN*         rowCount,
                               SQLUSMALLINT*    rowStatusArray);

    SQLRETURN SQLNumResultCols(SQLHSTMT stmt, SQLSMALLINT *columnNum);

    SQLRETURN SQLTables(SQLHSTMT    stmt,
                        SQLCHAR*    catalogName,
                        SQLSMALLINT catalogNameLen,
                        SQLCHAR*    schemaName,
                        SQLSMALLINT schemaNameLen,
                        SQLCHAR*    tableName,
                        SQLSMALLINT tableNameLen,
                        SQLCHAR*    tableType,
                        SQLSMALLINT tableTypeLen);

    SQLRETURN SQLColumns(SQLHSTMT       stmt,
                         SQLCHAR*       catalogName,
                         SQLSMALLINT    catalogNameLen,
                         SQLCHAR*       schemaName,
                         SQLSMALLINT    schemaNameLen,
                         SQLCHAR*       tableName,
                         SQLSMALLINT    tableNameLen,
                         SQLCHAR*       columnName,
                         SQLSMALLINT    columnNameLen);

    SQLRETURN SQLMoreResults(SQLHSTMT stmt);

    SQLRETURN SQLBindParameter(SQLHSTMT     stmt,
                               SQLUSMALLINT paramIdx,
                               SQLSMALLINT  ioType,
                               SQLSMALLINT  bufferType,
                               SQLSMALLINT  paramSqlType,
                               SQLULEN      columnSize,
                               SQLSMALLINT  decDigits,
                               SQLPOINTER   buffer,
                               SQLLEN       bufferLen,
                               SQLLEN*      resLen);

    SQLRETURN SQLNativeSql(SQLHDBC      conn,
                           SQLCHAR*     inQuery,
                           SQLINTEGER   inQueryLen,
                           SQLCHAR*     outQueryBuffer,
                           SQLINTEGER   outQueryBufferLen,
                           SQLINTEGER*  outQueryLen);

    SQLRETURN SQLColAttribute(SQLHSTMT        stmt,
                              SQLUSMALLINT    columnNum,
                              SQLUSMALLINT    fieldId,
                              SQLPOINTER      strAttr,
                              SQLSMALLINT     bufferLen,
                              SQLSMALLINT*    strAttrLen,
                              SQLLEN*         numericAttr);

    SQLRETURN SQLDescribeCol(SQLHSTMT       stmt,
                             SQLUSMALLINT   columnNum, 
                             SQLCHAR*       columnNameBuf,
                             SQLSMALLINT    columnNameBufLen,
                             SQLSMALLINT*   columnNameLen,
                             SQLSMALLINT*   dataType, 
                             SQLULEN*       columnSize,
                             SQLSMALLINT*   decimalDigits, 
                             SQLSMALLINT*   nullable);

    SQLRETURN SQLRowCount(SQLHSTMT stmt, SQLLEN* rowCnt);

    SQLRETURN SQLForeignKeys(SQLHSTMT       stmt,
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
                             SQLSMALLINT    foreignTableNameLen);

    SQLRETURN SQLGetStmtAttr(SQLHSTMT       stmt,
                             SQLINTEGER     attr,
                             SQLPOINTER     valueBuf,
                             SQLINTEGER     valueBufLen,
                             SQLINTEGER*    valueResLen);

    SQLRETURN SQLSetStmtAttr(SQLHSTMT    stmt,
                             SQLINTEGER  attr,
                             SQLPOINTER  value,
                             SQLINTEGER  valueLen);

    SQLRETURN SQLPrimaryKeys(SQLHSTMT       stmt,
                             SQLCHAR*       catalogName,
                             SQLSMALLINT    catalogNameLen,
                             SQLCHAR*       schemaName,
                             SQLSMALLINT    schemaNameLen,
                             SQLCHAR*       tableName,
                             SQLSMALLINT    tableNameLen);

    SQLRETURN SQLNumParams(SQLHSTMT stmt, SQLSMALLINT* paramCnt);

    SQLRETURN SQLGetDiagField(SQLSMALLINT   handleType,
                              SQLHANDLE     handle,
                              SQLSMALLINT   recNum,
                              SQLSMALLINT   diagId,
                              SQLPOINTER    buffer,
                              SQLSMALLINT   bufferLen,
                              SQLSMALLINT*  resLen);

    SQLRETURN SQLGetDiagRec(SQLSMALLINT     handleType,
                            SQLHANDLE       handle,
                            SQLSMALLINT     recNum,
                            SQLCHAR*        sqlState,
                            SQLINTEGER*     nativeError,
                            SQLCHAR*        msgBuffer,
                            SQLSMALLINT     msgBufferLen,
                            SQLSMALLINT*    msgLen);

    SQLRETURN SQLGetTypeInfo(SQLHSTMT stmt, SQLSMALLINT type);

    SQLRETURN SQLEndTran(SQLSMALLINT handleType, SQLHANDLE handle, SQLSMALLINT completionType);

    SQLRETURN SQLGetData(SQLHSTMT       stmt,
                         SQLUSMALLINT   colNum,
                         SQLSMALLINT    targetType,
                         SQLPOINTER     targetValue,
                         SQLLEN         bufferLength,
                         SQLLEN*        strLengthOrIndicator);

    SQLRETURN SQLSetEnvAttr(SQLHENV     env,
                            SQLINTEGER  attr,
                            SQLPOINTER  value,
                            SQLINTEGER  valueLen);

    SQLRETURN SQLGetEnvAttr(SQLHENV     env,
                            SQLINTEGER  attr,
                            SQLPOINTER  valueBuf,
                            SQLINTEGER  valueBufLen,
                            SQLINTEGER* valueResLen);

    SQLRETURN SQLSpecialColumns(SQLHSTMT    stmt,
                                SQLSMALLINT idType,
                                SQLCHAR*    catalogName,
                                SQLSMALLINT catalogNameLen,
                                SQLCHAR*    schemaName,
                                SQLSMALLINT schemaNameLen,
                                SQLCHAR*    tableName,
                                SQLSMALLINT tableNameLen,
                                SQLSMALLINT scope,
                                SQLSMALLINT nullable);

    SQLRETURN SQLParamData(SQLHSTMT stmt, SQLPOINTER* value);

    SQLRETURN SQLPutData(SQLHSTMT stmt, SQLPOINTER data, SQLLEN strLengthOrIndicator);

    SQLRETURN SQLDescribeParam(SQLHSTMT     stmt,
                               SQLUSMALLINT paramNum,
                               SQLSMALLINT* dataType,
                               SQLULEN*     paramSize,
                               SQLSMALLINT* decimalDigits,
                               SQLSMALLINT* nullable);

    SQLRETURN SQLError(SQLHENV      env,
                       SQLHDBC      conn,
                       SQLHSTMT     stmt,
                       SQLCHAR*     state,
                       SQLINTEGER*  error,
                       SQLCHAR*     msgBuf,
                       SQLSMALLINT  msgBufLen,
                       SQLSMALLINT* msgResLen);

    SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC    conn,
                                        SQLINTEGER attr,
                                        SQLPOINTER valueBuf,
                                        SQLINTEGER valueBufLen,
                                        SQLINTEGER* valueResLen);

    SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC    conn,
                                        SQLINTEGER attr,
                                        SQLPOINTER value,
                                        SQLINTEGER valueLen);
} // namespace ignite

#endif //_IGNITE_ODBC
