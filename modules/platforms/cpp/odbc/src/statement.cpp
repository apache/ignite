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

#include "ignite/odbc/system/odbc_constants.h"
#include "ignite/odbc/query/batch_query.h"
#include "ignite/odbc/query/data_query.h"
#include "ignite/odbc/query/column_metadata_query.h"
#include "ignite/odbc/query/table_metadata_query.h"
#include "ignite/odbc/query/foreign_keys_query.h"
#include "ignite/odbc/query/primary_keys_query.h"
#include "ignite/odbc/query/type_info_query.h"
#include "ignite/odbc/query/special_columns_query.h"
#include "ignite/odbc/connection.h"
#include "ignite/odbc/utility.h"
#include "ignite/odbc/message.h"
#include "ignite/odbc/statement.h"

namespace ignite
{
    namespace odbc
    {
        Statement::Statement(Connection& parent) :
            connection(parent),
            columnBindings(),
            currentQuery(),
            rowsFetched(0),
            rowStatuses(0),
            columnBindOffset(0),
            parameters()
        {
            // No-op.
        }

        Statement::~Statement()
        {
            // No-op.
        }

        void Statement::BindColumn(uint16_t columnIdx, const app::ApplicationDataBuffer& buffer)
        {
            IGNITE_ODBC_API_CALL_ALWAYS_SUCCESS;

            columnBindings[columnIdx] = buffer;
        }

        void Statement::UnbindColumn(uint16_t columnIdx)
        {
            IGNITE_ODBC_API_CALL_ALWAYS_SUCCESS;

            columnBindings.erase(columnIdx);
        }

        void Statement::UnbindAllColumns()
        {
            IGNITE_ODBC_API_CALL_ALWAYS_SUCCESS;

            columnBindings.clear();
        }

        void Statement::SetColumnBindOffsetPtr(int * ptr)
        {
            columnBindOffset = ptr;
        }

        int* Statement::GetColumnBindOffsetPtr()
        {
            return columnBindOffset;
        }

        int32_t Statement::GetColumnNumber()
        {
            int32_t res;

            IGNITE_ODBC_API_CALL(InternalGetColumnNumber(res));

            return res;
        }

        SqlResult Statement::InternalGetColumnNumber(int32_t &res)
        {
            const meta::ColumnMetaVector* meta = GetMeta();

            if (!meta)
            {
                AddStatusRecord(SQL_STATE_HY010_SEQUENCE_ERROR, "Query is not executed.");

                return SQL_RESULT_ERROR;
            }

            res = static_cast<int32_t>(meta->size());

            return SQL_RESULT_SUCCESS;
        }

        void Statement::BindParameter(uint16_t paramIdx, int16_t ioType, int16_t bufferType, int16_t paramSqlType,
            SqlUlen columnSize, int16_t decDigits, void* buffer, SqlLen bufferLen, SqlLen* resLen)
        {
            IGNITE_ODBC_API_CALL(InternalBindParameter(paramIdx, ioType, bufferType, paramSqlType,
                columnSize, decDigits, buffer, bufferLen, resLen));
        }

        SqlResult Statement::InternalBindParameter(uint16_t paramIdx, int16_t ioType, int16_t bufferType,
            int16_t paramSqlType, SqlUlen columnSize, int16_t decDigits, void* buffer, SqlLen bufferLen, SqlLen* resLen)
        {
            using namespace type_traits;
            using app::ApplicationDataBuffer;
            using app::Parameter;

            if (paramIdx == 0)
            {
                std::stringstream builder;
                builder << "The value specified for the argument ParameterNumber was less than 1. [ParameterNumber=" << paramIdx << ']';

                AddStatusRecord(SQL_STATE_24000_INVALID_CURSOR_STATE, builder.str());

                return SQL_RESULT_ERROR;
            }

            if (ioType != SQL_PARAM_INPUT)
            {
                std::stringstream builder;
                builder << "The value specified for the argument InputOutputType was not SQL_PARAM_INPUT. [ioType=" << ioType << ']';

                AddStatusRecord(SQL_STATE_HY105_INVALID_PARAMETER_TYPE, builder.str());

                return SQL_RESULT_ERROR;
            }

            if (!IsSqlTypeSupported(paramSqlType))
            {
                std::stringstream builder;
                builder << "Data type is not supported. [typeId=" << paramSqlType << ']';

                AddStatusRecord(SQL_STATE_HYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, builder.str());

                return SQL_RESULT_ERROR;
            }

            IgniteSqlType driverType = ToDriverType(bufferType);

            if (driverType == IGNITE_ODBC_C_TYPE_UNSUPPORTED)
            {
                std::stringstream builder;
                builder << "The argument TargetType was not a valid data type. [TargetType=" << bufferType << ']';

                AddStatusRecord(SQL_STATE_HY003_INVALID_APPLICATION_BUFFER_TYPE, builder.str());

                return SQL_RESULT_ERROR;
            }

            if (buffer)
            {
                ApplicationDataBuffer dataBuffer(driverType, buffer, bufferLen, resLen);

                Parameter param(dataBuffer, paramSqlType, columnSize, decDigits);

                parameters.BindParameter(paramIdx, param);
            }
            else
                parameters.UnbindParameter(paramIdx);

            return SQL_RESULT_SUCCESS;
        }

        void Statement::UnbindParameter(uint16_t paramIdx)
        {
            IGNITE_ODBC_API_CALL_ALWAYS_SUCCESS;

            parameters.UnbindParameter(paramIdx);
        }

        void Statement::UnbindAllParameters()
        {
            IGNITE_ODBC_API_CALL_ALWAYS_SUCCESS;

            parameters.UnbindAll();
        }

        void Statement::SetAttribute(int attr, void* value, SQLINTEGER valueLen)
        {
            IGNITE_ODBC_API_CALL(InternalSetAttribute(attr, value, valueLen));
        }

        SqlResult Statement::InternalSetAttribute(int attr, void* value, SQLINTEGER valueLen)
        {
            switch (attr)
            {
                case SQL_ATTR_ROW_ARRAY_SIZE:
                {
                    SqlUlen val = reinterpret_cast<SqlUlen>(value);

                    LOG_MSG("SQL_ATTR_ROW_ARRAY_SIZE: %d\n", val);

                    if (val != 1)
                    {
                        AddStatusRecord(SQL_STATE_HYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                            "Fetching of more than one row by call is not supported.");

                        return SQL_RESULT_ERROR;
                    }

                    break;
                }

                case SQL_ATTR_ROWS_FETCHED_PTR:
                {
                    SetRowsFetchedPtr(reinterpret_cast<size_t*>(value));

                    break;
                }

                case SQL_ATTR_ROW_STATUS_PTR:
                {
                    SetRowStatusesPtr(reinterpret_cast<uint16_t*>(value));

                    break;
                }

                case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
                {
                    SetParamBindOffsetPtr(reinterpret_cast<int*>(value));

                    break;
                }

                case SQL_ATTR_ROW_BIND_OFFSET_PTR:
                {
                    SetColumnBindOffsetPtr(reinterpret_cast<int*>(value));

                    break;
                }

                case SQL_ATTR_PARAMSET_SIZE:
                {
                    parameters.SetParamSetSize(reinterpret_cast<SqlUlen>(value));

                    break;
                }

                case SQL_ATTR_PARAMS_PROCESSED_PTR:
                {
                    parameters.SetParamsProcessedPtr(reinterpret_cast<SqlUlen*>(value));

                    break;
                }

                default:
                {
                    AddStatusRecord(SQL_STATE_HYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                        "Specified attribute is not supported.");

                    return SQL_RESULT_ERROR;
                }
            }

            return SQL_RESULT_SUCCESS;
        }

        void Statement::GetAttribute(int attr, void* buf, SQLINTEGER bufLen, SQLINTEGER* valueLen)
        {
            IGNITE_ODBC_API_CALL(InternalGetAttribute(attr, buf, bufLen, valueLen));
        }

        SqlResult Statement::InternalGetAttribute(int attr, void* buf, SQLINTEGER bufLen, SQLINTEGER* valueLen)
        {
            if (!buf)
            {
                AddStatusRecord(SQL_STATE_HY000_GENERAL_ERROR, "Data buffer is NULL.");

                return SQL_RESULT_ERROR;
            }

            switch (attr)
            {
                case SQL_ATTR_APP_ROW_DESC:
                case SQL_ATTR_APP_PARAM_DESC:
                case SQL_ATTR_IMP_ROW_DESC:
                case SQL_ATTR_IMP_PARAM_DESC:
                {
                    SQLPOINTER *val = reinterpret_cast<SQLPOINTER*>(buf);

                    *val = static_cast<SQLPOINTER>(this);

                    if (valueLen)
                        *valueLen = SQL_IS_POINTER;

                    break;
                }

                case SQL_ATTR_ROW_ARRAY_SIZE:
                {
                    SQLINTEGER *val = reinterpret_cast<SQLINTEGER*>(buf);

                    *val = static_cast<SQLINTEGER>(1);

                    if (valueLen)
                        *valueLen = SQL_IS_INTEGER;

                    break;
                }

                case SQL_ATTR_ROWS_FETCHED_PTR:
                {
                    SqlUlen** val = reinterpret_cast<SqlUlen**>(buf);

                    *val = reinterpret_cast<SqlUlen*>(GetRowsFetchedPtr());

                    if (valueLen)
                        *valueLen = SQL_IS_POINTER;

                    break;
                }

                case SQL_ATTR_ROW_STATUS_PTR:
                {
                    SQLUSMALLINT** val = reinterpret_cast<SQLUSMALLINT**>(buf);

                    *val = reinterpret_cast<SQLUSMALLINT*>(GetRowStatusesPtr());

                    if (valueLen)
                        *valueLen = SQL_IS_POINTER;

                    break;
                }

                case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
                {
                    SQLULEN** val = reinterpret_cast<SQLULEN**>(buf);

                    *val = reinterpret_cast<SQLULEN*>(parameters.GetParamBindOffsetPtr());

                    if (valueLen)
                        *valueLen = SQL_IS_POINTER;

                    break;
                }

                case SQL_ATTR_ROW_BIND_OFFSET_PTR:
                {
                    SqlUlen** val = reinterpret_cast<SqlUlen**>(buf);

                    *val = reinterpret_cast<SqlUlen*>(GetColumnBindOffsetPtr());

                    if (valueLen)
                        *valueLen = SQL_IS_POINTER;

                    break;
                }

                case SQL_ATTR_PARAMSET_SIZE:
                {
                    SqlUlen* val = reinterpret_cast<SqlUlen*>(buf);

                    *val = static_cast<SqlUlen>(parameters.GetParamSetSize());

                    if (valueLen)
                        *valueLen = SQL_IS_UINTEGER;

                    break;
                }

                case SQL_ATTR_PARAMS_PROCESSED_PTR:
                {
                    SqlUlen** val = reinterpret_cast<SqlUlen**>(buf);

                    *val = parameters.GetParamsProcessedPtr();

                    if (valueLen)
                        *valueLen = SQL_IS_POINTER;

                    break;
                }

                default:
                {
                    AddStatusRecord(SQL_STATE_HYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                        "Specified attribute is not supported.");

                    return SQL_RESULT_ERROR;
                }
            }

            return SQL_RESULT_SUCCESS;
        }

        void Statement::GetParametersNumber(uint16_t& paramNum)
        {
            IGNITE_ODBC_API_CALL(InternalGetParametersNumber(paramNum));
        }

        SqlResult Statement::InternalGetParametersNumber(uint16_t& paramNum)
        {
            if (!currentQuery.get())
            {
                AddStatusRecord(SQL_STATE_HY010_SEQUENCE_ERROR, "Query is not prepared.");

                return SQL_RESULT_ERROR;
            }

            if (currentQuery->GetType() != query::Query::DATA)
            {
                paramNum = 0;

                return SQL_RESULT_SUCCESS;
            }

            if (!parameters.IsMetadataSet())
            {
                SqlResult res = UpdateParamsMeta();

                if (res != SQL_RESULT_SUCCESS)
                    return res;
            }

            paramNum = parameters.GetExpectedParamNum();

            return SQL_RESULT_SUCCESS;
        }

        void Statement::SetParamBindOffsetPtr(int* ptr)
        {
            IGNITE_ODBC_API_CALL_ALWAYS_SUCCESS;

            parameters.SetParamBindOffsetPtr(ptr);
        }

        void Statement::GetColumnData(uint16_t columnIdx, app::ApplicationDataBuffer& buffer)
        {
            IGNITE_ODBC_API_CALL(InternalGetColumnData(columnIdx, buffer));
        }

        SqlResult Statement::InternalGetColumnData(uint16_t columnIdx,
            app::ApplicationDataBuffer& buffer)
        {
            if (!currentQuery.get())
            {
                AddStatusRecord(SQL_STATE_24000_INVALID_CURSOR_STATE,
                    "Cursor is not in the open state.");

                return SQL_RESULT_ERROR;
            }

            SqlResult res = currentQuery->GetColumn(columnIdx, buffer);

            return res;
        }

        void Statement::PrepareSqlQuery(const std::string& query)
        {
            IGNITE_ODBC_API_CALL(InternalPrepareSqlQuery(query));
        }

        SqlResult Statement::InternalPrepareSqlQuery(const std::string& query)
        {
            if (currentQuery.get())
                currentQuery->Close();

            // Resetting parameters types as we are changing the query.
            parameters.Prepare();

            currentQuery.reset(new query::DataQuery(*this, connection, query, parameters));

            return SQL_RESULT_SUCCESS;
        }

        void Statement::ExecuteSqlQuery(const std::string& query)
        {
            IGNITE_ODBC_API_CALL(InternalExecuteSqlQuery(query));
        }

        SqlResult Statement::InternalExecuteSqlQuery(const std::string& query)
        {
            SqlResult result = InternalPrepareSqlQuery(query);

            if (result != SQL_RESULT_SUCCESS)
                return result;

            return InternalExecuteSqlQuery();
        }

        void Statement::ExecuteSqlQuery()
        {
            IGNITE_ODBC_API_CALL(InternalExecuteSqlQuery());
        }

        SqlResult Statement::InternalExecuteSqlQuery()
        {
            if (!currentQuery.get())
            {
                AddStatusRecord(SQL_STATE_HY010_SEQUENCE_ERROR, "Query is not prepared.");

                return SQL_RESULT_ERROR;
            }

            if (parameters.GetParamSetSize() > 1 && currentQuery->GetType() == query::Query::DATA)
            {
                query::DataQuery& qry = static_cast<query::DataQuery&>(*currentQuery);

                currentQuery.reset(new query::BatchQuery(*this, connection, qry.GetSql(), parameters));
            }
            else if (parameters.GetParamSetSize() == 1 && currentQuery->GetType() == query::Query::BATCH)
            {
                query::BatchQuery& qry = static_cast<query::BatchQuery&>(*currentQuery);

                currentQuery.reset(new query::DataQuery(*this, connection, qry.GetSql(), parameters));
            }

            if (parameters.IsDataAtExecNeeded())
            {
                if (currentQuery->GetType() == query::Query::BATCH)
                {
                    AddStatusRecord(SQL_STATE_HYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                        "Data-at-execution is not supported together with batching.");

                    return SQL_RESULT_ERROR;
                }

                return SQL_RESULT_NEED_DATA;
            }

            return currentQuery->Execute();
        }

        void Statement::ExecuteGetColumnsMetaQuery(const std::string& schema,
            const std::string& table, const std::string& column)
        {
            IGNITE_ODBC_API_CALL(InternalExecuteGetColumnsMetaQuery(schema, table, column));
        }

        SqlResult Statement::InternalExecuteGetColumnsMetaQuery(const std::string& schema,
            const std::string& table, const std::string& column)
        {
            if (currentQuery.get())
                currentQuery->Close();

            std::string cache(schema);

            if (cache.empty())
                cache = connection.GetCache();

            currentQuery.reset(new query::ColumnMetadataQuery(*this,
                connection, cache, table, column));

            return currentQuery->Execute();
        }

        void Statement::ExecuteGetTablesMetaQuery(const std::string& catalog,
            const std::string& schema, const std::string& table, const std::string& tableType)
        {
            IGNITE_ODBC_API_CALL(InternalExecuteGetTablesMetaQuery(
                catalog, schema, table, tableType));
        }

        SqlResult Statement::InternalExecuteGetTablesMetaQuery(const std::string& catalog,
            const std::string& schema, const std::string& table, const std::string& tableType)
        {
            if (currentQuery.get())
                currentQuery->Close();

            currentQuery.reset(new query::TableMetadataQuery(*this,
                connection, catalog, schema, table, tableType));

            return currentQuery->Execute();
        }

        void Statement::ExecuteGetForeignKeysQuery(const std::string& primaryCatalog,
            const std::string& primarySchema, const std::string& primaryTable,
            const std::string& foreignCatalog, const std::string& foreignSchema,
            const std::string& foreignTable)
        {
            IGNITE_ODBC_API_CALL(InternalExecuteGetForeignKeysQuery(primaryCatalog,
                primarySchema, primaryTable, foreignCatalog, foreignSchema, foreignTable));
        }

        SqlResult Statement::InternalExecuteGetForeignKeysQuery(const std::string& primaryCatalog,
            const std::string& primarySchema, const std::string& primaryTable,
            const std::string& foreignCatalog, const std::string& foreignSchema,
            const std::string& foreignTable)
        {
            if (currentQuery.get())
                currentQuery->Close();

            currentQuery.reset(new query::ForeignKeysQuery(*this, connection, primaryCatalog,
                primarySchema, primaryTable, foreignCatalog, foreignSchema, foreignTable));

            return currentQuery->Execute();
        }

        void Statement::ExecuteGetPrimaryKeysQuery(const std::string& catalog,
            const std::string& schema, const std::string& table)
        {
            IGNITE_ODBC_API_CALL(InternalExecuteGetPrimaryKeysQuery(catalog, schema, table));
        }

        SqlResult Statement::InternalExecuteGetPrimaryKeysQuery(const std::string& catalog,
            const std::string& schema, const std::string& table)
        {
            if (currentQuery.get())
                currentQuery->Close();

            currentQuery.reset(new query::PrimaryKeysQuery(*this,
                connection, catalog, schema, table));

            return currentQuery->Execute();
        }

        void Statement::ExecuteSpecialColumnsQuery(int16_t type,
            const std::string& catalog, const std::string& schema,
            const std::string& table, int16_t scope, int16_t nullable)
        {
            IGNITE_ODBC_API_CALL(InternalExecuteSpecialColumnsQuery(type,
                catalog, schema, table, scope, nullable));
        }

        SqlResult Statement::InternalExecuteSpecialColumnsQuery(int16_t type,
            const std::string& catalog, const std::string& schema,
            const std::string& table, int16_t scope, int16_t nullable)
        {
            if (type != SQL_BEST_ROWID && type != SQL_ROWVER)
            {
                AddStatusRecord(SQL_STATE_HY097_COLUMN_TYPE_OUT_OF_RANGE,
                    "An invalid IdentifierType value was specified.");

                return SQL_RESULT_ERROR;
            }

            if (currentQuery.get())
                currentQuery->Close();

            currentQuery.reset(new query::SpecialColumnsQuery(*this, type,
                catalog, schema, table, scope, nullable));

            return currentQuery->Execute();
        }

        void Statement::ExecuteGetTypeInfoQuery(int16_t sqlType)
        {
            IGNITE_ODBC_API_CALL(InternalExecuteGetTypeInfoQuery(sqlType));
        }

        SqlResult Statement::InternalExecuteGetTypeInfoQuery(int16_t sqlType)
        {
            if (!type_traits::IsSqlTypeSupported(sqlType))
            {
                std::stringstream builder;
                builder << "Data type is not supported. [typeId=" << sqlType << ']';

                AddStatusRecord(SQL_STATE_HYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, builder.str());

                return SQL_RESULT_ERROR;
            }

            if (currentQuery.get())
                currentQuery->Close();

            currentQuery.reset(new query::TypeInfoQuery(*this, sqlType));

            return currentQuery->Execute();
        }

        void Statement::Close()
        {
            IGNITE_ODBC_API_CALL(InternalClose());
        }

        SqlResult Statement::InternalClose()
        {
            if (!currentQuery.get())
                return SQL_RESULT_SUCCESS;

            SqlResult result = currentQuery->Close();

            return result;
        }

        void Statement::FetchRow()
        {
            IGNITE_ODBC_API_CALL(InternalFetchRow());
        }

        SqlResult Statement::InternalFetchRow()
        {
            if (rowsFetched)
                *rowsFetched = 0;

            if (!currentQuery.get())
            {
                AddStatusRecord(SQL_STATE_24000_INVALID_CURSOR_STATE,
                    "Cursor is not in the open state.");

                return SQL_RESULT_ERROR;
            }

            if (columnBindOffset)
            {
                for (app::ColumnBindingMap::iterator it = columnBindings.begin(); it != columnBindings.end(); ++it)
                    it->second.SetByteOffset(*columnBindOffset);
            }

            SqlResult res = currentQuery->FetchNextRow(columnBindings);

            if (res == SQL_RESULT_SUCCESS)
            {
                if (rowsFetched)
                    *rowsFetched = 1;

                if (rowStatuses)
                    rowStatuses[0] = SQL_ROW_SUCCESS;
            }

            return res;
        }

        const meta::ColumnMetaVector* Statement::GetMeta() const
        {
            if (!currentQuery.get())
                return 0;

            return &currentQuery->GetMeta();
        }

        bool Statement::DataAvailable() const
        {
            return currentQuery.get() && currentQuery->DataAvailable();
        }

        void Statement::NextResults()
        {
            IGNITE_ODBC_API_CALL(InternalNextResults());
        }

        SqlResult Statement::InternalNextResults()
        {
            if (!currentQuery.get())
                return SQL_RESULT_NO_DATA;

            SqlResult result = currentQuery->Close();

            return result == SQL_RESULT_SUCCESS ? SQL_RESULT_NO_DATA : result;
        }

        void Statement::GetColumnAttribute(uint16_t colIdx, uint16_t attrId,
            char* strbuf, int16_t buflen, int16_t* reslen, SqlLen* numbuf)
        {
            IGNITE_ODBC_API_CALL(InternalGetColumnAttribute(colIdx, attrId,
                strbuf, buflen, reslen, numbuf));
        }

        SqlResult Statement::InternalGetColumnAttribute(uint16_t colIdx,
            uint16_t attrId, char* strbuf, int16_t buflen, int16_t* reslen,
            SqlLen* numbuf)
        {
            const meta::ColumnMetaVector *meta = GetMeta();

            if (!meta)
            {
                AddStatusRecord(SQL_STATE_HY010_SEQUENCE_ERROR, "Query is not executed.");

                return SQL_RESULT_ERROR;
            }

            if (colIdx > meta->size() + 1 || colIdx < 1)
            {
                AddStatusRecord(SQL_STATE_HY000_GENERAL_ERROR,
                    "Column index is out of range.", 0, colIdx);

                return SQL_RESULT_ERROR;
            }

            const meta::ColumnMeta& columnMeta = meta->at(colIdx - 1);

            bool found = false;

            if (numbuf)
                found = columnMeta.GetAttribute(attrId, *numbuf);

            if (!found)
            {
                std::string out;

                found = columnMeta.GetAttribute(attrId, out);

                size_t outSize = out.size();

                if (found && strbuf)
                    outSize = utility::CopyStringToBuffer(out, strbuf, buflen);

                if (found && reslen)
                    *reslen = static_cast<int16_t>(outSize);
            }

            if (!found)
            {
                AddStatusRecord(SQL_STATE_HYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                    "Unknown attribute.");

                return SQL_RESULT_ERROR;
            }

            return SQL_RESULT_SUCCESS;
        }

        int64_t Statement::AffectedRows()
        {
            int64_t rowCnt = 0;

            IGNITE_ODBC_API_CALL(InternalAffectedRows(rowCnt));

            return rowCnt;
        }

        SqlResult Statement::InternalAffectedRows(int64_t& rowCnt)
        {
            if (!currentQuery.get())
            {
                AddStatusRecord(SQL_STATE_HY010_SEQUENCE_ERROR, "Query is not executed.");

                return SQL_RESULT_ERROR;
            }

            rowCnt = currentQuery->AffectedRows();

            return SQL_RESULT_SUCCESS;
        }

        void Statement::SetRowsFetchedPtr(size_t* ptr)
        {
            rowsFetched = ptr;
        }

        size_t* Statement::GetRowsFetchedPtr()
        {
            return rowsFetched;
        }

        void Statement::SetRowStatusesPtr(uint16_t* ptr)
        {
            rowStatuses = ptr;
        }

        uint16_t * Statement::GetRowStatusesPtr()
        {
            return rowStatuses;
        }

        void Statement::SelectParam(void** paramPtr)
        {
            IGNITE_ODBC_API_CALL(InternalSelectParam(paramPtr));
        }

        SqlResult Statement::InternalSelectParam(void** paramPtr)
        {
            if (!paramPtr)
            {
                AddStatusRecord(SQL_STATE_HY000_GENERAL_ERROR,
                    "Invalid parameter: ValuePtrPtr is null.");

                return SQL_RESULT_ERROR;
            }

            if (!currentQuery.get())
            {
                AddStatusRecord(SQL_STATE_HY010_SEQUENCE_ERROR, "Query is not prepared.");

                return SQL_RESULT_ERROR;
            }

            app::Parameter *selected = parameters.GetSelectedParameter();

            if (selected && !selected->IsDataReady())
            {
                AddStatusRecord(SQL_STATE_22026_DATA_LENGTH_MISMATCH,
                    "Less data was sent for a parameter than was specified with "
                    "the StrLen_or_IndPtr argument in SQLBindParameter.");

                return SQL_RESULT_ERROR;
            }

            selected = parameters.SelectNextParameter();

            if (selected)
            {
                *paramPtr = selected->GetBuffer().GetData();

                return SQL_RESULT_NEED_DATA;
            }

            SqlResult res = currentQuery->Execute();

            if (res != SQL_RESULT_SUCCESS)
                res = SQL_RESULT_SUCCESS_WITH_INFO;

            return res;
        }

        void Statement::PutData(void* data, SqlLen len)
        {
            IGNITE_ODBC_API_CALL(InternalPutData(data, len));
        }

        SqlResult Statement::InternalPutData(void* data, SqlLen len)
        {
            if (!data && len != 0 && len != SQL_DEFAULT_PARAM && len != SQL_NULL_DATA)
            {
                AddStatusRecord(SQL_STATE_HY009_INVALID_USE_OF_NULL_POINTER,
                    "Invalid parameter: DataPtr is null StrLen_or_Ind is not 0, "
                    "SQL_DEFAULT_PARAM, or SQL_NULL_DATA.");

                return SQL_RESULT_ERROR;
            }

            if (!parameters.IsParameterSelected())
            {
                AddStatusRecord(SQL_STATE_HY010_SEQUENCE_ERROR,
                    "Parameter is not selected with the SQLParamData.");

                return SQL_RESULT_ERROR;
            }

            app::Parameter* param = parameters.GetSelectedParameter();

            if (!param)
            {
                AddStatusRecord(SQL_STATE_HY000_GENERAL_ERROR,
                    "Selected parameter has been unbound.");

                return SQL_RESULT_ERROR;
            }

            param->PutData(data, len);

            return SQL_RESULT_SUCCESS;
        }

        void Statement::DescribeParam(int16_t paramNum, int16_t* dataType,
            SqlUlen* paramSize, int16_t* decimalDigits, int16_t* nullable)
        {
            IGNITE_ODBC_API_CALL(InternalDescribeParam(paramNum,
                dataType, paramSize, decimalDigits, nullable));
        }

        SqlResult Statement::InternalDescribeParam(int16_t paramNum, int16_t* dataType,
            SqlUlen* paramSize, int16_t* decimalDigits, int16_t* nullable)
        {
            query::Query *qry = currentQuery.get();
            if (!qry)
            {
                AddStatusRecord(SQL_STATE_HY010_SEQUENCE_ERROR, "Query is not prepared.");

                return SQL_RESULT_ERROR;
            }

            if (qry->GetType() != query::Query::DATA)
            {
                AddStatusRecord(SQL_STATE_HY010_SEQUENCE_ERROR, "Query is not SQL data query.");

                return SQL_RESULT_ERROR;
            }

            int8_t type = parameters.GetParamType(paramNum, 0);

            LOG_MSG("Type: %d\n", type);

            if (!type)
            {
                SqlResult res = UpdateParamsMeta();

                if (res != SQL_RESULT_SUCCESS)
                    return res;

                type = parameters.GetParamType(paramNum, impl::binary::IGNITE_HDR_NULL);
            }

            if (dataType)
                *dataType = type_traits::BinaryToSqlType(type);

            if (paramSize)
                *paramSize = type_traits::BinaryTypeColumnSize(type);

            if (decimalDigits)
                *decimalDigits = type_traits::BinaryTypeDecimalDigits(type);

            if (nullable)
                *nullable = type_traits::BinaryTypeNullability(type);

            return SQL_RESULT_SUCCESS;
        }

        SqlResult Statement::UpdateParamsMeta()
        {
            query::Query *qry0 = currentQuery.get();

            assert(qry0 != 0);
            assert(qry0->GetType() == query::Query::DATA);

            query::DataQuery* qry = static_cast<query::DataQuery*>(qry0);

            const std::string& cacheName = connection.GetCache();
            const std::string& sql = qry->GetSql();

            QueryGetParamsMetaRequest req(cacheName, sql);
            QueryGetParamsMetaResponse rsp;

            try
            {
                connection.SyncMessage(req, rsp);
            }
            catch (const IgniteError& err)
            {
                AddStatusRecord(SQL_STATE_HYT01_CONNECTIOIN_TIMEOUT, err.GetText());

                return SQL_RESULT_ERROR;
            }

            if (rsp.GetStatus() != RESPONSE_STATUS_SUCCESS)
            {
                LOG_MSG("Error: %s\n", rsp.GetError().c_str());

                AddStatusRecord(SQL_STATE_HY000_GENERAL_ERROR, rsp.GetError());

                return SQL_RESULT_ERROR;
            }

            parameters.UpdateParamsTypes(rsp.GetTypeIds());

            for (size_t i = 0; i < rsp.GetTypeIds().size(); ++i)
            {
                LOG_MSG("[" << i << "] Parameter type: " << rsp.GetTypeIds()[i]);
            }

            return SQL_RESULT_SUCCESS;
        }
    }
}



































