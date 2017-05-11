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
#include "ignite/odbc/log.h"

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
            paramBindOffset(0),
            columnBindOffset(0),
            currentParamIdx(0)
        {
            // No-op.
        }

        Statement::~Statement()
        {
            // No-op.
        }

        void Statement::BindColumn(uint16_t columnIdx, int16_t targetType, void* targetValue, SqlLen bufferLength, SqlLen* strLengthOrIndicator)
        {
            IGNITE_ODBC_API_CALL(InternalBindColumn(columnIdx, targetType, targetValue, bufferLength, strLengthOrIndicator));
        }

        SqlResult::Type Statement::InternalBindColumn(uint16_t columnIdx, int16_t targetType, void* targetValue, SqlLen bufferLength, SqlLen* strLengthOrIndicator)
        {
            using namespace odbc::type_traits;
            OdbcNativeType::Type driverType = ToDriverType(targetType);

            if (driverType == OdbcNativeType::AI_UNSUPPORTED)
            {
                AddStatusRecord(odbc::SqlState::SHY003_INVALID_APPLICATION_BUFFER_TYPE, "The argument TargetType was not a valid data type.");

                return SqlResult::AI_ERROR;
            }

            if (bufferLength < 0)
            {
                AddStatusRecord(odbc::SqlState::SHY090_INVALID_STRING_OR_BUFFER_LENGTH,
                    "The value specified for the argument BufferLength was less than 0.");

                return SqlResult::AI_ERROR;
            }

            if (targetValue || strLengthOrIndicator)
            {
                app::ApplicationDataBuffer dataBuffer(driverType, targetValue, bufferLength, strLengthOrIndicator);

                SafeBindColumn(columnIdx, dataBuffer);
            }
            else
                SafeUnbindColumn(columnIdx);

            return SqlResult::AI_SUCCESS;
        }

        void Statement::SafeBindColumn(uint16_t columnIdx, const app::ApplicationDataBuffer& buffer)
        {
            columnBindings[columnIdx] = buffer;

            columnBindings[columnIdx].SetPtrToOffsetPtr(&columnBindOffset);
        }

        void Statement::SafeUnbindColumn(uint16_t columnIdx)
        {
            columnBindings.erase(columnIdx);
        }

        void Statement::SafeUnbindAllColumns()
        {
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

        SqlResult::Type Statement::InternalGetColumnNumber(int32_t &res)
        {
            const meta::ColumnMetaVector* meta = GetMeta();

            if (!meta)
            {
                AddStatusRecord(SqlState::SHY010_SEQUENCE_ERROR, "Query is not executed.");

                return SqlResult::AI_ERROR;
            }

            res = static_cast<int32_t>(meta->size());

            return SqlResult::AI_SUCCESS;
        }

        void Statement::BindParameter(uint16_t paramIdx, int16_t ioType, int16_t bufferType, int16_t paramSqlType,
                                      SqlUlen columnSize, int16_t decDigits, void* buffer, SqlLen bufferLen, SqlLen* resLen)
        {
            IGNITE_ODBC_API_CALL(InternalBindParameter(paramIdx, ioType, bufferType, paramSqlType, columnSize, decDigits, buffer, bufferLen, resLen));
        }

        SqlResult::Type Statement::InternalBindParameter(uint16_t paramIdx, int16_t ioType, int16_t bufferType, int16_t paramSqlType,
                                                   SqlUlen columnSize, int16_t decDigits, void* buffer, SqlLen bufferLen, SqlLen* resLen)
        {
            using namespace odbc::type_traits;
            using odbc::Statement;
            using odbc::app::ApplicationDataBuffer;
            using odbc::app::Parameter;
            using odbc::type_traits::IsSqlTypeSupported;

            if (paramIdx == 0)
            {
                AddStatusRecord(SqlState::S24000_INVALID_CURSOR_STATE,
                    "The value specified for the argument ParameterNumber was less than 1.");

                return SqlResult::AI_ERROR;
            }

            if (ioType != SQL_PARAM_INPUT)
            {
                AddStatusRecord(SqlState::SHY105_INVALID_PARAMETER_TYPE,
                    "The value specified for the argument InputOutputType was not SQL_PARAM_INPUT.");

                return SqlResult::AI_ERROR;
            }

            if (!IsSqlTypeSupported(paramSqlType))
            {
                AddStatusRecord(SqlState::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                    "Data type is not supported.");

                return SqlResult::AI_ERROR;
            }

            OdbcNativeType::Type driverType = ToDriverType(bufferType);

            if (driverType == OdbcNativeType::AI_UNSUPPORTED)
            {
                AddStatusRecord(odbc::SqlState::SHY003_INVALID_APPLICATION_BUFFER_TYPE,
                    "The argument TargetType was not a valid data type.");

                return SqlResult::AI_ERROR;
            }

            if (buffer)
            {
                ApplicationDataBuffer dataBuffer(driverType, buffer, bufferLen, resLen);

                Parameter param(dataBuffer, paramSqlType, columnSize, decDigits);

                SafeBindParameter(paramIdx, param);
            }
            else
                SafeUnbindParameter(paramIdx);

            return SqlResult::AI_SUCCESS;
        }

        void Statement::SafeBindParameter(uint16_t paramIdx, const app::Parameter& param)
        {
            paramBindings[paramIdx] = param;

            paramBindings[paramIdx].GetBuffer().SetPtrToOffsetPtr(&paramBindOffset);
        }

        void Statement::SafeUnbindParameter(uint16_t paramIdx)
        {
            paramBindings.erase(paramIdx);
        }

        void Statement::SafeUnbindAllParameters()
        {
            paramBindings.clear();
        }

        void Statement::SetAttribute(int attr, void* value, SQLINTEGER valueLen)
        {
            IGNITE_ODBC_API_CALL(InternalSetAttribute(attr, value, valueLen));
        }

        SqlResult::Type Statement::InternalSetAttribute(int attr, void* value, SQLINTEGER valueLen)
        {
            switch (attr)
            {
                case SQL_ATTR_ROW_ARRAY_SIZE:
                {
                    SQLULEN val = reinterpret_cast<SQLULEN>(value);

                    LOG_MSG("SQL_ATTR_ROW_ARRAY_SIZE: " << val);

                    if (val != 1)
                    {
                        AddStatusRecord(SqlState::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                            "Fetching of more than one row by call is not supported.");

                        return SqlResult::AI_ERROR;
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

                default:
                {
                    AddStatusRecord(SqlState::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                        "Specified attribute is not supported.");

                    return SqlResult::AI_ERROR;
                }
            }

            return SqlResult::AI_SUCCESS;
        }

        void Statement::GetAttribute(int attr, void* buf, SQLINTEGER bufLen, SQLINTEGER* valueLen)
        {
            IGNITE_ODBC_API_CALL(InternalGetAttribute(attr, buf, bufLen, valueLen));
        }

        SqlResult::Type Statement::InternalGetAttribute(int attr, void* buf, SQLINTEGER bufLen, SQLINTEGER* valueLen)
        {
            if (!buf)
            {
                AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Data buffer is NULL.");

                return SqlResult::AI_ERROR;
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

                    break;
                }

                case SQL_ATTR_ROW_ARRAY_SIZE:
                {
                    SQLINTEGER *val = reinterpret_cast<SQLINTEGER*>(buf);

                    *val = static_cast<SQLINTEGER>(1);

                    break;
                }

                case SQL_ATTR_ROWS_FETCHED_PTR:
                {
                    SQLULEN** val = reinterpret_cast<SQLULEN**>(buf);

                    *val = reinterpret_cast<SQLULEN*>(GetRowsFetchedPtr());

                    break;
                }

                case SQL_ATTR_ROW_STATUS_PTR:
                {
                    SQLUSMALLINT** val = reinterpret_cast<SQLUSMALLINT**>(buf);

                    *val = reinterpret_cast<SQLUSMALLINT*>(GetRowStatusesPtr());

                    break;
                }

                case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
                {
                    SQLULEN** val = reinterpret_cast<SQLULEN**>(buf);

                    *val = reinterpret_cast<SQLULEN*>(GetParamBindOffsetPtr());

                    break;
                }

                case SQL_ATTR_ROW_BIND_OFFSET_PTR:
                {
                    SQLULEN** val = reinterpret_cast<SQLULEN**>(buf);

                    *val = reinterpret_cast<SQLULEN*>(GetColumnBindOffsetPtr());

                    break;
                }

                default:
                {
                    AddStatusRecord(SqlState::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                        "Specified attribute is not supported.");

                    return SqlResult::AI_ERROR;
                }
            }

            return SqlResult::AI_SUCCESS;
        }

        uint16_t Statement::GetParametersNumber()
        {
            IGNITE_ODBC_API_CALL_ALWAYS_SUCCESS;

            return static_cast<uint16_t>(paramBindings.size());
        }

        void Statement::SetParamBindOffsetPtr(int* ptr)
        {
            IGNITE_ODBC_API_CALL_ALWAYS_SUCCESS;

            paramBindOffset = ptr;
        }

        int* Statement::GetParamBindOffsetPtr()
        {
            return paramBindOffset;
        }

        void Statement::GetColumnData(uint16_t columnIdx, app::ApplicationDataBuffer& buffer)
        {
            IGNITE_ODBC_API_CALL(InternalGetColumnData(columnIdx, buffer));
        }

        SqlResult::Type Statement::InternalGetColumnData(uint16_t columnIdx,
            app::ApplicationDataBuffer& buffer)
        {
            if (!currentQuery.get())
            {
                AddStatusRecord(SqlState::S24000_INVALID_CURSOR_STATE,
                    "Cursor is not in the open state.");

                return SqlResult::AI_ERROR;
            }

            SqlResult::Type res = currentQuery->GetColumn(columnIdx, buffer);

            return res;
        }

        void Statement::PrepareSqlQuery(const std::string& query)
        {
            IGNITE_ODBC_API_CALL(InternalPrepareSqlQuery(query));
        }

        SqlResult::Type Statement::InternalPrepareSqlQuery(const std::string& query)
        {
            if (currentQuery.get())
                currentQuery->Close();

            currentQuery.reset(new query::DataQuery(*this, connection, query, paramBindings));

            // Resetting parameters types as we are changing the query.
            paramTypes.clear();

            return SqlResult::AI_SUCCESS;
        }

        void Statement::ExecuteSqlQuery(const std::string& query)
        {
            IGNITE_ODBC_API_CALL(InternalExecuteSqlQuery(query));
        }

        SqlResult::Type Statement::InternalExecuteSqlQuery(const std::string& query)
        {
            SqlResult::Type result = InternalPrepareSqlQuery(query);

            if (result != SqlResult::AI_SUCCESS)
                return result;

            return InternalExecuteSqlQuery();
        }

        void Statement::ExecuteSqlQuery()
        {
            IGNITE_ODBC_API_CALL(InternalExecuteSqlQuery());
        }

        SqlResult::Type Statement::InternalExecuteSqlQuery()
        {
            if (!currentQuery.get())
            {
                AddStatusRecord(SqlState::SHY010_SEQUENCE_ERROR, "Query is not prepared.");

                return SqlResult::AI_ERROR;
            }

            bool paramDataReady = true;

            app::ParameterBindingMap::iterator it;
            for (it = paramBindings.begin(); it != paramBindings.end(); ++it)
            {
                app::Parameter& param = it->second;

                param.ResetStoredData();

                paramDataReady &= param.IsDataReady();
            }

            if (!paramDataReady)
                return SqlResult::AI_NEED_DATA;

            return currentQuery->Execute();
        }

        void Statement::ExecuteGetColumnsMetaQuery(const std::string& schema,
            const std::string& table, const std::string& column)
        {
            IGNITE_ODBC_API_CALL(InternalExecuteGetColumnsMetaQuery(schema, table, column));
        }

        SqlResult::Type Statement::InternalExecuteGetColumnsMetaQuery(const std::string& schema,
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

        SqlResult::Type Statement::InternalExecuteGetTablesMetaQuery(const std::string& catalog,
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

        SqlResult::Type Statement::InternalExecuteGetForeignKeysQuery(const std::string& primaryCatalog,
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

        SqlResult::Type Statement::InternalExecuteGetPrimaryKeysQuery(const std::string& catalog,
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

        SqlResult::Type Statement::InternalExecuteSpecialColumnsQuery(int16_t type,
            const std::string& catalog, const std::string& schema,
            const std::string& table, int16_t scope, int16_t nullable)
        {
            if (type != SQL_BEST_ROWID && type != SQL_ROWVER)
            {
                AddStatusRecord(SqlState::SHY097_COLUMN_TYPE_OUT_OF_RANGE,
                    "An invalid IdentifierType value was specified.");

                return SqlResult::AI_ERROR;
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

        SqlResult::Type Statement::InternalExecuteGetTypeInfoQuery(int16_t sqlType)
        {
            if (!type_traits::IsSqlTypeSupported(sqlType))
            {
                AddStatusRecord(SqlState::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                    "Data type is not supported.");

                return SqlResult::AI_ERROR;
            }

            if (currentQuery.get())
                currentQuery->Close();

            currentQuery.reset(new query::TypeInfoQuery(*this, sqlType));

            return currentQuery->Execute();
        }

        void Statement::FreeResources(int16_t option)
        {
            IGNITE_ODBC_API_CALL(InternalFreeResources(option));
        }

        SqlResult::Type Statement::InternalFreeResources(int16_t option)
        {
            switch (option)
            {
                case SQL_DROP:
                {
                    AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Deprecated, call SQLFreeHandle instead");

                    return SqlResult::AI_ERROR;
                }

                case SQL_CLOSE:
                {
                    return InternalClose();
                }

                case SQL_UNBIND:
                {
                    SafeUnbindAllColumns();

                    break;
                }

                case SQL_RESET_PARAMS:
                {
                    SafeUnbindAllParameters();

                    break;
                }

                default:
                {
                    AddStatusRecord(SqlState::SHY092_OPTION_TYPE_OUT_OF_RANGE, "The value specified for the argument Option was invalid");
                    return SqlResult::AI_ERROR;
                }
            }
            return SqlResult::AI_SUCCESS;
        }

        void Statement::Close()
        {
            IGNITE_ODBC_API_CALL(InternalClose());
        }

        SqlResult::Type Statement::InternalClose()
        {
            if (!currentQuery.get())
                return SqlResult::AI_SUCCESS;

            SqlResult::Type result = currentQuery->Close();

            if (result == SqlResult::AI_SUCCESS)
                currentQuery.reset();

            return result;
        }

        void Statement::FetchScroll(int16_t orientation, int64_t offset)
        {
            IGNITE_ODBC_API_CALL(InternalFetchScroll(orientation, offset));
        }

        SqlResult::Type Statement::InternalFetchScroll(int16_t orientation, int64_t offset)
        {
            UNREFERENCED_PARAMETER(offset);

            if (orientation != SQL_FETCH_NEXT)
            {
                AddStatusRecord(SqlState::SHY106_FETCH_TYPE_OUT_OF_RANGE, "The value specified for the argument FetchOrientation was not SQL_FETCH_NEXT.");
                return SqlResult::AI_ERROR;
            }

            return InternalFetchRow();
        }

        void Statement::FetchRow()
        {
            IGNITE_ODBC_API_CALL(InternalFetchRow());
        }

        SqlResult::Type Statement::InternalFetchRow()
        {
            if (rowsFetched)
                *rowsFetched = 0;

            if (!currentQuery.get())
            {
                AddStatusRecord(SqlState::S24000_INVALID_CURSOR_STATE,
                    "Cursor is not in the open state.");

                return SqlResult::AI_ERROR;
            }

            SqlResult::Type res = currentQuery->FetchNextRow(columnBindings);

            if (res == SqlResult::AI_SUCCESS)
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

        SqlResult::Type Statement::InternalNextResults()
        {
            if (!currentQuery.get())
                return SqlResult::AI_NO_DATA;

            SqlResult::Type result = currentQuery->Close();

            return result == SqlResult::AI_SUCCESS ? SqlResult::AI_NO_DATA : result;
        }

        void Statement::GetColumnAttribute(uint16_t colIdx, uint16_t attrId,
            char* strbuf, int16_t buflen, int16_t* reslen, SqlLen* numbuf)
        {
            IGNITE_ODBC_API_CALL(InternalGetColumnAttribute(colIdx, attrId,
                strbuf, buflen, reslen, numbuf));
        }

        SqlResult::Type Statement::InternalGetColumnAttribute(uint16_t colIdx,
            uint16_t attrId, char* strbuf, int16_t buflen, int16_t* reslen,
            SqlLen* numbuf)
        {
            const meta::ColumnMetaVector *meta = GetMeta();

            if (!meta)
            {
                AddStatusRecord(SqlState::SHY010_SEQUENCE_ERROR, "Query is not executed.");

                return SqlResult::AI_ERROR;
            }

            if (colIdx > meta->size() + 1 || colIdx < 1)
            {
                AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                    "Column index is out of range.", 0, colIdx);

                return SqlResult::AI_ERROR;
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
                AddStatusRecord(SqlState::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                    "Unknown attribute.");

                return SqlResult::AI_ERROR;
            }

            return SqlResult::AI_SUCCESS;
        }

        int64_t Statement::AffectedRows()
        {
            int64_t rowCnt = 0;

            IGNITE_ODBC_API_CALL(InternalAffectedRows(rowCnt));

            return rowCnt;
        }

        SqlResult::Type Statement::InternalAffectedRows(int64_t& rowCnt)
        {
            if (!currentQuery.get())
            {
                AddStatusRecord(SqlState::SHY010_SEQUENCE_ERROR, "Query is not executed.");

                return SqlResult::AI_ERROR;
            }

            rowCnt = currentQuery->AffectedRows();

            return SqlResult::AI_SUCCESS;
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

        SqlResult::Type Statement::InternalSelectParam(void** paramPtr)
        {
            if (!paramPtr)
            {
                AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                    "Invalid parameter: ValuePtrPtr is null.");

                return SqlResult::AI_ERROR;
            }

            if (!currentQuery.get())
            {
                AddStatusRecord(SqlState::SHY010_SEQUENCE_ERROR, "Query is not prepared.");

                return SqlResult::AI_ERROR;
            }

            app::ParameterBindingMap::iterator it;

            if (currentParamIdx)
            {
                it = paramBindings.find(currentParamIdx);

                if (it != paramBindings.end() && !it->second.IsDataReady())
                {
                    AddStatusRecord(SqlState::S22026_DATA_LENGTH_MISMATCH,
                        "Less data was sent for a parameter than was specified with "
                        "the StrLen_or_IndPtr argument in SQLBindParameter.");

                    return SqlResult::AI_ERROR;
                }
            }

            for (it = paramBindings.begin(); it != paramBindings.end(); ++it)
            {
                uint16_t paramIdx = it->first;
                app::Parameter& param = it->second;

                if (!param.IsDataReady())
                {
                    *paramPtr = param.GetBuffer().GetData();

                    currentParamIdx = paramIdx;

                    return SqlResult::AI_NEED_DATA;
                }
            }

            SqlResult::Type res = currentQuery->Execute();

            if (res != SqlResult::AI_SUCCESS)
                res = SqlResult::AI_SUCCESS_WITH_INFO;

            return res;
        }

        void Statement::PutData(void* data, SqlLen len)
        {
            IGNITE_ODBC_API_CALL(InternalPutData(data, len));
        }

        SqlResult::Type Statement::InternalPutData(void* data, SqlLen len)
        {
            if (!data && len != 0 && len != SQL_DEFAULT_PARAM && len != SQL_NULL_DATA)
            {
                AddStatusRecord(SqlState::SHY009_INVALID_USE_OF_NULL_POINTER,
                    "Invalid parameter: DataPtr is null StrLen_or_Ind is not 0, "
                    "SQL_DEFAULT_PARAM, or SQL_NULL_DATA.");

                return SqlResult::AI_ERROR;
            }

            if (currentParamIdx == 0)
            {
                AddStatusRecord(SqlState::SHY010_SEQUENCE_ERROR,
                    "Parameter is not selected with the SQLParamData.");

                return SqlResult::AI_ERROR;
            }

            app::ParameterBindingMap::iterator it = paramBindings.find(currentParamIdx);

            if (it == paramBindings.end())
            {
                AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                    "Selected parameter has been unbound.");

                return SqlResult::AI_ERROR;
            }

            app::Parameter& param = it->second;

            param.PutData(data, len);

            return SqlResult::AI_SUCCESS;
        }

        void Statement::DescribeParam(int16_t paramNum, int16_t* dataType,
            SqlUlen* paramSize, int16_t* decimalDigits, int16_t* nullable)
        {
            IGNITE_ODBC_API_CALL(InternalDescribeParam(paramNum,
                dataType, paramSize, decimalDigits, nullable));
        }

        SqlResult::Type Statement::InternalDescribeParam(int16_t paramNum, int16_t* dataType,
            SqlUlen* paramSize, int16_t* decimalDigits, int16_t* nullable)
        {
            query::Query *qry = currentQuery.get();
            if (!qry)
            {
                AddStatusRecord(SqlState::SHY010_SEQUENCE_ERROR, "Query is not prepared.");

                return SqlResult::AI_ERROR;
            }

            if (qry->GetType() != query::QueryType::DATA)
            {
                AddStatusRecord(SqlState::SHY010_SEQUENCE_ERROR, "Query is not SQL data query.");

                return SqlResult::AI_ERROR;
            }

            int8_t type = 0;

            if (paramNum > 0 && static_cast<size_t>(paramNum) <= paramTypes.size())
                type = paramTypes[paramNum - 1];

            LOG_MSG("Type: " << type);

            if (!type)
            {
                SqlResult::Type res = UpdateParamsMeta();

                if (res != SqlResult::AI_SUCCESS)
                    return res;

                if (paramNum < 1 || static_cast<size_t>(paramNum) > paramTypes.size())
                    type = impl::binary::IGNITE_HDR_NULL;
                else
                    type = paramTypes[paramNum - 1];
            }

            if (dataType)
                *dataType = type_traits::BinaryToSqlType(type);

            if (paramSize)
                *paramSize = type_traits::BinaryTypeColumnSize(type);

            if (decimalDigits)
                *decimalDigits = type_traits::BinaryTypeDecimalDigits(type);

            if (nullable)
                *nullable = type_traits::BinaryTypeNullability(type);

            return SqlResult::AI_SUCCESS;
        }

        SqlResult::Type Statement::UpdateParamsMeta()
        {
            query::Query *qry0 = currentQuery.get();

            assert(qry0 != 0);
            assert(qry0->GetType() == query::QueryType::DATA);

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
                AddStatusRecord(SqlState::SHYT01_CONNECTIOIN_TIMEOUT, err.GetText());

                return SqlResult::AI_ERROR;
            }

            if (rsp.GetStatus() != ResponseStatus::SUCCESS)
            {
                LOG_MSG("Error: " << rsp.GetError());

                AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, rsp.GetError());

                return SqlResult::AI_ERROR;
            }

            paramTypes = rsp.GetTypeIds();

            for (size_t i = 0; i < paramTypes.size(); ++i)
            {
                LOG_MSG("[" << i << "] Parameter type: " << paramTypes[i]);
            }

            return SqlResult::AI_SUCCESS;
        }
    }
}



































