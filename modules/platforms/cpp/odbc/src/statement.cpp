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

namespace ignite
{
    namespace odbc
    {
        Statement::Statement(Connection& parent) :
            connection(parent), columnBindings(), currentQuery(),
            rowsFetched(0), rowStatuses(0), paramBindOffset(0), columnBindOffset(0)
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

            columnBindings[columnIdx].SetPtrToOffsetPtr(&columnBindOffset);
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

        void Statement::SetColumnBindOffsetPtr(size_t * ptr)
        {
            columnBindOffset = ptr;
        }

        size_t * Statement::GetColumnBindOffsetPtr()
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

        void Statement::BindParameter(uint16_t paramIdx, const app::Parameter& param)
        {
            IGNITE_ODBC_API_CALL_ALWAYS_SUCCESS;

            paramBindings[paramIdx] = param;

            paramBindings[paramIdx].GetBuffer().SetPtrToOffsetPtr(&paramBindOffset);
        }

        void Statement::UnbindParameter(uint16_t paramIdx)
        {
            IGNITE_ODBC_API_CALL_ALWAYS_SUCCESS;

            paramBindings.erase(paramIdx);
        }

        void Statement::UnbindAllParameters()
        {
            IGNITE_ODBC_API_CALL_ALWAYS_SUCCESS;

            paramBindings.clear();
        }

        uint16_t Statement::GetParametersNumber()
        {
            IGNITE_ODBC_API_CALL_ALWAYS_SUCCESS;

            return static_cast<uint16_t>(paramBindings.size());
        }

        void Statement::SetParamBindOffsetPtr(size_t* ptr)
        {
            IGNITE_ODBC_API_CALL_ALWAYS_SUCCESS;

            paramBindOffset = ptr;
        }

        size_t * Statement::GetParamBindOffsetPtr()
        {
            return paramBindOffset;
        }

        void Statement::GetColumnData(uint16_t columnIdx, app::ApplicationDataBuffer& buffer)
        {
            IGNITE_ODBC_API_CALL(InternalGetColumnData(columnIdx, buffer));
        }

        SqlResult Statement::InternalGetColumnData(uint16_t columnIdx, app::ApplicationDataBuffer& buffer)
        {
            if (!currentQuery.get())
            {
                AddStatusRecord(SQL_STATE_24000_INVALID_CURSOR_STATE, "Cursor is not in the open state.");

                return SQL_RESULT_ERROR;
            }

            SqlResult res = currentQuery->GetColumn(columnIdx, buffer);

            return res;
        }

        void Statement::PrepareSqlQuery(const std::string& query)
        {
            return PrepareSqlQuery(query.data(), query.size());
        }

        void Statement::PrepareSqlQuery(const char* query, size_t len)
        {
            IGNITE_ODBC_API_CALL(InternalPrepareSqlQuery(query, len));
        }

        SqlResult Statement::InternalPrepareSqlQuery(const char* query, size_t len)
        {
            if (currentQuery.get())
                currentQuery->Close();

            std::string sql(query, len);

            currentQuery.reset(new query::DataQuery(*this, connection, sql, paramBindings));

            return SQL_RESULT_SUCCESS;
        }

        void Statement::ExecuteSqlQuery(const std::string& query)
        {
            ExecuteSqlQuery(query.data(), query.size());
        }

        void Statement::ExecuteSqlQuery(const char* query, size_t len)
        {
            IGNITE_ODBC_API_CALL(InternalExecuteSqlQuery(query, len));
        }

        SqlResult Statement::InternalExecuteSqlQuery(const char* query, size_t len)
        {
            SqlResult result = InternalPrepareSqlQuery(query, len);

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

            currentQuery.reset(new query::ColumnMetadataQuery(*this, connection, cache, table, column));

            return currentQuery->Execute();
        }

        void Statement::ExecuteGetTablesMetaQuery(const std::string& catalog,
            const std::string& schema, const std::string& table, const std::string& tableType)
        {
            IGNITE_ODBC_API_CALL(InternalExecuteGetTablesMetaQuery(catalog, schema, table, tableType));
        }

        SqlResult Statement::InternalExecuteGetTablesMetaQuery(const std::string& catalog,
            const std::string& schema, const std::string& table, const std::string& tableType)
        {
            if (currentQuery.get())
                currentQuery->Close();

            currentQuery.reset(new query::TableMetadataQuery(*this, connection, catalog, schema, table, tableType));

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

            currentQuery.reset(new query::ForeignKeysQuery(*this, connection, primaryCatalog, primarySchema,
                primaryTable, foreignCatalog, foreignSchema, foreignTable));

            return currentQuery->Execute();
        }

        void Statement::ExecuteGetPrimaryKeysQuery(const std::string& catalog, const std::string& schema,
            const std::string& table)
        {
            IGNITE_ODBC_API_CALL(InternalExecuteGetPrimaryKeysQuery(catalog, schema, table));
        }

        SqlResult Statement::InternalExecuteGetPrimaryKeysQuery(const std::string& catalog, const std::string& schema,
            const std::string& table)
        {
            if (currentQuery.get())
                currentQuery->Close();

            currentQuery.reset(new query::PrimaryKeysQuery(*this, connection, catalog, schema, table));

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
                AddStatusRecord(SQL_STATE_HYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, "Data type is not supported.");

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
            {
                AddStatusRecord(SQL_STATE_24000_INVALID_CURSOR_STATE, "Cursor is not in the open state.");

                return SQL_RESULT_ERROR;
            }

            SqlResult result = currentQuery->Close();

            if (result == SQL_RESULT_SUCCESS)
                currentQuery.reset();

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
                AddStatusRecord(SQL_STATE_24000_INVALID_CURSOR_STATE, "Cursor is not in the open state.");

                return SQL_RESULT_ERROR;
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
                AddStatusRecord(SQL_STATE_HY000_GENERAL_ERROR, "Column index is out of range.", 0, colIdx);

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

                if (found && strbuf)
                    *reslen = static_cast<int16_t>(outSize);
            }

            if (!found)
            {
                AddStatusRecord(SQL_STATE_HYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, "Unknown attribute.");

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
    }
}

