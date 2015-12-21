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

 // Undefining windows macro to use standard library tool
#   undef min
#endif //_WIN32

#include <sqlext.h>
#include <odbcinst.h>

#include "ignite/odbc/query/data_query.h"
#include "ignite/odbc/query/column_metadata_query.h"
#include "ignite/odbc/query/table_metadata_query.h"
#include "ignite/odbc/query/foreign_keys_query.h"
#include "ignite/odbc/query/primary_keys_query.h"
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
            rowsFetched(0), rowStatuses(0)
        {
            // No-op.
        }

        Statement::~Statement()
        {
            // No-op.
        }

        void Statement::BindColumn(uint16_t columnIdx, const app::ApplicationDataBuffer& buffer)
        {
            columnBindings[columnIdx] = buffer;
        }

        void Statement::UnbindColumn(uint16_t columnIdx)
        {
            columnBindings.erase(columnIdx);
        }

        void Statement::UnbindAllColumns()
        {
            columnBindings.clear();
        }

        void Statement::BindParameter(uint16_t paramIdx, const app::Parameter& param)
        {
            paramBindings[paramIdx] = param;
        }

        void Statement::UnbindParameter(uint16_t paramIdx)
        {
            paramBindings.erase(paramIdx);
        }

        void Statement::UnbindAllParameters()
        {
            paramBindings.clear();
        }

        uint16_t Statement::GetParametersNumber() const
        {
            return static_cast<uint16_t>(paramBindings.size());
        }

        bool Statement::PrepareSqlQuery(const char* query, size_t len)
        {
            if (currentQuery.get())
                currentQuery->Close();

            std::string sql(query, len);

            currentQuery.reset(new query::DataQuery(connection, sql, paramBindings));

            return false;
        }

        bool Statement::ExecuteSqlQuery(const char* query, size_t len)
        {
            PrepareSqlQuery(query, len);

            return ExecuteSqlQuery();
        }

        bool Statement::ExecuteSqlQuery()
        {
            if (!currentQuery.get())
                return false;

            return currentQuery->Execute();
        }

        bool Statement::ExecuteGetColumnsMetaQuery(const std::string& schema, 
            const std::string& table, const std::string& column)
        {
            if (currentQuery.get())
                currentQuery->Close();

            std::string cache(schema);

            if (cache.empty())
                cache = connection.GetCache();

            currentQuery.reset(new query::ColumnMetadataQuery(connection, cache, table, column));

            return currentQuery->Execute();
        }

        bool Statement::ExecuteGetTablesMetaQuery(const std::string& catalog, 
            const std::string& schema, const std::string& table, const std::string& tableType)
        {
            if (currentQuery.get())
                currentQuery->Close();

            std::string cache(schema);

            if (cache.empty())
                cache = connection.GetCache();

            currentQuery.reset(new query::TableMetadataQuery(connection, catalog, cache, table, tableType));

            return currentQuery->Execute();
        }

        bool Statement::ExecuteGetForeignKeysQuery(const std::string& primaryCatalog,
            const std::string& primarySchema, const std::string& primaryTable,
            const std::string& foreignCatalog, const std::string& foreignSchema,
            const std::string& foreignTable)
        {
            if (currentQuery.get())
                currentQuery->Close();

            currentQuery.reset(new query::ForeignKeysQuery(connection, primaryCatalog, primarySchema, 
                primaryTable, foreignCatalog, foreignSchema, foreignTable));

            return currentQuery->Execute();
        }

        bool Statement::ExecuteGetPrimaryKeysQuery(const std::string& catalog, const std::string& schema,
            const std::string& table)
        {
            if (currentQuery.get())
                currentQuery->Close();

            currentQuery.reset(new query::PrimaryKeysQuery(connection, catalog, schema, table));

            return currentQuery->Execute();
        }

        bool Statement::Close()
        {
            if (!currentQuery.get())
                return false;

            currentQuery->Close();

            currentQuery.reset();

            return true;
        }

        SqlResult Statement::FetchRow()
        {
            if (rowsFetched)
                *rowsFetched = 0;

            if (!currentQuery.get())
                return SQL_RESULT_ERROR;

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

        bool Statement::GetColumnAttribute(uint16_t colIdx, uint16_t attrId, char* strbuf,
            int16_t buflen, int16_t* reslen, int64_t* numbuf)
        {
            const meta::ColumnMetaVector *meta = GetMeta();

            if (!meta)
                return false;

            if (colIdx > meta->size() + 1 || colIdx < 1)
                return false;

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

            return found;
        }

        bool Statement::AffectedRows(int64_t& rowCnt) const
        {
            if (!currentQuery.get())
                return false;

            rowCnt = currentQuery->AffectedRows();

            return true;
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

