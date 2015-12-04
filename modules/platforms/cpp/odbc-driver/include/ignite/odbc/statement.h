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

#ifndef _IGNITE_ODBC_DRIVER_STATEMENT
#define _IGNITE_ODBC_DRIVER_STATEMENT

#include <stdint.h>

#include <map>
#include <memory>

#include <ignite/impl/interop/interop_output_stream.h>
#include <ignite/impl/interop/interop_input_stream.h>
#include <ignite/impl/binary/binary_writer_impl.h>

#include "ignite/odbc/query/query.h"
#include "ignite/odbc/application_data_buffer.h"
#include "ignite/odbc/parser.h"
#include "ignite/odbc/common_types.h"
#include "ignite/odbc/cursor.h"
#include "ignite/odbc/column_meta.h"
#include "ignite/odbc/utility.h"

namespace ignite
{
    namespace odbc
    {
        class Connection;

        /**
         * SQL-statement abstraction. Holds SQL query user buffers data and
         * call result.
         */
        class Statement
        {
            friend class Connection;
        public:
            /**
             * Destructor.
             */
            ~Statement();

            /**
             * Bind result column to specified data buffer.
             *
             * @param columnIdx Column index.
             * @param buffer Buffer to put column data to.
             */
            void BindColumn(uint16_t columnIdx, const ApplicationDataBuffer& buffer);

            /**
             * Unbind specified column buffer.
             *
             * @param columnIdx Column index.
             */
            void UnbindColumn(uint16_t columnIdx);

            /**
             * Unbind all column buffers.
             */
            void UnbindAllColumns();

            /**
             * Prepare SQL query.
             *
             * @note Only SELECT queries are supported currently.
             * @param query SQL query.
             * @param len Query length.
             * @return True on success.
             */
            void PrepareSqlQuery(const char* query, size_t len);

            /**
             * Execute SQL query.
             *
             * @note Only SELECT queries are supported currently.
             * @param query SQL query.
             * @param len Query length.
             * @return True on success.
             */
            bool ExecuteSqlQuery(const char* query, size_t len);

            /**
             * Execute SQL query.
             *
             * @note Only SELECT queries are supported currently.
             * @return True on success.
             */
            bool ExecuteSqlQuery();

            /**
             * Get columns metadata.
             *
             * @param cache Cache name.
             * @param table Table name.
             * @param column Column name.
             * @return True on success.
             */
            bool ExecuteGetColumnsMetaQuery(const std::string& cache, const std::string& table, const std::string& column);

            /**
             * Close statement.
             *
             * @return True on success.
             */
            bool Close();

            /**
             * Fetch query result row.
             *
             * @return Operation result.
             */
            SqlResult FetchRow();

            /**
             * Get column metadata.
             *
             * @return Column metadata.
             */
            const ColumnMetaVector* GetMeta() const;

            /**
             * Check if data is available.
             *
             * @return True if data is available.
             */
            bool DataAvailable() const;

        private:
            /**
             * Constructor.
             * Called by friend classes.
             *
             * @param parent Connection associated with the statement.
             */
            Statement(Connection& parent);

            /** Connection associated with the statement. */
            Connection& connection;

            /** Column bindings. */
            ColumnBindingMap columnBindings;

            /** Underlying query. */
            std::auto_ptr<query::Query> currentQuery;
        };
    }
}

#endif