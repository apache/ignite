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

#include "application_data_buffer.h"
#include "common_types.h"

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
            void BindResultColumn(uint16_t columnIdx, const ApplicationDataBuffer& buffer);

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
             * Fetch query result row.
             *
             * @return True on success.
             */
            SqlResult FetchRow();

        private:
            /**
             * Constructor.
             * Called by friend classes.
             * @param parent Connection associated with the statement.
             */
            Statement(Connection& parent);

            /** Column binging map type alias. */
            typedef std::map<uint16_t, ApplicationDataBuffer> ColumnBindingMap;

            /** Connection associated with the statement. */
            Connection& connection;

            /** Column bindings. */
            ColumnBindingMap columnBindings;

            //TODO: Move to separate Cursor class.
            /** Cursor id. */
            int64_t resultQueryId;
        };
    }
}

#endif