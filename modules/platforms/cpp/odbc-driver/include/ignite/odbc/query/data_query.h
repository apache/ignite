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

#ifndef _IGNITE_ODBC_DRIVER_DATA_QUERY
#define _IGNITE_ODBC_DRIVER_DATA_QUERY

#include "ignite/odbc/query/query.h"
#include "ignite/odbc/cursor.h"

namespace ignite
{
    namespace odbc
    {
        /** Connection forward-declaration. */
        class Connection;

        namespace query
        {
            /**
             * Query.
             */
            class DataQuery : public Query
            {
            public:
                /**
                 * Constructor.
                 */
                DataQuery(Connection& connection, const std::string& sql);

                /**
                 * Destructor.
                 */
                virtual ~DataQuery();

                /**
                 * Execute query.
                 *
                 * @return True on success.
                 */
                virtual bool Execute();

                /**
                 * Get column metadata.
                 *
                 * @return Column metadata.
                 */
                virtual const ColumnMetaVector& GetMeta() const;

                /**
                 * Fetch next result row to application buffers.
                 *
                 * @return Operation result.
                 */
                virtual SqlResult FetchNextRow(ColumnBindingMap& columnBindings);

                /**
                 * Close query.
                 *
                 * @return True on success.
                 */
                virtual bool Close();

            private:
                /**
                 * Make query execute request and use response to set internal
                 * state.
                 *
                 * @return True on success.
                 */
                bool MakeRequestExecute();

                /**
                 * Make query close request.
                 *
                 * @return True on success.
                 */
                bool MakeRequestClose();

                /**
                 * Make data fetch request and use response to set internal state.
                 *
                 * @return True on success.
                 */
                bool MakeRequestFetch();

                /** Connection associated with the statement. */
                Connection& connection;

                /** SQL Query. */
                std::string sql;

                /** Columns metadata. */
                ColumnMetaVector resultMeta;

                /** Cursor. */
                std::auto_ptr<Cursor> cursor;
            };
        }
    }
}

#endif