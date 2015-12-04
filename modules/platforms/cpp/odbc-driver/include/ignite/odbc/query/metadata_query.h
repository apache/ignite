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

#ifndef _IGNITE_ODBC_DRIVER_METADATA_QUERY
#define _IGNITE_ODBC_DRIVER_METADATA_QUERY

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
            class MetadataQuery : public Query
            {
            public:
                /**
                 * Constructor.
                 */
                MetadataQuery(Connection& connection, const std::string& cache, 
                    const std::string& table, const std::string& column);

                /**
                 * Destructor.
                 */
                virtual ~MetadataQuery();

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
                
                /**
                 * Check if data is available.
                 *
                 * @return True if data is available.
                 */
                virtual bool DataAvailable() const;

            private:
                /**
                 * Make get columns metadata requets and use response to set internal state.
                 *
                 * @return True on success.
                 */
                bool MakeRequestGetColumnsMeta();

                /** Connection associated with the statement. */
                Connection& connection;

                /** Cache search pattern. */
                std::string cache;

                /** Table search pattern. */
                std::string table;

                /** Column search pattern. */
                std::string column;

                /** Query executed. */
                bool executed;

                /** Fetched metadata. */
                ColumnMetaVector meta;

                /** Metadata cursor. */
                ColumnMetaVector::iterator cursor;

                /** Columns metadata. */
                ColumnMetaVector columnsMeta;
            };
        }
    }
}

#endif