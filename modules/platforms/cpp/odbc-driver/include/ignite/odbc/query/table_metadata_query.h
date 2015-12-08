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

#ifndef _IGNITE_ODBC_DRIVER_TABLE_METADATA_QUERY
#define _IGNITE_ODBC_DRIVER_TABLE_METADATA_QUERY

#include "ignite/odbc/query/query.h"
#include "ignite/odbc/meta/table_meta.h"

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
            class TableMetadataQuery : public Query
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param connection Associated connection.
                 * @param catalog Catalog search pattern.
                 * @param schema Schema search pattern.
                 * @param table Table search pattern.
                 * @param tableType Table type search pattern.
                 */
                TableMetadataQuery(Connection& connection, const std::string& catalog,
                    const std::string& schema, const std::string& table, const std::string& tableType);

                /**
                 * Destructor.
                 */
                virtual ~TableMetadataQuery();

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
                virtual const meta::ColumnMetaVector& GetMeta() const;

                /**
                 * Fetch next result row to application buffers.
                 *
                 * @return Operation result.
                 */
                virtual SqlResult FetchNextRow(app::ColumnBindingMap& columnBindings);

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
                bool MakeRequestGetTablesMeta();

                /**
                 * Check if it is special semantic case for the SQL_ALL_CATALOGS.
                 *
                 * @return True if it is special semantic case for the SQL_ALL_CATALOGS.
                 */
                bool SpecialSemanticsAllCatalogs();

                /**
                 * Check if it is special semantic case for the SQL_ALL_SCHEMAS.
                 *
                 * @return True if it is special semantic case for the SQL_ALL_SCHEMAS.
                 */
                bool SpecialSemanticsAllSchemas();

                /**
                 * Check if it is special semantic case for the SQL_ALL_TABLE_TYPES .
                 *
                 * @return True if it is special semantic case for the SQL_ALL_TABLE_TYPES .
                 */
                bool SpecialSemanticsAllTableTypes();

                /** Connection associated with the statement. */
                Connection& connection;

                /** Catalog search pattern. */
                std::string catalog;

                /** Schema search pattern. */
                std::string schema;

                /** Table search pattern. */
                std::string table;

                /** Table type search pattern. */
                std::string tableType;

                /** Query executed. */
                bool executed;

                /** Fetched metadata. */
                meta::TableMetaVector meta;

                /** Metadata cursor. */
                meta::TableMetaVector::iterator cursor;

                /** Columns metadata. */
                meta::ColumnMetaVector columnsMeta;
            };
        }
    }
}

#endif