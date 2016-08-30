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

#ifndef _IGNITE_ODBC_META_TABLE_META
#define _IGNITE_ODBC_META_TABLE_META

#include <stdint.h>
#include <string>

#include "ignite/impl/binary/binary_reader_impl.h"

#include "ignite/odbc/utility.h"

namespace ignite
{
    namespace odbc
    {
        namespace meta
        {
            /**
             * Table metadata.
             */
            class TableMeta
            {
            public:
                /**
                 * Default constructor.
                 */
                TableMeta()
                {
                    // No-op.
                }
            
                /**
                 * Constructor.
                 *
                 * @param catalogName Catalog name.
                 * @param schemaName Schema name.
                 * @param tableName Table name.
                 * @param tableType Table type.
                 */
                TableMeta(const std::string& catalogName, const std::string& schemaName,
                          const std::string& tableName, const std::string& tableType) :
                    catalogName(catalogName), schemaName(schemaName), tableName(tableName),
                    tableType(tableType)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                ~TableMeta()
                {
                    // No-op.
                }

                /**
                 * Copy constructor.
                 */
                TableMeta(const TableMeta& other) :
                    catalogName(other.catalogName),
                    schemaName(other.schemaName),
                    tableName(other.tableName),
                    tableType(other.tableType)
                {
                    // No-op.
                }

                /**
                 * Copy operator.
                 */
                TableMeta& operator=(const TableMeta& other)
                {
                    catalogName = other.catalogName;
                    schemaName = other.schemaName;
                    tableName = other.tableName;
                    tableType = other.tableType;

                    return *this;
                }

                /**
                 * Read using reader.
                 * @param reader Reader.
                 */
                void Read(ignite::impl::binary::BinaryReaderImpl& reader);

                /**
                 * Get catalog name.
                 * @return Catalog name.
                 */
                const std::string& GetCatalogName() const
                {
                    return catalogName;
                }

                /**
                 * Get schema name.
                 * @return Schema name.
                 */
                const std::string& GetSchemaName() const
                {
                    return schemaName;
                }

                /**
                 * Get table name.
                 * @return Table name.
                 */
                const std::string& GetTableName() const
                {
                    return tableName;
                }

                /**
                 * Get table type.
                 * @return Table type.
                 */
                const std::string& GetTableType() const
                {
                    return tableType;
                }

            private:
                /** Catalog name. */
                std::string catalogName;

                /** Schema name. */
                std::string schemaName;

                /** Table name. */
                std::string tableName;

                /** Table type. */
                std::string tableType;
            };

            /** Table metadata vector alias. */
            typedef std::vector<TableMeta> TableMetaVector;

            /**
             * Read tables metadata collection.
             * @param reader Reader.
             * @param meta Collection.
             */
            void ReadTableMetaVector(ignite::impl::binary::BinaryReaderImpl& reader, TableMetaVector& meta);
        }
    }
}

#endif //_IGNITE_ODBC_META_TABLE_META