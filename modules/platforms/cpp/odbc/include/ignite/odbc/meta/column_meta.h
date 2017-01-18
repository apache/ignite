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

#ifndef _IGNITE_ODBC_META_COLUMN_META
#define _IGNITE_ODBC_META_COLUMN_META

#include <stdint.h>
#include <string>

#include "ignite/impl/binary/binary_reader_impl.h"

#include "ignite/odbc/common_types.h"
#include "ignite/odbc/utility.h"

namespace ignite
{
    namespace odbc
    {
        namespace meta
        {
            /**
             * Column metadata.
             */
            class ColumnMeta
            {
            public:
                /**
                 * Convert attribute ID to string containing its name.
                 * Debug function.
                 * @param type Attribute ID.
                 * @return Null-terminated string containing attribute name.
                 */
                static const char* AttrIdToString(uint16_t id);

                /**
                 * Default constructor.
                 */
                ColumnMeta()
                {
                    // No-op.
                }

                /**
                 * Constructor.
                 *
                 * @param schemaName Schema name.
                 * @param tableName Table name.
                 * @param columnName Column name.
                 * @param typeName Type name.
                 * @param dataType Data type.
                 */
                ColumnMeta(const std::string& schemaName, const std::string& tableName,
                           const std::string& columnName, int8_t dataType) :
                    schemaName(schemaName), tableName(tableName), columnName(columnName), dataType(dataType)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                ~ColumnMeta()
                {
                    // No-op.
                }

                /**
                 * Copy constructor.
                 */
                ColumnMeta(const ColumnMeta& other) :
                    schemaName(other.schemaName),
                    tableName(other.tableName),
                    columnName(other.columnName),
                    dataType(other.dataType)
                {
                    // No-op.
                }

                /**
                 * Copy operator.
                 */
                ColumnMeta& operator=(const ColumnMeta& other)
                {
                    schemaName = other.schemaName;
                    tableName = other.tableName;
                    columnName = other.columnName;
                    dataType = other.dataType;

                    return *this;
                }

                /**
                 * Read using reader.
                 * @param reader Reader.
                 */
                void Read(ignite::impl::binary::BinaryReaderImpl& reader);

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
                 * Get column name.
                 * @return Column name.
                 */
                const std::string& GetColumnName() const
                {
                    return columnName;
                }

                /**
                 * Get data type.
                 * @return Data type.
                 */
                int8_t GetDataType() const 
                {
                    return dataType;
                }

                /**
                 * Try to get attribute of a string type.
                 *
                 * @param fieldId Field ID.
                 * @param value Output attribute value.
                 * @return True if the attribute supported and false otherwise.
                 */
                bool GetAttribute(uint16_t fieldId, std::string& value) const;

                /**
                 * Try to get attribute of a integer type.
                 *
                 * @param fieldId Field ID.
                 * @param value Output attribute value.
                 * @return True if the attribute supported and false otherwise.
                 */
                bool GetAttribute(uint16_t fieldId, SqlLen& value) const;

            private:
                /** Schema name. */
                std::string schemaName;

                /** Table name. */
                std::string tableName;

                /** Column name. */
                std::string columnName;

                /** Data type. */
                int8_t dataType;
            };

            /** Column metadata vector alias. */
            typedef std::vector<ColumnMeta> ColumnMetaVector;

            /**
             * Read columns metadata collection.
             * @param reader Reader.
             * @param meta Collection.
             */
            void ReadColumnMetaVector(ignite::impl::binary::BinaryReaderImpl& reader, ColumnMetaVector& meta);
        }
    }
}

#endif //_IGNITE_ODBC_META_COLUMN_META