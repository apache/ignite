/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

#ifndef _IGNITE_ODBC_META_COLUMN_META
#define _IGNITE_ODBC_META_COLUMN_META

#include <stdint.h>
#include <string>

#include "ignite/impl/binary/binary_reader_impl.h"

#include "ignite/odbc/protocol_version.h"
#include "ignite/odbc/common_types.h"
#include "ignite/odbc/utility.h"

namespace ignite
{
    namespace odbc
    {
        namespace meta
        {
            using namespace ignite::odbc;

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
                    schemaName(schemaName), tableName(tableName), columnName(columnName), dataType(dataType),
                    precision(-1), scale(-1)
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
                    dataType(other.dataType),
                    precision(other.precision),
                    scale(other.scale)
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
                    precision = other.precision;
                    scale = other.scale;

                    return *this;
                }

                /**
                 * Read using reader.
                 * @param reader Reader.
                 * @param ver Server version.
                 */
                void Read(ignite::impl::binary::BinaryReaderImpl& reader, const ProtocolVersion& ver);

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
                 * Get column precision.
                 * @return Column precision.
                 */
                const int32_t GetPrecision() const
                {
                    return precision;
                }

                /**
                 * Get column scale.
                 * @return Column scale.
                 */
                const int32_t GetScale() const
                {
                    return scale;
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

                /** Column precision. */
                int32_t precision;

                /** Column scale. */
                int32_t scale;
            };

            /** Column metadata vector alias. */
            typedef std::vector<ColumnMeta> ColumnMetaVector;

            /**
             * Read columns metadata collection.
             * @param reader Reader.
             * @param meta Collection.
             * @param ver Server protocol version.
             */
            void ReadColumnMetaVector(ignite::impl::binary::BinaryReaderImpl& reader, ColumnMetaVector& meta,
                    const ProtocolVersion& ver);
        }
    }
}

#endif //_IGNITE_ODBC_META_COLUMN_META
