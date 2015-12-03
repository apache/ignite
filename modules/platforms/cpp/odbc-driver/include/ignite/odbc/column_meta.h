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

#ifndef _IGNITE_ODBC_COLUMN_META
#define _IGNITE_ODBC_COLUMN_META

#include <stdint.h>
#include <string>

#include "ignite/impl/binary/binary_reader_impl.h"

#include "ignite/odbc/utility.h"

namespace ignite
{
    namespace odbc
    {
        class ColumnMeta
        {
        public:
            /**
             * Default constructor.
             */
            ColumnMeta()
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
             * Get type name.
             * @return Type name.
             */
            const std::string& GetTypeName() const
            {
                return tableName;
            }

            /**
             * Get field name.
             * @return Field name.
             */
            const std::string& GetFieldName() const
            {
                return columnName;
            }

            /**
             * Get field type name.
             * @return Field type name.
             */
            const std::string& GetFieldTypeName() const
            {
                return typeName;
            }

            /**
             * Get data type.
             * @return Data type.
             */
            int8_t GetDataType() const 
            {
                return dataType;
            }

        private:
            /** Schema name. */
            std::string schemaName;

            /** Table name. */
            std::string tableName;

            /** Column name. */
            std::string columnName;

            /** Type name. */
            std::string typeName;

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

#endif