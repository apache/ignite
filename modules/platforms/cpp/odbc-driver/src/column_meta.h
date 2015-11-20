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

#include "utility.h"

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
            void Read(ignite::impl::binary::BinaryReaderImpl& reader)
            {
                utility::ReadString(reader, schemaName);
                utility::ReadString(reader, typeName);
                utility::ReadString(reader, fieldName);
                utility::ReadString(reader, fieldTypeName);
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
             * Get type name.
             * @return Type name.
             */
            const std::string& GetTypeName() const
            {
                return typeName;
            }

            /**
             * Get field name.
             * @return Field name.
             */
            const std::string& GetFieldName() const
            {
                return fieldName;
            }

            /**
             * Get field type name.
             * @return Field type name.
             */
            const std::string& GetFieldTypeName() const
            {
                return fieldTypeName;
            }

        private:
            /** Schema name. */
            std::string schemaName;

            /** Type name. */
            std::string typeName;

            /** Name. */
            std::string fieldName;

            /** Type. */
            std::string fieldTypeName;
        };
    }
}

#endif