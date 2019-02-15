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

#ifndef _IGNITE_ODBC_META_PRIMARY_KEY_META
#define _IGNITE_ODBC_META_PRIMARY_KEY_META

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
             * Primary key metadata.
             */
            class PrimaryKeyMeta
            {
            public:
                /**
                 * Default constructor.
                 */
                PrimaryKeyMeta()
                {
                    // No-op.
                }
            
                /**
                 * Constructor.
                 *
                 * @param catalog Catalog name.
                 * @param schema Schema name.
                 * @param table Table name.
                 * @param column Column name.
                 * @param keySeq Column sequence number in key (starting with 1).
                 * @param keyName Key name.
                 */
                PrimaryKeyMeta(const std::string& catalog, const std::string& schema,
                    const std::string& table, const std::string& column, int16_t keySeq,
                    const std::string& keyName) :
                    catalog(catalog),
                    schema(schema),
                    table(table),
                    column(column),
                    keySeq(keySeq),
                    keyName(keyName)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                ~PrimaryKeyMeta()
                {
                    // No-op.
                }

                /**
                 * Copy constructor.
                 */
                PrimaryKeyMeta(const PrimaryKeyMeta& other) :
                    catalog(other.catalog),
                    schema(other.schema),
                    table(other.table),
                    column(other.column),
                    keySeq(other.keySeq),
                    keyName(other.keyName)
                {
                    // No-op.
                }

                /**
                 * Copy operator.
                 */
                PrimaryKeyMeta& operator=(const PrimaryKeyMeta& other)
                {
                    catalog = other.catalog;
                    schema = other.schema;
                    table = other.table;
                    column = other.column;
                    keySeq = other.keySeq;
                    keyName = other.keyName;

                    return *this;
                }

                /**
                 * Get catalog name.
                 * @return Catalog name.
                 */
                const std::string& GetCatalogName() const
                {
                    return catalog;
                }

                /**
                 * Get schema name.
                 * @return Schema name.
                 */
                const std::string& GetSchemaName() const
                {
                    return schema;
                }

                /**
                 * Get table name.
                 * @return Table name.
                 */
                const std::string& GetTableName() const
                {
                    return table;
                }

                /**
                 * Get column name.
                 * @return Column name.
                 */
                const std::string& GetColumnName() const
                {
                    return table;
                }

                /**
                 * Get column sequence number in key.
                 * @return Sequence number in key.
                 */
                int16_t GetKeySeq() const
                {
                    return keySeq;
                }

                /**
                 * Get key name.
                 * @return Key name.
                 */
                const std::string& GetKeyName() const
                {
                    return keyName;
                }

            private:
                /** Catalog name. */
                std::string catalog;

                /** Schema name. */
                std::string schema;

                /** Table name. */
                std::string table;

                /** Collumn name. */
                std::string column;
                
                /** Column sequence number in key. */
                int16_t keySeq;

                /** Key name. */
                std::string keyName;
            };

            /** Table metadata vector alias. */
            typedef std::vector<PrimaryKeyMeta> PrimaryKeyMetaVector;
        }
    }
}

#endif //_IGNITE_ODBC_META_PRIMARY_KEY_META