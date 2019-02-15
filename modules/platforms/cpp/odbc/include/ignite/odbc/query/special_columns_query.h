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

#ifndef _IGNITE_ODBC_QUERY_SPECIAL_COLUMNS_QUERY
#define _IGNITE_ODBC_QUERY_SPECIAL_COLUMNS_QUERY

#include "ignite/odbc/query/query.h"

namespace ignite
{
    namespace odbc
    {
        namespace query
        {
            /**
             * Special columns query.
             */
            class SpecialColumnsQuery : public Query
            {
            public:

                /**
                 * Constructor.
                 *
                 * @param diag Diagnostics collector.
                 * @param catalog Catalog name.
                 * @param schema Schema name.
                 * @param table Table name.
                 * @param scope Minimum required scope of the rowid.
                 * @param nullable Determines whether to return special columns
                 *                 that can have a NULL value.
                 */
                SpecialColumnsQuery(diagnostic::Diagnosable& diag, int16_t type,
                    const std::string& catalog, const std::string& schema,
                    const std::string& table, int16_t scope, int16_t nullable);

                /**
                 * Destructor.
                 */
                virtual ~SpecialColumnsQuery();

                /**
                 * Execute query.
                 *
                 * @return True on success.
                 */
                virtual SqlResult::Type Execute();

                /**
                 * Fetch next result row to application buffers.
                 *
                 * @param columnBindings Application buffers to put data to.
                 * @return Operation result.
                 */
                virtual SqlResult::Type FetchNextRow(app::ColumnBindingMap& columnBindings);

                /**
                 * Get data of the specified column in the result set.
                 *
                 * @param columnIdx Column index.
                 * @param buffer Buffer to put column data to.
                 * @return Operation result.
                 */
                virtual SqlResult::Type GetColumn(uint16_t columnIdx, app::ApplicationDataBuffer& buffer);

                /**
                 * Close query.
                 *
                 * @return True on success.
                 */
                virtual SqlResult::Type Close();

                /**
                 * Get column metadata.
                 *
                 * @return Column metadata.
                 */
                virtual const meta::ColumnMetaVector& GetMeta() const;

                /**
                 * Check if data is available.
                 *
                 * @return True if data is available.
                 */
                virtual bool DataAvailable() const;

                /**
                 * Get number of rows affected by the statement.
                 *
                 * @return Number of rows affected by the statement.
                 */
                virtual int64_t AffectedRows() const;

                /**
                 * Move to the next result set.
                 *
                 * @return Operation result.
                 */
                virtual SqlResult::Type NextResultSet();

            private:
                IGNITE_NO_COPY_ASSIGNMENT(SpecialColumnsQuery);

                /** Query type. */
                int16_t type;

                /** Catalog name. */
                std::string catalog;

                /** Schema name. */
                std::string schema;

                /** Table name. */
                std::string table;

                /** Minimum required scope of the rowid. */
                int16_t scope;

                /**
                 * Determines whether to return special columns that can have
                 * a NULL value.
                 */
                int16_t nullable;

                /** Query executed. */
                bool executed;

                /** Columns metadata. */
                meta::ColumnMetaVector columnsMeta;
            };
        }
    }
}

#endif //_IGNITE_ODBC_QUERY_SPECIAL_COLUMNS_QUERY