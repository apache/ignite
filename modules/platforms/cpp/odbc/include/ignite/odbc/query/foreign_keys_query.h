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

#ifndef _IGNITE_ODBC_QUERY_FOREIGN_KEYS_QUERY
#define _IGNITE_ODBC_QUERY_FOREIGN_KEYS_QUERY

#include "ignite/odbc/connection.h"
#include "ignite/odbc/query/query.h"

namespace ignite
{
    namespace odbc
    {
        namespace query
        {
            /**
             * Foreign keys query.
             */
            class ForeignKeysQuery : public Query
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param diag Diagnostics collector.
                 * @param connection Statement-associated connection.
                 * @param primaryCatalog Primary key catalog name.
                 * @param primarySchema Primary key schema name.
                 * @param primaryTable Primary key table name.
                 * @param foreignCatalog Foreign key catalog name.
                 * @param foreignSchema Foreign key schema name.
                 * @param foreignTable Foreign key table name.
                 */
                ForeignKeysQuery(diagnostic::Diagnosable& diag, Connection& connection,
                    const std::string& primaryCatalog, const std::string& primarySchema,
                    const std::string& primaryTable, const std::string& foreignCatalog,
                    const std::string& foreignSchema, const std::string& foreignTable);

                /**
                 * Destructor.
                 */
                virtual ~ForeignKeysQuery();

                /**
                 * Execute query.
                 *
                 * @return True on success.
                 */
                virtual SqlResult::Type Execute();

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
                IGNITE_NO_COPY_ASSIGNMENT(ForeignKeysQuery);

                /** Connection associated with the statement. */
                Connection& connection;

                /** Primary key catalog name. */
                std::string primaryCatalog;

                /** Primary key schema name. */
                std::string primarySchema;

                /** Primary key table name. */
                std::string primaryTable;

                /** Foreign key catalog name. */
                std::string foreignCatalog;

                /** Foreign key schema name. */
                std::string foreignSchema;

                /** Foreign key table name. */
                std::string foreignTable;

                /** Query executed. */
                bool executed;

                /** Columns metadata. */
                meta::ColumnMetaVector columnsMeta;
            };
        }
    }
}

#endif //_IGNITE_ODBC_QUERY_FOREIGN_KEYS_QUERY