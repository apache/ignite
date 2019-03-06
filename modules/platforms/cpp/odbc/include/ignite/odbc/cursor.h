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

#ifndef _IGNITE_ODBC_CURSOR
#define _IGNITE_ODBC_CURSOR

#include <stdint.h>

#include <map>
#include <memory>

#include "ignite/odbc/result_page.h"
#include "ignite/odbc/common_types.h"
#include "ignite/odbc/row.h"

namespace ignite
{
    namespace odbc
    {
        /**
         * Query result cursor.
         */
        class Cursor
        {
        public:
            /**
             * Constructor.
             * @param queryId ID of the executed query.
             */
            Cursor(int64_t queryId);

            /**
             * Destructor.
             */
            ~Cursor();

            /**
             * Move cursor to the next result row.
             *
             * @return False if data update required or no more data.
             */
            bool Increment();

            /**
             * Check if the cursor needs data update.
             *
             * @return True if the cursor needs data update.
             */
            bool NeedDataUpdate() const;

            /**
             * Check if the cursor has data.
             *
             * @return True if the cursor has data.
             */
            bool HasData() const;

            /**
             * Check whether cursor closed remotely.
             *
             * @return true, if the cursor closed remotely.
             */
            bool IsClosedRemotely() const;

            /**
             * Get query ID.
             *
             * @return Query ID.
             */
            int64_t GetQueryId() const
            {
                return queryId;
            }

            /**
             * Update current cursor page data.
             *
             * @param newPage New result page.
             */
            void UpdateData(std::auto_ptr<ResultPage>& newPage);

            /**
             * Get current row.
             *
             * @return Current row. Returns zero if cursor needs data update or has no more data.
             */
            Row* GetRow();

        private:
            IGNITE_NO_COPY_ASSIGNMENT(Cursor);

            /** Cursor id. */
            int64_t queryId;

            /** Current page. */
            std::auto_ptr<ResultPage> currentPage;

            /** Row position in current page. */
            int32_t currentPagePos;

            /** Current row. */
            std::auto_ptr<Row> currentRow;
        };
    }
}

#endif //_IGNITE_ODBC_CURSOR
