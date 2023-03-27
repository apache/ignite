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

#ifndef _IGNITE_ODBC_ROW
#define _IGNITE_ODBC_ROW

#include <stdint.h>
#include <vector>

#include "ignite/odbc/column.h"
#include "ignite/odbc/app/application_data_buffer.h"


namespace ignite
{
    namespace odbc
    {
        /**
         * Query result row.
         */
        class Row
        {
        public:
            /**
             * Constructor.
             *
             * @param pageData Page data.
             */
            Row(ignite::impl::interop::InteropUnpooledMemory& pageData);

            /**
             * Destructor.
             */
            ~Row();

            /**
             * Get row size in columns.
             *
             * @return Row size.
             */
            int32_t GetSize() const
            {
                return size;
            }

            /**
             * Read column data and store it in application data buffer.
             *
             * @param columnIdx Column index.
             * @param dataBuf Application data buffer.
             * @return Conversion result.
             */
            app::ConversionResult::Type ReadColumnToBuffer(uint16_t columnIdx, app::ApplicationDataBuffer& dataBuf);

            /**
             * Move to next row.
             *
             * @return True on success.
             */
            bool MoveToNext();

        private:
            IGNITE_NO_COPY_ASSIGNMENT(Row);

            /**
             * Reinitialize row state using stream data.
             * @note Stream must be positioned at the beginning of the row.
             */
            void Reinit();

            /**
             * Get columns by its index.
             *
             * Column indexing starts at 1.
             *
             * @note This operation is private because it's unsafe to use:
             *       It is neccessary to ensure that column is discovered prior
             *       to calling this method using EnsureColumnDiscovered().
             *
             * @param columnIdx Column index.
             * @return Reference to specified column.
             */
            Column& GetColumn(uint16_t columnIdx)
            {
                return columns[columnIdx - 1];
            }

            /**
             * Ensure that column data is discovered.
             *
             * @param columnIdx Column index.
             * @return True if the column is discovered and false if it can not
             * be discovered.
             */
            bool EnsureColumnDiscovered(uint16_t columnIdx);

            /** Row position in current page. */
            int32_t rowBeginPos;

            /** Current position in row. */
            int32_t pos;

            /** Row size in columns. */
            int32_t size;

            /** Memory that contains current row data. */
            ignite::impl::interop::InteropUnpooledMemory& pageData;

            /** Page data input stream. */
            ignite::impl::interop::InteropInputStream stream;

            /** Data reader. */
            ignite::impl::binary::BinaryReaderImpl reader;

            /** Columns. */
            std::vector<Column> columns;
        };
    }
}

#endif //_IGNITE_ODBC_ROW