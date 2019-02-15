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

#ifndef _IGNITE_ODBC_COLUMN
#define _IGNITE_ODBC_COLUMN

#include <stdint.h>

#include <ignite/impl/binary/binary_reader_impl.h>

#include "ignite/odbc/app/application_data_buffer.h"

namespace ignite
{
    namespace odbc
    {
        /**
         * Result set column.
         */
        class Column
        {
        public:
            /**
             * Default constructor.
             */
            Column();

            /**
             * Copy constructor.
             *
             * @param other Another instance.
             */
            Column(const Column& other);

            /**
             * Copy operator.
             *
             * @param other Another instance.
             * @return This.
             */
            Column& operator=(const Column& other);

            /**
             * Destructor.
             */
            ~Column();

            /**
             * Constructor.
             *
             * @param reader Reader to be used to retrieve column data.
             */
            Column(ignite::impl::binary::BinaryReaderImpl& reader);

            /**
             * Get column size in bytes.
             *
             * @return Column size.
             */
            int32_t GetSize() const
            {
                return size;
            }

            /**
             * Read column data and store it in application data buffer.
             *
             * @param reader Reader to use.
             * @param dataBuf Application data buffer.
             * @return Operation result.
             */
            app::ConversionResult::Type ReadToBuffer(ignite::impl::binary::BinaryReaderImpl& reader,
                app::ApplicationDataBuffer& dataBuf);

            /**
             * Check if the column is in valid state.
             *
             * Invalid instance can be returned if some of the previous
             * operations have resulted in a failure. For example invalid
             * instance can be returned by not-throwing version of method
             * in case of error. Invalid instances also often can be
             * created using default constructor.
             *
             * @return True if valid.
             */
            bool IsValid() const
            {
                return startPos >= 0;
            }

            /**
             * Get unread data length.
             * Find out how many bytes of data are left unread.
             *
             * @return Lengh of unread data in bytes.
             */
            int32_t GetUnreadDataLength() const
            {
                return size - offset;
            }

            /**
             * Get unread data length.
             * Find out how many bytes of data are left unread.
             *
             * @return Lengh of unread data in bytes.
             */
            int32_t GetEndPosition() const
            {
                return endPos;
            }

        private:
            /**
             * Increase offset.
             *
             * Increases offset on specified value and makes sure resulting
             * offset does not exceed column size.
             *
             * @param value Offset is incremented on this value.
             */
            void IncreaseOffset(int32_t value);

            /** Column type */
            int8_t type;

            /** Column position in current row. */
            int32_t startPos;

            /** Column end position in current row. */
            int32_t endPos;

            /** Current offset in column. */
            int32_t offset;

            /** Column data size in bytes. */
            int32_t size;
        };
    }
}

#endif //_IGNITE_ODBC_COLUMN