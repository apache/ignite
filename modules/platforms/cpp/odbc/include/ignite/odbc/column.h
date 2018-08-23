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