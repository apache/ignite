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

#ifndef _IGNITE_ODBC_DRIVER_ROW
#define _IGNITE_ODBC_DRIVER_ROW

#include <stdint.h>

#include "result_page.h"
#include "common_types.h"
#include "application_data_buffer.h"

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
             */
            Row(ignite::impl::interop::InteropUnpooledMemory& pageData);

            /**
             * Destructor.
             */
            ~Row();

            /**
             * Get row size in columns.
             * @return Row size.
             */
            int32_t GetSize() const
            {
                return size;
            }

            /**
             * Read column data and store it in application data buffer.
             * @param dataBuf Application data buffer.
             */
            void ReadColumnToBuffer(ApplicationDataBuffer& dataBuf);


        private:
            /** Row size in columns. */
            int32_t size;

            /** Row position in current page. */
            int32_t rowBeginPos;

            /** Memory that contains current row data. */
            ignite::impl::interop::InteropUnpooledMemory& pageData;

            /** Page data input stream. */
            ignite::impl::interop::InteropInputStream stream;
        };
    }
}

#endif