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

#ifndef _IGNITE_ODBC_RESULT_PAGE
#define _IGNITE_ODBC_RESULT_PAGE

#include <stdint.h>

#include <ignite/impl/binary/binary_reader_impl.h>

#include "ignite/odbc/app/application_data_buffer.h"
#include "ignite/odbc/common_types.h"

namespace ignite
{
    namespace odbc
    {
        /**
         * Query result page.
         */
        class ResultPage
        {
            enum { DEFAULT_ALLOCATED_MEMORY = 1024 };

        public:
            /**
             * Constructor.
             */
            ResultPage();

            /**
             * Destructor.
             */
            ~ResultPage();
            
            /**
             * Read result page using provided reader.
             * @param reader Reader.
             */
            void Read(ignite::impl::binary::BinaryReaderImpl& reader);

            /**
             * Get page size.
             * @return Page size.
             */
            int32_t GetSize() const
            {
                return size;
            }

            /**
             * Check if the page is last.
             * @return True if the page is last.
             */
            bool IsLast() const
            {
                return last;
            }

            /**
             * Get page data.
             * @return Page data.
             */
            ignite::impl::interop::InteropUnpooledMemory& GetData()
            {
                return data;
            }

        private:
            IGNITE_NO_COPY_ASSIGNMENT(ResultPage);

            /** Last page flag. */
            bool last;

            /** Page size in rows. */
            int32_t size;

            /** Memory that contains current row page data. */
            ignite::impl::interop::InteropUnpooledMemory data;
        };
    }
}

#endif //_IGNITE_ODBC_RESULT_PAGE