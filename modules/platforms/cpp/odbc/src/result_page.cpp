/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <ignite/impl/interop/interop_input_stream.h>

#include "ignite/odbc/result_page.h"
#include "ignite/odbc/utility.h"

namespace ignite
{
    namespace odbc
    {
        ResultPage::ResultPage() :
            last(false), size(0), data(DEFAULT_ALLOCATED_MEMORY)
        {
            //No-op.
        }

        ResultPage::~ResultPage()
        {
            //No-op.
        }

        void ResultPage::Read(ignite::impl::binary::BinaryReaderImpl& reader)
        {
            last = reader.ReadBool();
            size = reader.ReadInt32();

            impl::interop::InteropInputStream& stream = *reader.GetStream();

            int32_t dataToRead = stream.Remaining();

            data.Length(dataToRead);

            if (dataToRead)
            {
                data.Reallocate(dataToRead);

                reader.GetStream()->ReadInt8Array(data.Data(), dataToRead);
            }
        }
    }
}

