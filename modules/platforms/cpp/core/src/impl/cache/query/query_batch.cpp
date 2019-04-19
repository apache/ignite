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

#include "ignite/impl/cache/query/query_batch.h"
#include "ignite/impl/cache/query/query_fields_row_impl.h"

namespace ignite
{
    namespace impl
    {
        namespace cache
        {
            namespace query
            {
                QueryFieldsRowImpl* QueryBatch::GetNextRow()
                {
                    assert(Left() > 0);

                    int32_t rowBegin = stream.Position();

                    int32_t rowLen = reader.ReadInt32();
                    int32_t columnNum = reader.ReadInt32();

                    int32_t dataPos = stream.Position();

                    assert(rowLen >= 4);

                    ++pos;

                    stream.Position(rowBegin + rowLen);

                    return new QueryFieldsRowImpl(mem, dataPos, columnNum);
                }

            }
        }
    }
}
