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

#include "ignite/impl/cache/query/query_batch.h"
#include "ignite/impl/cache/query/query_fields_row_impl.h"

using namespace ignite::common::concurrent;
using namespace ignite::common::java;
using namespace ignite::impl::interop;
using namespace ignite::impl::binary;

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

                    int32_t columnNum = reader.ReadInt32();

                    for (int32_t i = 0; i < columnNum; ++i)
                        reader.SkipTopObject();

                    int32_t rowEnd = stream.Position();

                    int32_t rowLen = rowEnd - rowBegin;

                    assert(rowLen >= 4);

                    MemorySharedPtr rowMem = env.Get()->AllocateMemory(rowLen);

                    memcpy(rowMem.Get()->Data(), mem.Get()->Data() + rowBegin, rowLen);

                    rowMem.Get()->Length(rowLen);

                    ++pos;

                    return new QueryFieldsRowImpl(rowMem);
                }

            }
        }
    }
}