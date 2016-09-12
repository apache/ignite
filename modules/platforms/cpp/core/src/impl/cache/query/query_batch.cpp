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
