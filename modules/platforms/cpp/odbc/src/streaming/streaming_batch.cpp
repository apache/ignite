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

#include "ignite/impl/interop/interop_output_stream.h"
#include "ignite/impl/binary/binary_writer_impl.h"

#include "ignite/odbc/app/parameter_set.h"
#include "ignite/odbc/streaming/streaming_batch.h"

namespace ignite
{
    namespace odbc
    {
        namespace streaming
        {
            StreamingBatch::StreamingBatch() :
                currentSql(),
                size(0),
                data(1024 * 16)
            {
                // No-op.
            }

            StreamingBatch::~StreamingBatch()
            {
                // No-op.
            }

            void StreamingBatch::AddRow(const std::string& sql, const app::ParameterSet& params)
            {
                impl::interop::InteropOutputStream out(&data);

                out.Position(data.Length());

                impl::binary::BinaryWriterImpl writer(&out, 0);

                if (currentSql != sql)
                {
                    currentSql = sql;

                    writer.WriteString(sql);
                }
                else
                    writer.WriteNull();

                params.Write(writer);
                ++size;

                out.Synchronize();
            }

            void StreamingBatch::Clear()
            {
                currentSql.clear();

                size = 0;

                data.Length(0);
            }
        }
    }
}
