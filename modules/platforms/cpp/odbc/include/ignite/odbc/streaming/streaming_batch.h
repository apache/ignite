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

#ifndef _IGNITE_ODBC_STREAMING_STREAMING_BATCH
#define _IGNITE_ODBC_STREAMING_STREAMING_BATCH

#include <string>

#include "ignite/impl/interop/interop_memory.h"


namespace ignite
{
    namespace odbc
    {
        namespace app
        {
            // Forward declaration.
            class ParameterSet;
        }
        
        namespace streaming
        {
            /**
             * Streaming batch.
             *
             * Accumulates data for streaming.
             */
            class StreamingBatch
            {
            public:
                /**
                 * Default constructor.
                 */
                StreamingBatch();

                /**
                 * Destructor.
                 */
                ~StreamingBatch();

                /**
                 * Add another row to a batch.
                 *
                 * @param sql Sql.
                 * @param params Parameters.
                 */
                void AddRow(const std::string& sql, const app::ParameterSet& params);

                /**
                 * Clear the batch data. 
                 */
                void Clear();

                /**
                 * Get data.
                 *
                 * @return Data.
                 */
                const int8_t* GetData() const
                {
                    return data.Data();
                }

                /**
                 * Get data length.
                 *
                 * @return Data length.
                 */
                int32_t GetDataLength() const
                {
                    return data.Length();
                }

                /**
                 * Get number of rows in batch.
                 *
                 * @return Number of rows in batch.
                 */
                int32_t GetSize() const
                {
                    return size;
                }

            private:
                IGNITE_NO_COPY_ASSIGNMENT(StreamingBatch);

                /** Current SQL. */
                std::string currentSql;

                /** Batch size in rows. */
                int32_t size;

                /** Batch data. */
                impl::interop::InteropUnpooledMemory data;
            };
        }
    }
}

#endif //_IGNITE_ODBC_STREAMING_STREAMING_BATCH
