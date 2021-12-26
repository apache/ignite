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

#ifndef _IGNITE_NETWORK_DATA_BUFFER
#define _IGNITE_NETWORK_DATA_BUFFER

#include <ignite/impl/interop/interop_memory.h>

namespace ignite
{
    namespace network
    {
        class DataBuffer
        {
        public:
            /**
             * Default constructor.
             */
            DataBuffer();

            /**
             * Constructor.
             */
            DataBuffer(const impl::interop::SP_ConstInteropMemory& data, int32_t pos);

            /**
             * Destructor.
             */
            ~DataBuffer()
            {
                // No-op.
            }

            /**
             * Read next byte from the beginning of the buffer.
             *
             * @return Value.
             */
            int8_t GetByte();

            /**
             * Copy data from buffer to specified place in memory.
             *
             * @param dst Destination in memory.
             * @param size Number of bytes to copy.
             */
            void GetData(int8_t* dst, int32_t size);

            /**
             * Get packet size.
             *
             * @return Packet size.
             */
            int32_t GetSize() const;

            /**
             * Check whether data buffer was fully consumed.
             *
             * @return @c true if consumed and @c false otherwise.
             */
            bool IsConsumed() const;

            /**
             * Consume the whole buffer.
             *
             * @return Buffer containing consumed data.
             */
            DataBuffer ConsumeEntirely();

        private:
            /**
             * Advance position in packet by specified value.
             *
             * @param val Bytes to advance.
             */
            void Advance(int32_t val);

            /** Position in current data. */
            int32_t position;

            /** Data */
            const impl::interop::SP_ConstInteropMemory data;
        };
    }
}

#endif //_IGNITE_NETWORK_DATA_BUFFER
