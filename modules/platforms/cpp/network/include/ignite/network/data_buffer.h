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
#include <ignite/impl/interop/interop_input_stream.h>

namespace ignite
{
    namespace network
    {
        /**
         * Data buffer.
         */
        class IGNITE_IMPORT_EXPORT DataBuffer
        {
        public:
            /**
             * Default constructor.
             */
            DataBuffer();

            /**
             * Constructor.
             *
             * @param data Data.
             */
            explicit DataBuffer(const impl::interop::SP_ConstInteropMemory& data);

            /**
             * Constructor.
             *
             * @param data Data.
             * @param pos Start of data.
             * @param len Length.
             */
            DataBuffer(const impl::interop::SP_ConstInteropMemory& data, int32_t pos, int32_t len);

            /**
             * Destructor.
             */
            ~DataBuffer()
            {
                // No-op.
            }

            /**
             * Consume data from buffer to specified place in memory.
             *
             * @param dst Destination in memory.
             * @param size Number of bytes to copy.
             */
            void Consume(int8_t* dst, int32_t size);

            /**
             * Get data pointer.
             *
             * @return Data.
             */
            const int8_t* GetData() const;

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
            bool IsEmpty() const;

            /**
             * Consume the whole buffer.
             *
             * @return Buffer containing consumed data.
             */
            DataBuffer ConsumeEntirely();

            /**
             * Get input stream for a data buffer.
             * @return Stream set up to read data from buffer.
             */
            impl::interop::InteropInputStream GetInputStream() const;

            /**
             * Clone underlying buffer into a new one.
             *
             * @return New data buffer.
             */
            DataBuffer Clone() const;

            /**
             * Skip specified number of bytes.
             *
             * @param bytes Bytes to skip.
             */
            void Skip(int32_t bytes);

        private:
            /**
             * Advance position in packet by specified value.
             *
             * @param val Bytes to advance.
             */
            void Advance(int32_t val);

            /** Position in current data. */
            int32_t position;

            /** Data length. */
            int32_t length;

            /** Data */
            impl::interop::SP_ConstInteropMemory data;
        };
    }
}

#endif //_IGNITE_NETWORK_DATA_BUFFER
