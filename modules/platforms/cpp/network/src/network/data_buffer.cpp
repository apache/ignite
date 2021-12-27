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

#include <ignite/ignite_error.h>
#include <ignite/network/data_buffer.h>

namespace ignite
{
    namespace network
    {
        DataBuffer::DataBuffer() :
            position(0),
            data()
        {
            // No-op.
        }

        DataBuffer::DataBuffer(const impl::interop::SP_ConstInteropMemory& data0, int32_t pos, int32_t len) :
            position(pos),
            length(len),
            data(data0)
        {
            // No-op.
        }

        int8_t DataBuffer::GetByte()
        {
            if (IsEmpty())
                throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Codec error: Not enough data to read next byte");

            int8_t val = data.Get()->Data()[position];
            Advance(1);

            return val;
        }

        void DataBuffer::GetData(int8_t *dst, int32_t size)
        {
            if (!size)
                return;

            if (size < 0)
                throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                    "Codec error: Can not read negative number of bytes");

            if (GetSize() < size)
                throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                    "Codec error: Not enough data to read data from buffer");

            memcpy(dst, data.Get()->Data() + position, size);
            Advance(size);
        }

        int32_t DataBuffer::GetSize() const
        {
            if (!data.IsValid())
                return 0;

            return data.Get()->Length() - position;
        }

        bool DataBuffer::IsEmpty() const
        {
            return GetSize() <= 0;
        }

        DataBuffer DataBuffer::ConsumeEntirely()
        {
            DataBuffer res(*this);
            Advance(GetSize());

            return res;
        }

        void DataBuffer::Advance(int32_t val)
        {
            position += val;
        }

        impl::interop::InteropInputStream DataBuffer::GetInputStream() const
        {
            impl::interop::InteropInputStream stream = impl::interop::InteropInputStream(data.Get(), length);
            stream.Position(position);

            return stream;
        }

        DataBuffer DataBuffer::Clone() const
        {
            if (IsEmpty())
                return DataBuffer();

            impl::interop::SP_InteropMemory mem(new impl::interop::InteropUnpooledMemory(length));
            mem.Get()->Length(length);
            memcpy(mem.Get()->Data(), data.Get()->Data() + position, length);

            return DataBuffer(mem, 0, length);
        }
    }
}
