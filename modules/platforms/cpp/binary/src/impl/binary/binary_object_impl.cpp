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

#include <ignite/impl/binary/binary_object_header.h>
#include <ignite/impl/binary/binary_object_impl.h>

using namespace ignite::impl::binary;

namespace ignite
{
    namespace impl
    {
        namespace binary
        {
            BinaryObjectImpl::BinaryObjectImpl(interop::InteropMemory& mem, int32_t start) :
                mem(&mem),
                start(start)
            {
                // No-op.
            }

            BinaryObjectImpl::BinaryObjectImpl(const BinaryObjectImpl& other) :
                mem(other.mem),
                start(other.start)
            {
                // No-op.
            }

            BinaryObjectImpl& BinaryObjectImpl::operator=(const BinaryObjectImpl& other)
            {
                mem = other.mem;

                return *this;
            }

            BinaryObjectImpl BinaryObjectImpl::FromMemory(interop::InteropMemory& mem, int32_t offset)
            {
                BinaryObjectHeader header = BinaryObjectHeader::FromMemory(mem, offset);

                int32_t adjustedStart = static_cast<int32_t>(header.GetMem() - mem.Data());

                assert(adjustedStart >= 0);

                return BinaryObjectImpl(mem, adjustedStart);
            }

            BinaryObjectImpl BinaryObjectImpl::FromMemoryUnsafe(interop::InteropMemory& mem, int32_t offset)
            {
                return BinaryObjectImpl(mem, offset);
            }

            BinaryObjectImpl BinaryObjectImpl::GetField(int32_t idx)
            {
                int32_t offset = start + BinaryObjectHeader::SIZE;

                for (int32_t i = 0; i < idx; ++i)
                {
                    BinaryObjectHeader fieldHeader = BinaryObjectHeader::FromMemory(*mem, offset);

                    offset += fieldHeader.GetLength();
                }

                return BinaryObjectImpl::FromMemory(*mem, offset);
            }

            const int8_t* BinaryObjectImpl::GetData() const
            {
                return mem->Data() + start + BinaryObjectHeader::SIZE;
            }

            int32_t BinaryObjectImpl::GetLength() const
            {
                BinaryObjectHeader header(mem->Data() + start);

                return header.GetDataLength();
            }

            int32_t BinaryObjectImpl::GetTypeId() const
            {
                BinaryObjectHeader header(mem->Data() + start);

                return header.GetTypeId();
            }

            int32_t BinaryObjectImpl::GetHashCode() const
            {
                BinaryObjectHeader header(mem->Data() + start);

                return header.GetHashCode();
            }
        }
    }
}