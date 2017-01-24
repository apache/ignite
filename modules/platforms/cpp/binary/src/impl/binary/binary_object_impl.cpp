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
            BinaryObjectImpl::BinaryObjectImpl(impl::interop::InteropMemory& mem, int32_t start) :
                mem(mem),
                start(start)
            {
                // No-op.
            }

            const int8_t* BinaryObjectImpl::GetData() const
            {
                // Creating header here to validate object header layout.
                BinaryObjectHeader header = BinaryObjectHeader::FromMemory(mem, start);

                return mem.Data() + start + BinaryObjectHeader::SIZE;
            }

            int32_t BinaryObjectImpl::GetLength() const
            {
                BinaryObjectHeader header = BinaryObjectHeader::FromMemory(mem, start);

                return header.GetDataLength();
            }
        }
    }
}