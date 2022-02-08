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

#include <ignite/impl/binary/binary_utils.h>

#include <ignite/network/length_prefix_codec.h>

namespace ignite
{
    namespace network
    {
        using impl::interop::SP_InteropMemory;

        LengthPrefixCodec::LengthPrefixCodec() :
            packetSize(-1)
        {
            // No-op.
        }

        LengthPrefixCodec::~LengthPrefixCodec()
        {
            // No-op.
        }

        DataBuffer LengthPrefixCodec::Encode(DataBuffer& data)
        {
            // Just pass data as is, because we encode message size in
            // the application to avoid unnecessary re-allocations and copying.
            return data.ConsumeEntirely();
        }

        DataBuffer LengthPrefixCodec::Decode(DataBuffer& data)
        {
            if (packet.IsValid() && packet.Get()->Length() == (PACKET_HEADER_SIZE + packetSize))
            {
                packetSize = -1;
                packet.Get()->Length(0);
            }

            if (packetSize < 0)
            {
                Consume(data, PACKET_HEADER_SIZE);

                if (packet.Get()->Length() < PACKET_HEADER_SIZE)
                    return DataBuffer();

                packetSize = impl::binary::BinaryUtils::ReadInt32(*packet.Get(), 0);
            }

            Consume(data, PACKET_HEADER_SIZE + packetSize);

            if (packet.Get()->Length() == (PACKET_HEADER_SIZE + packetSize))
                return DataBuffer(packet, 0, PACKET_HEADER_SIZE + packetSize);

            return DataBuffer();
        }

        void LengthPrefixCodec::Consume(DataBuffer &data, int32_t desired)
        {
            if (!packet.IsValid())
                packet = impl::interop::SP_InteropMemory(new impl::interop::InteropUnpooledMemory(desired));

            impl::interop::InteropMemory& packet0 = *packet.Get();

            if (packet0.Capacity() < desired)
                packet0.Reallocate(desired);

            int32_t toCopy = desired - packet0.Length();
            if (toCopy <= 0)
                return;

            if (data.GetSize() < toCopy)
                toCopy = data.GetSize();

            int8_t* dst = packet0.Data() + packet0.Length();
            packet0.Length(packet0.Length() + toCopy);
            data.Consume(dst, toCopy);
        }
    }
}
