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

#include <time.h>

#include "ignite/ignite_error.h"

#include "ignite/impl/interop/interop.h"
#include "ignite/impl/binary/binary_utils.h"

using namespace ignite::impl::interop;
using namespace ignite::impl::binary;

namespace
{
    /**
     * Check if there is enough data in memory.
     * @throw IgniteError if there is not enough memory.
     *
     * @param mem Memory.
     * @param pos Position.
     * @param len Data to read.
     */
    inline void CheckEnoughData(InteropMemory& mem, int32_t pos, int32_t len)
    {
        if (mem.Length() < (pos + len))
        {
            IGNITE_ERROR_FORMATTED_4(ignite::IgniteError::IGNITE_ERR_MEMORY, "Not enough data in "
                "the binary object", "memPtr", mem.PointerLong(), "len", mem.Length(), "pos", pos,
                "requested", len);
        }
    }

    /**
     * Read primitive int type from the specific place in memory.
     * @throw IgniteError if there is not enough memory.
     *
     * @param mem Memory.
     * @param pos Position.
     * @return Primitive.
     */
    template<typename T>
    inline T ReadPrimitive(InteropMemory& mem, int32_t pos)
    {
        CheckEnoughData(mem, pos, sizeof(T));

        return *reinterpret_cast<T*>(mem.Data() + pos);
    }

    /**
     * Read primitive int type from the specific place in memory.
     * @warning Does not check if there is enough data in memory to read.
     *
     * @param mem Memory.
     * @param pos Position.
     * @return Primitive.
     */
    template<typename T>
    inline T UnsafeReadPrimitive(InteropMemory& mem, int32_t pos)
    {
        return *reinterpret_cast<T*>(mem.Data() + pos);
    }
}

namespace ignite
{
    namespace impl
    {
        namespace binary
        {
            int32_t BinaryUtils::GetDataHashCode(const void * data, size_t size)
            {
                if (data)
                {
                    int32_t hash = 1;
                    const int8_t* bytes = static_cast<const int8_t*>(data);

                    for (int i = 0; i < size; ++i)
                        hash = 31 * hash + bytes[i];

                    return hash;
                }

                return 0;
            }

            int8_t BinaryUtils::ReadInt8(InteropInputStream* stream)
            {
                return stream->ReadInt8();
            }

            int8_t BinaryUtils::ReadInt8(InteropMemory& mem, int32_t pos)
            {
                return ReadPrimitive<int8_t>(mem, pos);
            }

            int8_t BinaryUtils::UnsafeReadInt8(interop::InteropMemory& mem, int32_t pos)
            {
                return UnsafeReadPrimitive<int8_t>(mem, pos);
            }

            void BinaryUtils::WriteInt8(InteropOutputStream* stream, int8_t val)
            {
                stream->WriteInt8(val); 
            }

            void BinaryUtils::ReadInt8Array(InteropInputStream* stream, int8_t* res, const int32_t len)
            {
                stream->ReadInt8Array(res, len);
            }

            void BinaryUtils::WriteInt8Array(InteropOutputStream* stream, const int8_t* val, const int32_t len)
            {
                stream->WriteInt8Array(val, len);
            }

            bool BinaryUtils::ReadBool(InteropInputStream* stream)
            {
                return stream->ReadBool();
            }

            void BinaryUtils::WriteBool(InteropOutputStream* stream, bool val)
            {
                stream->WriteBool(val);
            }

            void BinaryUtils::ReadBoolArray(InteropInputStream* stream, bool* res, const int32_t len)
            {
                stream->ReadBoolArray(res, len);
            }

            void BinaryUtils::WriteBoolArray(InteropOutputStream* stream, const bool* val, const int32_t len)
            {
                stream->WriteBoolArray(val, len);
            }

            int16_t BinaryUtils::ReadInt16(InteropInputStream* stream)
            {
                return stream->ReadInt16();
            }

            int16_t BinaryUtils::ReadInt16(interop::InteropMemory& mem, int32_t pos)
            {
                return ReadPrimitive<int16_t>(mem, pos);
            }

            int16_t BinaryUtils::UnsafeReadInt16(interop::InteropMemory& mem, int32_t pos)
            {
                return UnsafeReadPrimitive<int16_t>(mem, pos);
            }

            void BinaryUtils::WriteInt16(InteropOutputStream* stream, int16_t val)
            {
                stream->WriteInt16(val);
            }

            void BinaryUtils::ReadInt16Array(InteropInputStream* stream, int16_t* res, const int32_t len)
            {
                stream->ReadInt16Array(res, len);
            }
            
            void BinaryUtils::WriteInt16Array(InteropOutputStream* stream, const int16_t* val, const int32_t len)
            {
                stream->WriteInt16Array(val, len);
            }

            uint16_t BinaryUtils::ReadUInt16(InteropInputStream* stream)
            {
                return stream->ReadUInt16();
            }

            void BinaryUtils::WriteUInt16(InteropOutputStream* stream, uint16_t val)
            {
                stream->WriteUInt16(val);
            }

            void BinaryUtils::ReadUInt16Array(InteropInputStream* stream, uint16_t* res, const int32_t len)
            {
                stream->ReadUInt16Array(res, len);
            }

            void BinaryUtils::WriteUInt16Array(InteropOutputStream* stream, const uint16_t* val, const int32_t len)
            {
                stream->WriteUInt16Array(val, len);
            }

            int32_t BinaryUtils::ReadInt32(InteropInputStream* stream)
            {
                return stream->ReadInt32();
            }

            int32_t BinaryUtils::ReadInt32(interop::InteropMemory& mem, int32_t pos)
            {
                return ReadPrimitive<int32_t>(mem, pos);
            }

            int32_t BinaryUtils::UnsafeReadInt32(interop::InteropMemory& mem, int32_t pos)
            {
                return UnsafeReadPrimitive<int32_t>(mem, pos);
            }

            void BinaryUtils::WriteInt32(InteropOutputStream* stream, int32_t val)
            {
                stream->WriteInt32(val);
            }

            void BinaryUtils::ReadInt32Array(InteropInputStream* stream, int32_t* res, const int32_t len)
            {
                stream->ReadInt32Array(res, len);
            }

            void BinaryUtils::WriteInt32Array(InteropOutputStream* stream, const int32_t* val, const int32_t len)
            {
                stream->WriteInt32Array(val, len);
            }

            int64_t BinaryUtils::ReadInt64(InteropInputStream* stream)
            {
                return stream->ReadInt64();
            }

            void BinaryUtils::WriteInt64(InteropOutputStream* stream, int64_t val)
            {
                stream->WriteInt64(val);
            }

            void BinaryUtils::ReadInt64Array(InteropInputStream* stream, int64_t* res, const int32_t len)
            {
                stream->ReadInt64Array(res, len);
            }

            void BinaryUtils::WriteInt64Array(InteropOutputStream* stream, const int64_t* val, const int32_t len)
            {
                stream->WriteInt64Array(val, len);
            }

            float BinaryUtils::ReadFloat(InteropInputStream* stream)
            {
                return stream->ReadFloat();
            }

            void BinaryUtils::WriteFloat(InteropOutputStream* stream, float val)
            {
                stream->WriteFloat(val);
            }

            void BinaryUtils::ReadFloatArray(InteropInputStream* stream, float* res, const int32_t len)
            {
                stream->ReadFloatArray(res, len);
            }

            void BinaryUtils::WriteFloatArray(InteropOutputStream* stream, const float* val, const int32_t len)
            {
                stream->WriteFloatArray(val, len);
            }

            double BinaryUtils::ReadDouble(InteropInputStream* stream)
            {
                return stream->ReadDouble();
            }

            void BinaryUtils::WriteDouble(InteropOutputStream* stream, double val)
            {
                stream->WriteDouble(val);
            }

            void BinaryUtils::ReadDoubleArray(InteropInputStream* stream, double* res, const int32_t len)
            {
                stream->ReadDoubleArray(res, len);
            }

            void BinaryUtils::WriteDoubleArray(InteropOutputStream* stream, const double* val, const int32_t len)
            {
                stream->WriteDoubleArray(val, len);
            }

            Guid BinaryUtils::ReadGuid(interop::InteropInputStream* stream)
            {
                int64_t most = stream->ReadInt64();
                int64_t least = stream->ReadInt64();

                return Guid(most, least);
            }

            void BinaryUtils::WriteGuid(interop::InteropOutputStream* stream, const Guid val)
            {
                stream->WriteInt64(val.GetMostSignificantBits());
                stream->WriteInt64(val.GetLeastSignificantBits());
            }

            Date BinaryUtils::ReadDate(interop::InteropInputStream * stream)
            {
                int64_t milliseconds = stream->ReadInt64();

                return Date(milliseconds);
            }

            void BinaryUtils::WriteDate(interop::InteropOutputStream* stream, const Date val)
            {
                stream->WriteInt64(val.GetMilliseconds());
            }

            Timestamp BinaryUtils::ReadTimestamp(interop::InteropInputStream* stream)
            {
                int64_t milliseconds = stream->ReadInt64();
                int32_t nanoseconds = stream->ReadInt32();

                return Timestamp(milliseconds / 1000, (milliseconds % 1000) * 1000000 + nanoseconds);
            }

            void BinaryUtils::WriteTimestamp(interop::InteropOutputStream* stream, const Timestamp val)
            {
                stream->WriteInt64(val.GetSeconds() * 1000 + val.GetSecondFraction() / 1000000);
                stream->WriteInt32(val.GetSecondFraction() % 1000000);
            }

            void BinaryUtils::WriteString(interop::InteropOutputStream* stream, const char* val, const int32_t len)
            {
                stream->WriteInt32(len);
                stream->WriteInt8Array(reinterpret_cast<const int8_t*>(val), len);
            }
        }
    }
}
