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

#include "ignite/impl/interop/interop.h"
#include "ignite/impl/portable/portable_utils.h"

using namespace ignite::impl::interop;
using namespace ignite::impl::portable;

namespace ignite
{
    namespace impl
    {
        namespace portable
        {
            int8_t PortableUtils::ReadInt8(InteropInputStream* stream)
            {
                return stream->ReadInt8();
            }

            void PortableUtils::WriteInt8(InteropOutputStream* stream, int8_t val)
            {
                stream->WriteInt8(val); 
            }

            void PortableUtils::ReadInt8Array(InteropInputStream* stream, int8_t* res, const int32_t len)
            {
                stream->ReadInt8Array(res, len);
            }

            void PortableUtils::WriteInt8Array(InteropOutputStream* stream, const int8_t* val, const int32_t len)
            {
                stream->WriteInt8Array(val, len);
            }

            bool PortableUtils::ReadBool(InteropInputStream* stream)
            {
                return stream->ReadBool();
            }

            void PortableUtils::WriteBool(InteropOutputStream* stream, bool val)
            {
                stream->WriteBool(val);
            }

            void PortableUtils::ReadBoolArray(InteropInputStream* stream, bool* res, const int32_t len)
            {
                stream->ReadBoolArray(res, len);
            }

            void PortableUtils::WriteBoolArray(InteropOutputStream* stream, const bool* val, const int32_t len)
            {
                stream->WriteBoolArray(val, len);
            }

            int16_t PortableUtils::ReadInt16(InteropInputStream* stream)
            {
                return stream->ReadInt16();
            }

            void PortableUtils::WriteInt16(InteropOutputStream* stream, int16_t val)
            {
                stream->WriteInt16(val);
            }

            void PortableUtils::ReadInt16Array(InteropInputStream* stream, int16_t* res, const int32_t len)
            {
                stream->ReadInt16Array(res, len);
            }
            
            void PortableUtils::WriteInt16Array(InteropOutputStream* stream, const int16_t* val, const int32_t len)
            {
                stream->WriteInt16Array(val, len);
            }

            uint16_t PortableUtils::ReadUInt16(InteropInputStream* stream)
            {
                return stream->ReadUInt16();
            }

            void PortableUtils::WriteUInt16(InteropOutputStream* stream, uint16_t val)
            {
                stream->WriteUInt16(val);
            }

            void PortableUtils::ReadUInt16Array(InteropInputStream* stream, uint16_t* res, const int32_t len)
            {
                stream->ReadUInt16Array(res, len);
            }

            void PortableUtils::WriteUInt16Array(InteropOutputStream* stream, const uint16_t* val, const int32_t len)
            {
                stream->WriteUInt16Array(val, len);
            }

            int32_t PortableUtils::ReadInt32(InteropInputStream* stream)
            {
                return stream->ReadInt32();
            }

            void PortableUtils::WriteInt32(InteropOutputStream* stream, int32_t val)
            {
                stream->WriteInt32(val);
            }

            void PortableUtils::ReadInt32Array(InteropInputStream* stream, int32_t* res, const int32_t len)
            {
                stream->ReadInt32Array(res, len);
            }

            void PortableUtils::WriteInt32Array(InteropOutputStream* stream, const int32_t* val, const int32_t len)
            {
                stream->WriteInt32Array(val, len);
            }

            int64_t PortableUtils::ReadInt64(InteropInputStream* stream)
            {
                return stream->ReadInt64();
            }

            void PortableUtils::WriteInt64(InteropOutputStream* stream, int64_t val)
            {
                stream->WriteInt64(val);
            }

            void PortableUtils::ReadInt64Array(InteropInputStream* stream, int64_t* res, const int32_t len)
            {
                stream->ReadInt64Array(res, len);
            }

            void PortableUtils::WriteInt64Array(InteropOutputStream* stream, const int64_t* val, const int32_t len)
            {
                stream->WriteInt64Array(val, len);
            }

            float PortableUtils::ReadFloat(InteropInputStream* stream)
            {
                return stream->ReadFloat();
            }

            void PortableUtils::WriteFloat(InteropOutputStream* stream, float val)
            {
                stream->WriteFloat(val);
            }

            void PortableUtils::ReadFloatArray(InteropInputStream* stream, float* res, const int32_t len)
            {
                stream->ReadFloatArray(res, len);
            }

            void PortableUtils::WriteFloatArray(InteropOutputStream* stream, const float* val, const int32_t len)
            {
                stream->WriteFloatArray(val, len);
            }

            double PortableUtils::ReadDouble(InteropInputStream* stream)
            {
                return stream->ReadDouble();
            }

            void PortableUtils::WriteDouble(InteropOutputStream* stream, double val)
            {
                stream->WriteDouble(val);
            }

            void PortableUtils::ReadDoubleArray(InteropInputStream* stream, double* res, const int32_t len)
            {
                stream->ReadDoubleArray(res, len);
            }

            void PortableUtils::WriteDoubleArray(InteropOutputStream* stream, const double* val, const int32_t len)
            {
                stream->WriteDoubleArray(val, len);
            }

            Guid PortableUtils::ReadGuid(interop::InteropInputStream* stream)
            {
                int64_t most = stream->ReadInt64();
                int64_t least = stream->ReadInt64();

                return Guid(most, least);
            }

            void PortableUtils::WriteGuid(interop::InteropOutputStream* stream, const Guid val)
            {
                stream->WriteInt64(val.GetMostSignificantBits());
                stream->WriteInt64(val.GetLeastSignificantBits());
            }

            void PortableUtils::WriteString(interop::InteropOutputStream* stream, const char* val, const int32_t len)
            {
                stream->WriteBool(false);
                stream->WriteInt32(len);

                for (int i = 0; i < len; i++)
                    stream->WriteUInt16(*(val + i));
            }
        }
    }
}