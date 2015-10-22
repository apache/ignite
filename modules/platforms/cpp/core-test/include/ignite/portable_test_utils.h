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

#ifndef _IGNITE_PORTABLE_TEST_UTILS
#define _IGNITE_PORTABLE_TEST_UTILS

#include "ignite/portable/portable.h"

using namespace ignite;
using namespace ignite::portable;
using namespace ignite::impl::portable;

namespace ignite_test
{
    namespace core
    {
        namespace portable
        {
            template<typename T>
            inline void Write(PortableRawWriter& writer, T val)
            {
                throw std::runtime_error("Function is not defined");
            }

            template<typename T>
            inline T Read(PortableRawReader& reader)
            {
                throw std::runtime_error("Function is not defined");
            }

            template<>
            inline void Write(PortableRawWriter& writer, int8_t val)
            {
                writer.WriteInt8(val);
            }

            template<>
            inline int8_t Read(PortableRawReader& reader)
            {
                return reader.ReadInt8();
            }

            template<>
            inline void Write(PortableRawWriter& writer, bool val)
            {
                writer.WriteBool(val);
            }

            template<>
            inline bool Read(PortableRawReader& reader)
            {
                return reader.ReadBool();
            }

            template<>
            inline void Write(PortableRawWriter& writer, int16_t val)
            {
                writer.WriteInt16(val);
            }

            template<>
            inline int16_t Read(PortableRawReader& reader)
            {
                return reader.ReadInt16();
            }

            template<>
            inline void Write(PortableRawWriter& writer, uint16_t val)
            {
                writer.WriteUInt16(val);
            }

            template<>
            inline uint16_t Read(PortableRawReader& reader)
            {
                return reader.ReadUInt16();
            }

            template<>
            inline void Write(PortableRawWriter& writer, int32_t val)
            {
                writer.WriteInt32(val);
            }

            template<>
            inline int32_t Read(PortableRawReader& reader)
            {
                return reader.ReadInt32();
            }

            template<>
            inline void Write(PortableRawWriter& writer, int64_t val)
            {
                writer.WriteInt64(val);
            }

            template<>
            inline int64_t Read(PortableRawReader& reader)
            {
                return reader.ReadInt64();
            }

            template<>
            inline void Write(PortableRawWriter& writer, float val)
            {
                writer.WriteFloat(val);
            }

            template<>
            inline float Read(PortableRawReader& reader)
            {
                return reader.ReadFloat();
            }

            template<>
            inline void Write(PortableRawWriter& writer, double val)
            {
                writer.WriteDouble(val);
            }

            template<>
            inline double Read(PortableRawReader& reader)
            {
                return reader.ReadDouble();
            }

            template<>
            inline void Write(PortableRawWriter& writer, Guid val)
            {
                writer.WriteGuid(val);
            }

            template<>
            inline Guid Read(PortableRawReader& reader)
            {
                return reader.ReadGuid();
            }

            template<typename T>
            inline void WriteArray(PortableRawWriter& writer, T* val, int32_t len)
            {
                throw std::runtime_error("Function is not defined");
            }

            template<typename T>
            inline int32_t ReadArray(PortableRawReader& reader, T* val, int32_t len)
            {
                throw std::runtime_error("Function is not defined");
            }

            template<>
            inline void WriteArray(PortableRawWriter& writer, int8_t* val, int32_t len)
            {
                writer.WriteInt8Array(val, len);
            }

            template<>
            inline int32_t ReadArray(PortableRawReader& reader, int8_t* val, int32_t len)
            {
                return reader.ReadInt8Array(val, len);
            }

            template<>
            inline void WriteArray(PortableRawWriter& writer, bool* val, int32_t len)
            {
                writer.WriteBoolArray(val, len);
            }

            template<>
            inline int32_t ReadArray(PortableRawReader& reader, bool* val, int32_t len)
            {
                return reader.ReadBoolArray(val, len);
            }

            template<>
            inline void WriteArray(PortableRawWriter& writer, int16_t* val, int32_t len)
            {
                writer.WriteInt16Array(val, len);
            }

            template<>
            inline int32_t ReadArray(PortableRawReader& reader, int16_t* val, int32_t len)
            {
                return reader.ReadInt16Array(val, len);
            }

            template<>
            inline void WriteArray(PortableRawWriter& writer, uint16_t* val, int32_t len)
            {
                writer.WriteUInt16Array(val, len);
            }

            template<>
            inline int32_t ReadArray(PortableRawReader& reader, uint16_t* val, int32_t len)
            {
                return reader.ReadUInt16Array(val, len);
            }

            template<>
            inline void WriteArray(PortableRawWriter& writer, int32_t* val, int32_t len)
            {
                writer.WriteInt32Array(val, len);
            }

            template<>
            inline int32_t ReadArray(PortableRawReader& reader, int32_t* val, int32_t len)
            {
                return reader.ReadInt32Array(val, len);
            }

            template<>
            inline void WriteArray(PortableRawWriter& writer, int64_t* val, int32_t len)
            {
                writer.WriteInt64Array(val, len);
            }

            template<>
            inline int32_t ReadArray(PortableRawReader& reader, int64_t* val, int32_t len)
            {
                return reader.ReadInt64Array(val, len);
            }

            template<>
            inline void WriteArray(PortableRawWriter& writer, float* val, int32_t len)
            {
                writer.WriteFloatArray(val, len);
            }

            template<>
            inline int32_t ReadArray(PortableRawReader& reader, float* val, int32_t len)
            {
                return reader.ReadFloatArray(val, len);
            }

            template<>
            inline void WriteArray(PortableRawWriter& writer, double* val, int32_t len)
            {
                writer.WriteDoubleArray(val, len);
            }

            template<>
            inline int32_t ReadArray(PortableRawReader& reader, double* val, int32_t len)
            {
                return reader.ReadDoubleArray(val, len);
            }

            template<>
            inline void WriteArray(PortableRawWriter& writer, Guid* val, int32_t len)
            {
                writer.WriteGuidArray(val, len);
            }

            template<>
            inline int32_t ReadArray(PortableRawReader& reader, Guid* val, int32_t len)
            {
                return reader.ReadGuidArray(val, len);
            }

            template<typename T>
            inline void Write(PortableWriter& writer, const char* fieldName, T val)
            {
                throw std::runtime_error("Function is not defined");
            }

            template<typename T>
            inline T Read(PortableReader& reader, const char* fieldName)
            {
                throw std::runtime_error("Function is not defined");
            }

            template<>
            inline void Write(PortableWriter& writer, const char* fieldName, int8_t val)
            {
                writer.WriteInt8(fieldName, val);
            }

            template<>
            inline int8_t Read(PortableReader& reader, const char* fieldName)
            {
                return reader.ReadInt8(fieldName);
            }

            template<>
            inline void Write(PortableWriter& writer, const char* fieldName, bool val)
            {
                writer.WriteBool(fieldName, val);
            }

            template<>
            inline bool Read(PortableReader& reader, const char* fieldName)
            {
                return reader.ReadBool(fieldName);
            }

            template<>
            inline void Write(PortableWriter& writer, const char* fieldName, int16_t val)
            {
                writer.WriteInt16(fieldName, val);
            }

            template<>
            inline int16_t Read(PortableReader& reader, const char* fieldName)
            {
                return reader.ReadInt16(fieldName);
            }

            template<>
            inline void Write(PortableWriter& writer, const char* fieldName, uint16_t val)
            {
                writer.WriteUInt16(fieldName, val);
            }

            template<>
            inline uint16_t Read(PortableReader& reader, const char* fieldName)
            {
                return reader.ReadUInt16(fieldName);
            }

            template<>
            inline void Write(PortableWriter& writer, const char* fieldName, int32_t val)
            {
                writer.WriteInt32(fieldName, val);
            }

            template<>
            inline int32_t Read(PortableReader& reader, const char* fieldName)
            {
                return reader.ReadInt32(fieldName);
            }

            template<>
            inline void Write(PortableWriter& writer, const char* fieldName, int64_t val)
            {
                writer.WriteInt64(fieldName, val);
            }

            template<>
            inline int64_t Read(PortableReader& reader, const char* fieldName)
            {
                return reader.ReadInt64(fieldName);
            }

            template<>
            inline void Write(PortableWriter& writer, const char* fieldName, float val)
            {
                writer.WriteFloat(fieldName, val);
            }

            template<>
            inline float Read(PortableReader& reader, const char* fieldName)
            {
                return reader.ReadFloat(fieldName);
            }

            template<>
            inline void Write(PortableWriter& writer, const char* fieldName, double val)
            {
                writer.WriteDouble(fieldName, val);
            }

            template<>
            inline double Read(PortableReader& reader, const char* fieldName)
            {
                return reader.ReadDouble(fieldName);
            }

            template<>
            inline void Write(PortableWriter& writer, const char* fieldName, Guid val)
            {
                writer.WriteGuid(fieldName, val);
            }

            template<>
            inline Guid Read(PortableReader& reader, const char* fieldName)
            {
                return reader.ReadGuid(fieldName);
            }

            template<typename T>
            inline void WriteArray(PortableWriter& writer, const char* fieldName, T* val, int32_t len)
            {
                throw std::runtime_error("Function is not defined");
            }

            template<typename T>
            inline int32_t ReadArray(PortableReader& reader, const char* fieldName, T* val, int32_t len)
            {
                throw std::runtime_error("Function is not defined");
            }

            template<>
            inline void WriteArray(PortableWriter& writer, const char* fieldName, int8_t* val, int32_t len)
            {
                writer.WriteInt8Array(fieldName, val, len);
            }

            template<>
            inline int32_t ReadArray(PortableReader& reader, const char* fieldName, int8_t* val, int32_t len)
            {
                return reader.ReadInt8Array(fieldName, val, len);
            }

            template<>
            inline void WriteArray(PortableWriter& writer, const char* fieldName, bool* val, int32_t len)
            {
                writer.WriteBoolArray(fieldName, val, len);
            }

            template<>
            inline int32_t ReadArray(PortableReader& reader, const char* fieldName, bool* val, int32_t len)
            {
                return reader.ReadBoolArray(fieldName, val, len);
            }

            template<>
            inline void WriteArray(PortableWriter& writer, const char* fieldName, int16_t* val, int32_t len)
            {
                writer.WriteInt16Array(fieldName, val, len);
            }

            template<>
            inline int32_t ReadArray(PortableReader& reader, const char* fieldName, int16_t* val, int32_t len)
            {
                return reader.ReadInt16Array(fieldName, val, len);
            }

            template<>
            inline void WriteArray(PortableWriter& writer, const char* fieldName, uint16_t* val, int32_t len)
            {
                writer.WriteUInt16Array(fieldName, val, len);
            }

            template<>
            inline int32_t ReadArray(PortableReader& reader, const char* fieldName, uint16_t* val, int32_t len)
            {
                return reader.ReadUInt16Array(fieldName, val, len);
            }

            template<>
            inline void WriteArray(PortableWriter& writer, const char* fieldName, int32_t* val, int32_t len)
            {
                writer.WriteInt32Array(fieldName, val, len);
            }

            template<>
            inline int32_t ReadArray(PortableReader& reader, const char* fieldName, int32_t* val, int32_t len)
            {
                return reader.ReadInt32Array(fieldName, val, len);
            }

            template<>
            inline void WriteArray(PortableWriter& writer, const char* fieldName, int64_t* val, int32_t len)
            {
                writer.WriteInt64Array(fieldName, val, len);
            }

            template<>
            inline int32_t ReadArray(PortableReader& reader, const char* fieldName, int64_t* val, int32_t len)
            {
                return reader.ReadInt64Array(fieldName, val, len);
            }

            template<>
            inline void WriteArray(PortableWriter& writer, const char* fieldName, float* val, int32_t len)
            {
                writer.WriteFloatArray(fieldName, val, len);
            }

            template<>
            inline int32_t ReadArray(PortableReader& reader, const char* fieldName, float* val, int32_t len)
            {
                return reader.ReadFloatArray(fieldName, val, len);
            }

            template<>
            inline void WriteArray(PortableWriter& writer, const char* fieldName, double* val, int32_t len)
            {
                writer.WriteDoubleArray(fieldName, val, len);
            }

            template<>
            inline int32_t ReadArray(PortableReader& reader, const char* fieldName, double* val, int32_t len)
            {
                return reader.ReadDoubleArray(fieldName, val, len);
            }

            template<>
            inline void WriteArray(PortableWriter& writer, const char* fieldName, Guid* val, int32_t len)
            {
                writer.WriteGuidArray(fieldName, val, len);
            }

            template<>
            inline int32_t ReadArray(PortableReader& reader, const char* fieldName, Guid* val, int32_t len)
            {
                return reader.ReadGuidArray(fieldName, val, len);
            }
        }
    }
}

#endif