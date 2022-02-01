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

#ifndef _IGNITE_BINARY_TEST_UTILS
#define _IGNITE_BINARY_TEST_UTILS

#include "ignite/binary/binary.h"

using namespace ignite;
using namespace ignite::binary;
using namespace ignite::impl::binary;
using namespace ignite::impl::interop;

namespace ignite_test
{
    namespace core
    {
        namespace binary
        {
            inline bool IsBinaryError(const IgniteError& err)
            {
                return err.GetCode() == IgniteError::IGNITE_ERR_BINARY;
            }

            inline bool IsStreamPositionEqualOnSkip(InteropInputStream& in, int32_t prevPos = 0)
            {
                int32_t pos = in.Position();

                BinaryReaderImpl reader(&in);

                in.Position(prevPos);
                reader.Skip();
                int32_t skipPos = in.Position();

                in.Position(pos);

                return skipPos == pos;
            }

            template<typename T>
            inline void Write(BinaryRawWriter&, T)
            {
                throw std::runtime_error("Function is not defined");
            }

            template<typename T>
            inline T Read(BinaryRawReader&)
            {
                throw std::runtime_error("Function is not defined");
            }

            template<>
            inline void Write(BinaryRawWriter& writer, int8_t val)
            {
                writer.WriteInt8(val);
            }

            template<>
            inline int8_t Read(BinaryRawReader& reader)
            {
                return reader.ReadInt8();
            }

            template<>
            inline void Write(BinaryRawWriter& writer, bool val)
            {
                writer.WriteBool(val);
            }

            template<>
            inline bool Read(BinaryRawReader& reader)
            {
                return reader.ReadBool();
            }

            template<>
            inline void Write(BinaryRawWriter& writer, int16_t val)
            {
                writer.WriteInt16(val);
            }

            template<>
            inline int16_t Read(BinaryRawReader& reader)
            {
                return reader.ReadInt16();
            }

            template<>
            inline void Write(BinaryRawWriter& writer, uint16_t val)
            {
                writer.WriteUInt16(val);
            }

            template<>
            inline uint16_t Read(BinaryRawReader& reader)
            {
                return reader.ReadUInt16();
            }

            template<>
            inline void Write(BinaryRawWriter& writer, int32_t val)
            {
                writer.WriteInt32(val);
            }

            template<>
            inline int32_t Read(BinaryRawReader& reader)
            {
                return reader.ReadInt32();
            }

            template<>
            inline void Write(BinaryRawWriter& writer, int64_t val)
            {
                writer.WriteInt64(val);
            }

            template<>
            inline int64_t Read(BinaryRawReader& reader)
            {
                return reader.ReadInt64();
            }

            template<>
            inline void Write(BinaryRawWriter& writer, float val)
            {
                writer.WriteFloat(val);
            }

            template<>
            inline float Read(BinaryRawReader& reader)
            {
                return reader.ReadFloat();
            }

            template<>
            inline void Write(BinaryRawWriter& writer, double val)
            {
                writer.WriteDouble(val);
            }

            template<>
            inline double Read(BinaryRawReader& reader)
            {
                return reader.ReadDouble();
            }

            template<>
            inline void Write(BinaryRawWriter& writer, Guid val)
            {
                writer.WriteGuid(val);
            }

            template<>
            inline Guid Read(BinaryRawReader& reader)
            {
                return reader.ReadGuid();
            }

            template<>
            inline void Write(BinaryRawWriter& writer, Date val)
            {
                writer.WriteDate(val);
            }

            template<>
            inline Date Read(BinaryRawReader& reader)
            {
                return reader.ReadDate();
            }

            template<>
            inline void Write(BinaryRawWriter& writer, Time val)
            {
                writer.WriteTime(val);
            }

            template<>
            inline Time Read(BinaryRawReader& reader)
            {
                return reader.ReadTime();
            }

            template<>
            inline void Write(BinaryRawWriter& writer, Timestamp val)
            {
                writer.WriteTimestamp(val);
            }

            template<>
            inline Timestamp Read(BinaryRawReader& reader)
            {
                return reader.ReadTimestamp();
            }

            template<typename T>
            inline void WriteArray(BinaryRawWriter&, T*, int32_t)
            {
                throw std::runtime_error("Function is not defined");
            }

            template<typename T>
            inline int32_t ReadArray(BinaryRawReader&, T*, int32_t)
            {
                throw std::runtime_error("Function is not defined");
            }

            template<>
            inline void WriteArray(BinaryRawWriter& writer, int8_t* val, int32_t len)
            {
                writer.WriteInt8Array(val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryRawReader& reader, int8_t* val, int32_t len)
            {
                return reader.ReadInt8Array(val, len);
            }

            template<>
            inline void WriteArray(BinaryRawWriter& writer, bool* val, int32_t len)
            {
                writer.WriteBoolArray(val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryRawReader& reader, bool* val, int32_t len)
            {
                return reader.ReadBoolArray(val, len);
            }

            template<>
            inline void WriteArray(BinaryRawWriter& writer, int16_t* val, int32_t len)
            {
                writer.WriteInt16Array(val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryRawReader& reader, int16_t* val, int32_t len)
            {
                return reader.ReadInt16Array(val, len);
            }

            template<>
            inline void WriteArray(BinaryRawWriter& writer, uint16_t* val, int32_t len)
            {
                writer.WriteUInt16Array(val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryRawReader& reader, uint16_t* val, int32_t len)
            {
                return reader.ReadUInt16Array(val, len);
            }

            template<>
            inline void WriteArray(BinaryRawWriter& writer, int32_t* val, int32_t len)
            {
                writer.WriteInt32Array(val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryRawReader& reader, int32_t* val, int32_t len)
            {
                return reader.ReadInt32Array(val, len);
            }

            template<>
            inline void WriteArray(BinaryRawWriter& writer, int64_t* val, int32_t len)
            {
                writer.WriteInt64Array(val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryRawReader& reader, int64_t* val, int32_t len)
            {
                return reader.ReadInt64Array(val, len);
            }

            template<>
            inline void WriteArray(BinaryRawWriter& writer, float* val, int32_t len)
            {
                writer.WriteFloatArray(val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryRawReader& reader, float* val, int32_t len)
            {
                return reader.ReadFloatArray(val, len);
            }

            template<>
            inline void WriteArray(BinaryRawWriter& writer, double* val, int32_t len)
            {
                writer.WriteDoubleArray(val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryRawReader& reader, double* val, int32_t len)
            {
                return reader.ReadDoubleArray(val, len);
            }

            template<>
            inline void WriteArray(BinaryRawWriter& writer, Guid* val, int32_t len)
            {
                writer.WriteGuidArray(val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryRawReader& reader, Guid* val, int32_t len)
            {
                return reader.ReadGuidArray(val, len);
            }

            template<>
            inline void WriteArray(BinaryRawWriter& writer, Date* val, int32_t len)
            {
                writer.WriteDateArray(val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryRawReader& reader, Date* val, int32_t len)
            {
                return reader.ReadDateArray(val, len);
            }

            template<>
            inline void WriteArray(BinaryRawWriter& writer, Time* val, int32_t len)
            {
                writer.WriteTimeArray(val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryRawReader& reader, Time* val, int32_t len)
            {
                return reader.ReadTimeArray(val, len);
            }

            template<>
            inline void WriteArray(BinaryRawWriter& writer, Timestamp* val, int32_t len)
            {
                writer.WriteTimestampArray(val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryRawReader& reader, Timestamp* val, int32_t len)
            {
                return reader.ReadTimestampArray(val, len);
            }

            template<typename T>
            inline void Write(BinaryWriter&, const char*, T)
            {
                throw std::runtime_error("Function is not defined");
            }

            template<typename T>
            inline T Read(BinaryReader&, const char*)
            {
                throw std::runtime_error("Function is not defined");
            }

            template<>
            inline void Write(BinaryWriter& writer, const char* fieldName, int8_t val)
            {
                writer.WriteInt8(fieldName, val);
            }

            template<>
            inline int8_t Read(BinaryReader& reader, const char* fieldName)
            {
                return reader.ReadInt8(fieldName);
            }

            template<>
            inline void Write(BinaryWriter& writer, const char* fieldName, bool val)
            {
                writer.WriteBool(fieldName, val);
            }

            template<>
            inline bool Read(BinaryReader& reader, const char* fieldName)
            {
                return reader.ReadBool(fieldName);
            }

            template<>
            inline void Write(BinaryWriter& writer, const char* fieldName, int16_t val)
            {
                writer.WriteInt16(fieldName, val);
            }

            template<>
            inline int16_t Read(BinaryReader& reader, const char* fieldName)
            {
                return reader.ReadInt16(fieldName);
            }

            template<>
            inline void Write(BinaryWriter& writer, const char* fieldName, uint16_t val)
            {
                writer.WriteUInt16(fieldName, val);
            }

            template<>
            inline uint16_t Read(BinaryReader& reader, const char* fieldName)
            {
                return reader.ReadUInt16(fieldName);
            }

            template<>
            inline void Write(BinaryWriter& writer, const char* fieldName, int32_t val)
            {
                writer.WriteInt32(fieldName, val);
            }

            template<>
            inline int32_t Read(BinaryReader& reader, const char* fieldName)
            {
                return reader.ReadInt32(fieldName);
            }

            template<>
            inline void Write(BinaryWriter& writer, const char* fieldName, int64_t val)
            {
                writer.WriteInt64(fieldName, val);
            }

            template<>
            inline int64_t Read(BinaryReader& reader, const char* fieldName)
            {
                return reader.ReadInt64(fieldName);
            }

            template<>
            inline void Write(BinaryWriter& writer, const char* fieldName, float val)
            {
                writer.WriteFloat(fieldName, val);
            }

            template<>
            inline float Read(BinaryReader& reader, const char* fieldName)
            {
                return reader.ReadFloat(fieldName);
            }

            template<>
            inline void Write(BinaryWriter& writer, const char* fieldName, double val)
            {
                writer.WriteDouble(fieldName, val);
            }

            template<>
            inline double Read(BinaryReader& reader, const char* fieldName)
            {
                return reader.ReadDouble(fieldName);
            }

            template<>
            inline void Write(BinaryWriter& writer, const char* fieldName, Guid val)
            {
                writer.WriteGuid(fieldName, val);
            }

            template<>
            inline Guid Read(BinaryReader& reader, const char* fieldName)
            {
                return reader.ReadGuid(fieldName);
            }

            template<>
            inline void Write(BinaryWriter& writer, const char* fieldName, Date val)
            {
                writer.WriteDate(fieldName, val);
            }

            template<>
            inline Date Read(BinaryReader& reader, const char* fieldName)
            {
                return reader.ReadDate(fieldName);
            }

            template<>
            inline void Write(BinaryWriter& writer, const char* fieldName, Time val)
            {
                writer.WriteTime(fieldName, val);
            }

            template<>
            inline Time Read(BinaryReader& reader, const char* fieldName)
            {
                return reader.ReadTime(fieldName);
            }

            template<>
            inline void Write(BinaryWriter& writer, const char* fieldName, Timestamp val)
            {
                writer.WriteTimestamp(fieldName, val);
            }

            template<>
            inline Timestamp Read(BinaryReader& reader, const char* fieldName)
            {
                return reader.ReadTimestamp(fieldName);
            }

            template<typename T>
            inline void WriteArray(BinaryWriter&, const char*, T*, int32_t)
            {
                throw std::runtime_error("Function is not defined");
            }

            template<typename T>
            inline int32_t ReadArray(BinaryReader&, const char*, T*, int32_t)
            {
                throw std::runtime_error("Function is not defined");
            }

            template<>
            inline void WriteArray(BinaryWriter& writer, const char* fieldName, int8_t* val, int32_t len)
            {
                writer.WriteInt8Array(fieldName, val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryReader& reader, const char* fieldName, int8_t* val, int32_t len)
            {
                return reader.ReadInt8Array(fieldName, val, len);
            }

            template<>
            inline void WriteArray(BinaryWriter& writer, const char* fieldName, bool* val, int32_t len)
            {
                writer.WriteBoolArray(fieldName, val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryReader& reader, const char* fieldName, bool* val, int32_t len)
            {
                return reader.ReadBoolArray(fieldName, val, len);
            }

            template<>
            inline void WriteArray(BinaryWriter& writer, const char* fieldName, int16_t* val, int32_t len)
            {
                writer.WriteInt16Array(fieldName, val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryReader& reader, const char* fieldName, int16_t* val, int32_t len)
            {
                return reader.ReadInt16Array(fieldName, val, len);
            }

            template<>
            inline void WriteArray(BinaryWriter& writer, const char* fieldName, uint16_t* val, int32_t len)
            {
                writer.WriteUInt16Array(fieldName, val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryReader& reader, const char* fieldName, uint16_t* val, int32_t len)
            {
                return reader.ReadUInt16Array(fieldName, val, len);
            }

            template<>
            inline void WriteArray(BinaryWriter& writer, const char* fieldName, int32_t* val, int32_t len)
            {
                writer.WriteInt32Array(fieldName, val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryReader& reader, const char* fieldName, int32_t* val, int32_t len)
            {
                return reader.ReadInt32Array(fieldName, val, len);
            }

            template<>
            inline void WriteArray(BinaryWriter& writer, const char* fieldName, int64_t* val, int32_t len)
            {
                writer.WriteInt64Array(fieldName, val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryReader& reader, const char* fieldName, int64_t* val, int32_t len)
            {
                return reader.ReadInt64Array(fieldName, val, len);
            }

            template<>
            inline void WriteArray(BinaryWriter& writer, const char* fieldName, float* val, int32_t len)
            {
                writer.WriteFloatArray(fieldName, val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryReader& reader, const char* fieldName, float* val, int32_t len)
            {
                return reader.ReadFloatArray(fieldName, val, len);
            }

            template<>
            inline void WriteArray(BinaryWriter& writer, const char* fieldName, double* val, int32_t len)
            {
                writer.WriteDoubleArray(fieldName, val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryReader& reader, const char* fieldName, double* val, int32_t len)
            {
                return reader.ReadDoubleArray(fieldName, val, len);
            }

            template<>
            inline void WriteArray(BinaryWriter& writer, const char* fieldName, Guid* val, int32_t len)
            {
                writer.WriteGuidArray(fieldName, val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryReader& reader, const char* fieldName, Guid* val, int32_t len)
            {
                return reader.ReadGuidArray(fieldName, val, len);
            }

            template<>
            inline void WriteArray(BinaryWriter& writer, const char* fieldName, Date* val, int32_t len)
            {
                writer.WriteDateArray(fieldName, val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryReader& reader, const char* fieldName, Date* val, int32_t len)
            {
                return reader.ReadDateArray(fieldName, val, len);
            }

            template<>
            inline void WriteArray(BinaryWriter& writer, const char* fieldName, Time* val, int32_t len)
            {
                writer.WriteTimeArray(fieldName, val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryReader& reader, const char* fieldName, Time* val, int32_t len)
            {
                return reader.ReadTimeArray(fieldName, val, len);
            }

            template<>
            inline void WriteArray(BinaryWriter& writer, const char* fieldName, Timestamp* val, int32_t len)
            {
                writer.WriteTimestampArray(fieldName, val, len);
            }

            template<>
            inline int32_t ReadArray(BinaryReader& reader, const char* fieldName, Timestamp* val, int32_t len)
            {
                return reader.ReadTimestampArray(fieldName, val, len);
            }
        }
    }
}

#endif
