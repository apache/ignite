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

#include "ignite/impl/binary/binary_writer_impl.h"
#include "ignite/impl/interop/interop_stream_position_guard.h"

using namespace ignite::impl::interop;
using namespace ignite::impl::binary;
using namespace ignite::binary;

namespace ignite
{
    namespace impl
    {
        namespace binary
        {
            BinaryWriterImpl::BinaryWriterImpl(InteropOutputStream* stream, BinaryIdResolver* idRslvr, 
                BinaryTypeManager* metaMgr, BinaryTypeHandler* metaHnd, int32_t start) :
                stream(stream), idRslvr(idRslvr), metaMgr(metaMgr), metaHnd(metaHnd), typeId(idRslvr->GetTypeId()),
                elemIdGen(0), elemId(0), elemCnt(0), elemPos(-1), rawPos(-1), start(start)
            {
                // No-op.
            }
            
            BinaryWriterImpl::BinaryWriterImpl(InteropOutputStream* stream, BinaryTypeManager* metaMgr) :
                stream(stream), idRslvr(NULL), metaMgr(metaMgr), metaHnd(NULL), typeId(0), 
                elemIdGen(0), elemId(0), elemCnt(0), elemPos(-1), rawPos(0), start(stream->Position())
            {
                // No-op.
            }

            void BinaryWriterImpl::WriteInt8(const int8_t val)
            {
                WritePrimitiveRaw<int8_t>(val, BinaryUtils::WriteInt8);
            }

            void BinaryWriterImpl::WriteInt8Array(const int8_t* val, const int32_t len)
            {
                WritePrimitiveArrayRaw<int8_t>(val, len, BinaryUtils::WriteInt8Array, IGNITE_TYPE_ARRAY_BYTE);
            }

            void BinaryWriterImpl::WriteInt8(const char* fieldName, const int8_t val)
            {
                WritePrimitive<int8_t>(fieldName, val, BinaryUtils::WriteInt8, IGNITE_TYPE_BYTE, 1);
            }

            void BinaryWriterImpl::WriteInt8Array(const char* fieldName, const int8_t* val, const int32_t len)
            {
                WritePrimitiveArray<int8_t>(fieldName, val, len, BinaryUtils::WriteInt8Array, IGNITE_TYPE_ARRAY_BYTE, 0);
            }

            void BinaryWriterImpl::WriteBool(const bool val)
            {
                WritePrimitiveRaw<bool>(val, BinaryUtils::WriteBool);
            }

            void BinaryWriterImpl::WriteBoolArray(const bool* val, const int32_t len)
            {
                WritePrimitiveArrayRaw<bool>(val, len, BinaryUtils::WriteBoolArray, IGNITE_TYPE_ARRAY_BOOL);
            }

            void BinaryWriterImpl::WriteBool(const char* fieldName, const bool val)
            {
                WritePrimitive<bool>(fieldName, val, BinaryUtils::WriteBool, IGNITE_TYPE_BOOL, 1);
            }

            void BinaryWriterImpl::WriteBoolArray(const char* fieldName, const bool* val, const int32_t len)
            {
                WritePrimitiveArray<bool>(fieldName, val, len, BinaryUtils::WriteBoolArray, IGNITE_TYPE_ARRAY_BOOL, 0);
            }

            void BinaryWriterImpl::WriteInt16(const int16_t val)
            {
                WritePrimitiveRaw<int16_t>(val, BinaryUtils::WriteInt16);
            }

            void BinaryWriterImpl::WriteInt16Array(const int16_t* val, const int32_t len)
            {
                WritePrimitiveArrayRaw<int16_t>(val, len, BinaryUtils::WriteInt16Array, IGNITE_TYPE_ARRAY_SHORT);
            }

            void BinaryWriterImpl::WriteInt16(const char* fieldName, const int16_t val)
            {
                WritePrimitive<int16_t>(fieldName, val, BinaryUtils::WriteInt16, IGNITE_TYPE_SHORT, 2);
            }

            void BinaryWriterImpl::WriteInt16Array(const char* fieldName, const int16_t* val, const int32_t len)
            {
                WritePrimitiveArray<int16_t>(fieldName, val, len, BinaryUtils::WriteInt16Array, IGNITE_TYPE_ARRAY_SHORT, 1);
            }

            void BinaryWriterImpl::WriteUInt16(const uint16_t val)
            {
                WritePrimitiveRaw<uint16_t>(val, BinaryUtils::WriteUInt16);
            }

            void BinaryWriterImpl::WriteUInt16Array(const uint16_t* val, const int32_t len)
            {
                WritePrimitiveArrayRaw<uint16_t>(val, len, BinaryUtils::WriteUInt16Array, IGNITE_TYPE_ARRAY_CHAR);
            }

            void BinaryWriterImpl::WriteUInt16(const char* fieldName, const uint16_t val)
            {
                WritePrimitive<uint16_t>(fieldName, val, BinaryUtils::WriteUInt16, IGNITE_TYPE_CHAR, 2);
            }

            void BinaryWriterImpl::WriteUInt16Array(const char* fieldName, const uint16_t* val, const int32_t len)
            {
                WritePrimitiveArray<uint16_t>(fieldName, val, len, BinaryUtils::WriteUInt16Array, IGNITE_TYPE_ARRAY_CHAR, 1);
            }

            void BinaryWriterImpl::WriteInt32(const int32_t val)
            {
                WritePrimitiveRaw<int32_t>(val, BinaryUtils::WriteInt32);
            }

            void BinaryWriterImpl::WriteInt32Array(const int32_t* val, const int32_t len)
            {
                WritePrimitiveArrayRaw<int32_t>(val, len, BinaryUtils::WriteInt32Array, IGNITE_TYPE_ARRAY_INT);
            }

            void BinaryWriterImpl::WriteInt32(const char* fieldName, const int32_t val)
            {
                WritePrimitive<int32_t>(fieldName, val, BinaryUtils::WriteInt32, IGNITE_TYPE_INT, 4);
            }

            void BinaryWriterImpl::WriteInt32Array(const char* fieldName, const int32_t* val, const int32_t len)
            {
                WritePrimitiveArray<int32_t>(fieldName, val, len, BinaryUtils::WriteInt32Array, IGNITE_TYPE_ARRAY_INT, 2);
            }

            void BinaryWriterImpl::WriteInt64(const int64_t val)
            {
                WritePrimitiveRaw<int64_t>(val, BinaryUtils::WriteInt64);
            }

            void BinaryWriterImpl::WriteInt64Array(const int64_t* val, const int32_t len)
            {
                WritePrimitiveArrayRaw<int64_t>(val, len, BinaryUtils::WriteInt64Array, IGNITE_TYPE_ARRAY_LONG);
            }

            void BinaryWriterImpl::WriteInt64(const char* fieldName, const int64_t val)
            {
                WritePrimitive<int64_t>(fieldName, val, BinaryUtils::WriteInt64, IGNITE_TYPE_LONG, 8);
            }

            void BinaryWriterImpl::WriteInt64Array(const char* fieldName, const int64_t* val, const int32_t len)
            {
                WritePrimitiveArray<int64_t>(fieldName, val, len, BinaryUtils::WriteInt64Array, IGNITE_TYPE_ARRAY_LONG, 3);
            }

            void BinaryWriterImpl::WriteFloat(const float val)
            {
                WritePrimitiveRaw<float>(val, BinaryUtils::WriteFloat);
            }

            void BinaryWriterImpl::WriteFloatArray(const float* val, const int32_t len)
            {
                WritePrimitiveArrayRaw<float>(val, len, BinaryUtils::WriteFloatArray, IGNITE_TYPE_ARRAY_FLOAT);
            }

            void BinaryWriterImpl::WriteFloat(const char* fieldName, const float val)
            {
                WritePrimitive<float>(fieldName, val, BinaryUtils::WriteFloat, IGNITE_TYPE_FLOAT, 4);
            }

            void BinaryWriterImpl::WriteFloatArray(const char* fieldName, const float* val, const int32_t len)
            {
                WritePrimitiveArray<float>(fieldName, val, len, BinaryUtils::WriteFloatArray, IGNITE_TYPE_ARRAY_FLOAT, 2);
            }

            void BinaryWriterImpl::WriteDouble(const double val)
            {
                WritePrimitiveRaw<double>(val, BinaryUtils::WriteDouble);
            }

            void BinaryWriterImpl::WriteDoubleArray(const double* val, const int32_t len)
            {
                WritePrimitiveArrayRaw<double>(val, len, BinaryUtils::WriteDoubleArray, IGNITE_TYPE_ARRAY_DOUBLE);
            }

            void BinaryWriterImpl::WriteDouble(const char* fieldName, const double val)
            {
                WritePrimitive<double>(fieldName, val, BinaryUtils::WriteDouble, IGNITE_TYPE_DOUBLE, 8);
            }

            void BinaryWriterImpl::WriteDoubleArray(const char* fieldName, const double* val, const int32_t len)
            {
                WritePrimitiveArray<double>(fieldName, val, len, BinaryUtils::WriteDoubleArray, IGNITE_TYPE_ARRAY_DOUBLE, 3);
            }

            void BinaryWriterImpl::WriteGuid(const Guid val)
            {
                CheckRawMode(true);
                CheckSingleMode(true);

                stream->WriteInt8(IGNITE_TYPE_UUID);

                BinaryUtils::WriteGuid(stream, val);
            }

            void BinaryWriterImpl::WriteGuidArray(const Guid* val, const int32_t len)
            {
                CheckRawMode(true);
                CheckSingleMode(true);
                
                if (val)
                {
                    stream->WriteInt8(IGNITE_TYPE_ARRAY_UUID);
                    stream->WriteInt32(len);

                    for (int i = 0; i < len; i++)
                    {
                        Guid elem = *(val + i);

                        stream->WriteInt8(IGNITE_TYPE_UUID);
                        BinaryUtils::WriteGuid(stream, elem);
                    }
                }
                else
                    stream->WriteInt8(IGNITE_HDR_NULL);
            }

            void BinaryWriterImpl::WriteGuid(const char* fieldName, const Guid val)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                WriteFieldId(fieldName, IGNITE_TYPE_UUID);

                stream->WriteInt8(IGNITE_TYPE_UUID);

                BinaryUtils::WriteGuid(stream, val);
            }

            void BinaryWriterImpl::WriteGuidArray(const char* fieldName, const Guid* val, const int32_t len)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                WriteFieldId(fieldName, IGNITE_TYPE_ARRAY_UUID);

                if (val)
                {
                    stream->WriteInt8(IGNITE_TYPE_ARRAY_UUID);
                    stream->WriteInt32(len);

                    for (int i = 0; i < len; i++)
                    {
                        Guid elem = *(val + i);

                        WriteTopObject(elem);
                    }
                }
                else
                {
                    stream->WriteInt8(IGNITE_HDR_NULL);
                }
            }

            void BinaryWriterImpl::WriteDate(const Date& val)
            {
                CheckRawMode(true);
                CheckSingleMode(true);

                stream->WriteInt8(IGNITE_TYPE_DATE);

                BinaryUtils::WriteDate(stream, val);
            }

            void BinaryWriterImpl::WriteDateArray(const Date* val, const int32_t len)
            {
                CheckRawMode(true);
                CheckSingleMode(true);

                if (val)
                {
                    stream->WriteInt8(IGNITE_TYPE_ARRAY_DATE);
                    stream->WriteInt32(len);

                    for (int i = 0; i < len; i++)
                    {
                        const Date& elem = *(val + i);

                        stream->WriteInt8(IGNITE_TYPE_DATE);
                        BinaryUtils::WriteDate(stream, elem);
                    }
                }
                else
                    stream->WriteInt8(IGNITE_HDR_NULL);
            }

            void BinaryWriterImpl::WriteDate(const char* fieldName, const Date& val)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                WriteFieldId(fieldName, IGNITE_TYPE_DATE);

                stream->WriteInt8(IGNITE_TYPE_DATE);

                BinaryUtils::WriteDate(stream, val);
            }

            void BinaryWriterImpl::WriteDateArray(const char* fieldName, const Date* val, const int32_t len)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                WriteFieldId(fieldName, IGNITE_TYPE_ARRAY_DATE);

                if (val)
                {
                    stream->WriteInt8(IGNITE_TYPE_ARRAY_DATE);
                    stream->WriteInt32(len);

                    for (int i = 0; i < len; i++)
                    {
                        const Date& elem = *(val + i);

                        WriteTopObject(elem);
                    }
                }
                else
                    stream->WriteInt8(IGNITE_HDR_NULL);
            }

            void BinaryWriterImpl::WriteTimestamp(const Timestamp& val)
            {
                CheckRawMode(true);
                CheckSingleMode(true);

                stream->WriteInt8(IGNITE_TYPE_TIMESTAMP);

                BinaryUtils::WriteTimestamp(stream, val);
            }

            void BinaryWriterImpl::WriteTimestampArray(const Timestamp* val, const int32_t len)
            {
                CheckRawMode(true);
                CheckSingleMode(true);

                if (val)
                {
                    stream->WriteInt8(IGNITE_TYPE_ARRAY_TIMESTAMP);
                    stream->WriteInt32(len);

                    for (int i = 0; i < len; i++)
                    {
                        const Timestamp& elem = *(val + i);

                        stream->WriteInt8(IGNITE_TYPE_TIMESTAMP);
                        BinaryUtils::WriteTimestamp(stream, elem);
                    }
                }
                else
                    stream->WriteInt8(IGNITE_HDR_NULL);
            }

            void BinaryWriterImpl::WriteTimestamp(const char* fieldName, const Timestamp& val)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                WriteFieldId(fieldName, IGNITE_TYPE_TIMESTAMP);

                stream->WriteInt8(IGNITE_TYPE_TIMESTAMP);

                BinaryUtils::WriteTimestamp(stream, val);
            }

            void BinaryWriterImpl::WriteTimestampArray(const char* fieldName, const Timestamp* val, const int32_t len)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                WriteFieldId(fieldName, IGNITE_TYPE_ARRAY_TIMESTAMP);

                if (val)
                {
                    stream->WriteInt8(IGNITE_TYPE_ARRAY_TIMESTAMP);
                    stream->WriteInt32(len);

                    for (int i = 0; i < len; i++)
                    {
                        const Timestamp& elem = *(val + i);

                        WriteTopObject(elem);
                    }
                }
                else
                    stream->WriteInt8(IGNITE_HDR_NULL);
            }

            void BinaryWriterImpl::WriteString(const char* val, const int32_t len)
            {
                CheckRawMode(true);
                CheckSingleMode(true);

                if (val) 
                {
                    stream->WriteInt8(IGNITE_TYPE_STRING);

                    BinaryUtils::WriteString(stream, val, len);
                }
                else
                    stream->WriteInt8(IGNITE_HDR_NULL);
            }

            void BinaryWriterImpl::WriteString(const char* fieldName, const char* val, const int32_t len)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                WriteFieldId(fieldName, IGNITE_TYPE_STRING);
                
                if (val)
                {
                    stream->WriteInt8(IGNITE_TYPE_STRING);

                    BinaryUtils::WriteString(stream, val, len);
                }
                else
                    stream->WriteInt8(IGNITE_HDR_NULL);
            }

            int32_t BinaryWriterImpl::WriteStringArray()
            {
                StartContainerSession(true);

                stream->WriteInt8(IGNITE_TYPE_ARRAY_STRING);
                stream->Position(stream->Position() + 4);

                return elemId;
            }

            int32_t BinaryWriterImpl::WriteStringArray(const char* fieldName)
            {
                StartContainerSession(false);

                WriteFieldId(fieldName, IGNITE_TYPE_ARRAY_STRING);

                stream->WriteInt8(IGNITE_TYPE_ARRAY_STRING);
                stream->Position(stream->Position() + 4);

                return elemId;
            }

            void BinaryWriterImpl::WriteStringElement(int32_t id, const char* val, int32_t len)
            {
                CheckSession(id);

                if (val)
                {
                    stream->WriteInt8(IGNITE_TYPE_STRING);

                    BinaryUtils::WriteString(stream, val, len);
                }
                else
                    stream->WriteInt8(IGNITE_HDR_NULL);

                elemCnt++;
            }

            void BinaryWriterImpl::WriteNull()
            {
                CheckRawMode(true);
                CheckSingleMode(true);

                stream->WriteInt8(IGNITE_HDR_NULL);
            }

            void BinaryWriterImpl::WriteNull(const char* fieldName)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                WriteFieldId(fieldName, IGNITE_TYPE_OBJECT);
                stream->WriteInt8(IGNITE_HDR_NULL);
            }

            int32_t BinaryWriterImpl::WriteArray()
            {
                StartContainerSession(true);
                
                stream->WriteInt8(IGNITE_TYPE_ARRAY);
                stream->Position(stream->Position() + 4);

                return elemId;
            }

            int32_t BinaryWriterImpl::WriteArray(const char* fieldName)
            {
                StartContainerSession(false);

                WriteFieldId(fieldName, IGNITE_TYPE_ARRAY);

                stream->WriteInt8(IGNITE_TYPE_ARRAY);
                stream->Position(stream->Position() + 4);

                return elemId;
            }

            int32_t BinaryWriterImpl::WriteCollection(CollectionType typ)
            {
                StartContainerSession(true);

                stream->WriteInt8(IGNITE_TYPE_COLLECTION);
                stream->Position(stream->Position() + 4);
                stream->WriteInt8(typ);

                return elemId;
            }

            int32_t BinaryWriterImpl::WriteCollection(const char* fieldName, CollectionType typ)
            {
                StartContainerSession(false);
                
                WriteFieldId(fieldName, IGNITE_TYPE_COLLECTION);

                stream->WriteInt8(IGNITE_TYPE_COLLECTION);
                stream->Position(stream->Position() + 4);
                stream->WriteInt8(typ);

                return elemId;
            }

            int32_t BinaryWriterImpl::WriteMap(ignite::binary::MapType typ)
            {
                StartContainerSession(true);

                stream->WriteInt8(IGNITE_TYPE_MAP);
                stream->Position(stream->Position() + 4);
                stream->WriteInt8(typ);

                return elemId;
            }

            int32_t BinaryWriterImpl::WriteMap(const char* fieldName, ignite::binary::MapType typ)
            {
                StartContainerSession(false);

                WriteFieldId(fieldName, IGNITE_TYPE_MAP);
                
                stream->WriteInt8(IGNITE_TYPE_MAP);
                stream->Position(stream->Position() + 4);
                stream->WriteInt8(typ);

                return elemId;
            }

            void BinaryWriterImpl::CommitContainer(int32_t id)
            {
                CheckSession(id);

                stream->WriteInt32(elemPos + 1, elemCnt);

                elemId = 0;
                elemCnt = 0;
                elemPos = -1;
            }
            
            void BinaryWriterImpl::SetRawMode()
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                rawPos = stream->Position();
            }

            int32_t BinaryWriterImpl::GetRawPosition() const
            {
                return rawPos == -1 ? stream->Position() : rawPos;
            }

            void BinaryWriterImpl::CheckRawMode(bool expected) const
            {
                bool rawMode = rawPos != -1;

                if (expected && !rawMode) {
                    IGNITE_ERROR_1(IgniteError::IGNITE_ERR_BINARY, "Operation can be performed only in raw mode.");
                }
                else if (!expected && rawMode) {
                    IGNITE_ERROR_1(IgniteError::IGNITE_ERR_BINARY, "Operation cannot be performed in raw mode.");
                }
            }

            void BinaryWriterImpl::CheckSingleMode(bool expected) const
            {
                if (expected && elemId != 0) {
                    IGNITE_ERROR_1(IgniteError::IGNITE_ERR_BINARY, "Operation cannot be performed when container is being written.");
                }
                else if (!expected && elemId == 0) {
                    IGNITE_ERROR_1(IgniteError::IGNITE_ERR_BINARY, "Operation can be performed only when container is being written.");
                }
            }

            void BinaryWriterImpl::StartContainerSession(bool expRawMode)
            {
                CheckRawMode(expRawMode);
                CheckSingleMode(true);

                elemId = ++elemIdGen;
                elemPos = stream->Position();
            }

            void BinaryWriterImpl::CheckSession(int32_t expSes) const
            {
                if (elemId != expSes) 
                {
                    IGNITE_ERROR_1(IgniteError::IGNITE_ERR_BINARY, "Containter write session has been finished or is not started yet.");
                }
            }

            void BinaryWriterImpl::WriteFieldId(const char* fieldName, int32_t fieldTypeId)
            {
                int32_t fieldId = idRslvr->GetFieldId(typeId, fieldName);
                int32_t fieldOff = stream->Position() - start;

                schema.AddField(fieldId, fieldOff);

                if (metaHnd)
                    metaHnd->OnFieldWritten(fieldId, fieldName, fieldTypeId);
            }

            template <>
            void BinaryWriterImpl::WriteTopObject<int8_t>(const int8_t& obj)
            {
                WriteTopObject0<int8_t>(obj, BinaryUtils::WriteInt8, IGNITE_TYPE_BYTE);
            }

            template <>
            void BinaryWriterImpl::WriteTopObject<bool>(const bool& obj)
            {
                WriteTopObject0<bool>(obj, BinaryUtils::WriteBool, IGNITE_TYPE_BOOL);
            }

            template <>
            void BinaryWriterImpl::WriteTopObject<int16_t>(const int16_t& obj)
            {
                WriteTopObject0<int16_t>(obj, BinaryUtils::WriteInt16, IGNITE_TYPE_SHORT);
            }

            template <>
            void BinaryWriterImpl::WriteTopObject<uint16_t>(const uint16_t& obj)
            {
                WriteTopObject0<uint16_t>(obj, BinaryUtils::WriteUInt16, IGNITE_TYPE_CHAR);
            }

            template <>
            void BinaryWriterImpl::WriteTopObject<int32_t>(const int32_t& obj)
            {
                WriteTopObject0<int32_t>(obj, BinaryUtils::WriteInt32, IGNITE_TYPE_INT);
            }

            template <>
            void BinaryWriterImpl::WriteTopObject<int64_t>(const int64_t& obj)
            {
                WriteTopObject0<int64_t>(obj, BinaryUtils::WriteInt64, IGNITE_TYPE_LONG);
            }

            template <>
            void BinaryWriterImpl::WriteTopObject<float>(const float& obj)
            {
                WriteTopObject0<float>(obj, BinaryUtils::WriteFloat, IGNITE_TYPE_FLOAT);
            }

            template <>
            void BinaryWriterImpl::WriteTopObject<double>(const double& obj)
            {
                WriteTopObject0<double>(obj, BinaryUtils::WriteDouble, IGNITE_TYPE_DOUBLE);
            }

            template <>
            void BinaryWriterImpl::WriteTopObject<Guid>(const Guid& obj)
            {
                WriteTopObject0<Guid>(obj, BinaryUtils::WriteGuid, IGNITE_TYPE_UUID);
            }

            template <>
            void BinaryWriterImpl::WriteTopObject<Date>(const Date& obj)
            {
                WriteTopObject0<Date>(obj, BinaryUtils::WriteDate, IGNITE_TYPE_DATE);
            }

            template <>
            void BinaryWriterImpl::WriteTopObject<Timestamp>(const Timestamp& obj)
            {
                WriteTopObject0<Timestamp>(obj, BinaryUtils::WriteTimestamp, IGNITE_TYPE_TIMESTAMP);
            }

            void BinaryWriterImpl::PostWrite()
            {
                int32_t lenWithoutSchema = stream->Position() - start;

                uint16_t flags = IGNITE_BINARY_FLAG_USER_TYPE;

                if (rawPos > 0)
                    flags |= IGNITE_BINARY_FLAG_HAS_RAW;

                if (!HasSchema())
                {
                    stream->WriteInt16(start + IGNITE_OFFSET_FLAGS, flags);
                    stream->WriteInt32(start + IGNITE_OFFSET_LEN, lenWithoutSchema);
                    stream->WriteInt32(start + IGNITE_OFFSET_SCHEMA_ID, 0);
                    stream->WriteInt32(start + IGNITE_OFFSET_SCHEMA_OR_RAW_OFF, GetRawPosition() - start);
                }
                else
                {
                    int32_t schemaId = schema.GetId();
                    BinaryOffsetType schemaType = schema.GetType();

                    WriteAndClearSchema();

                    if (rawPos > 0)
                        stream->WriteInt32(rawPos - start);

                    int32_t length = stream->Position() - start;

                    flags |= IGNITE_BINARY_FLAG_HAS_SCHEMA;

                    if (schemaType == OFFSET_TYPE_ONE_BYTE)
                        flags |= IGNITE_BINARY_FLAG_OFFSET_ONE_BYTE;
                    else if (schemaType == OFFSET_TYPE_TWO_BYTES)
                        flags |= IGNITE_BINARY_FLAG_OFFSET_TWO_BYTES;

                    stream->WriteInt16(start + IGNITE_OFFSET_FLAGS, flags);
                    stream->WriteInt32(start + IGNITE_OFFSET_LEN, length);
                    stream->WriteInt32(start + IGNITE_OFFSET_SCHEMA_ID, schemaId);
                    stream->WriteInt32(start + IGNITE_OFFSET_SCHEMA_OR_RAW_OFF, lenWithoutSchema);
                }
            }

            bool BinaryWriterImpl::HasSchema() const
            {
                return !schema.Empty();
            }

            void BinaryWriterImpl::WriteAndClearSchema()
            {
                schema.Write(*stream);

                schema.Clear();
            }

            InteropOutputStream* BinaryWriterImpl::GetStream()
            {
                return stream;
            }
        }
    }
}