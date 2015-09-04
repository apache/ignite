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

#include "ignite/impl/portable/portable_writer_impl.h"
#include "ignite/ignite_error.h"

using namespace ignite::impl::interop;
using namespace ignite::impl::portable;
using namespace ignite::portable;

namespace ignite
{
    namespace impl
    {
        namespace portable
        {
            PortableWriterImpl::PortableWriterImpl(InteropOutputStream* stream, PortableIdResolver* idRslvr, 
                PortableMetadataManager* metaMgr, PortableMetadataHandler* metaHnd) :
                stream(stream), idRslvr(idRslvr), metaMgr(metaMgr), metaHnd(metaHnd), typeId(idRslvr->GetTypeId()),
                elemIdGen(0), elemId(0), elemCnt(0), elemPos(-1), rawPos(-1)
            {
                // No-op.
            }
            
            PortableWriterImpl::PortableWriterImpl(InteropOutputStream* stream, PortableMetadataManager* metaMgr) :
                stream(stream), idRslvr(NULL), metaMgr(metaMgr), metaHnd(NULL), typeId(0), 
                elemIdGen(0), elemId(0), elemCnt(0), elemPos(-1), rawPos(0)
            {
                // No-op.
            }

            void PortableWriterImpl::WriteInt8(const int8_t val)
            {
                WritePrimitiveRaw<int8_t>(val, PortableUtils::WriteInt8);
            }

            void PortableWriterImpl::WriteInt8Array(const int8_t* val, const int32_t len)
            {
                WritePrimitiveArrayRaw<int8_t>(val, len, PortableUtils::WriteInt8Array, IGNITE_TYPE_ARRAY_BYTE);
            }

            void PortableWriterImpl::WriteInt8(const char* fieldName, const int8_t val)
            {
                WritePrimitive<int8_t>(fieldName, val, PortableUtils::WriteInt8, IGNITE_TYPE_BYTE, 1);
            }

            void PortableWriterImpl::WriteInt8Array(const char* fieldName, const int8_t* val, const int32_t len)
            {
                WritePrimitiveArray<int8_t>(fieldName, val, len, PortableUtils::WriteInt8Array, IGNITE_TYPE_ARRAY_BYTE, 0);
            }

            void PortableWriterImpl::WriteBool(const bool val)
            {
                WritePrimitiveRaw<bool>(val, PortableUtils::WriteBool);
            }

            void PortableWriterImpl::WriteBoolArray(const bool* val, const int32_t len)
            {
                WritePrimitiveArrayRaw<bool>(val, len, PortableUtils::WriteBoolArray, IGNITE_TYPE_ARRAY_BOOL);
            }

            void PortableWriterImpl::WriteBool(const char* fieldName, const bool val)
            {
                WritePrimitive<bool>(fieldName, val, PortableUtils::WriteBool, IGNITE_TYPE_BOOL, 1);
            }

            void PortableWriterImpl::WriteBoolArray(const char* fieldName, const bool* val, const int32_t len)
            {
                WritePrimitiveArray<bool>(fieldName, val, len, PortableUtils::WriteBoolArray, IGNITE_TYPE_ARRAY_BOOL, 0);
            }

            void PortableWriterImpl::WriteInt16(const int16_t val)
            {
                WritePrimitiveRaw<int16_t>(val, PortableUtils::WriteInt16);
            }

            void PortableWriterImpl::WriteInt16Array(const int16_t* val, const int32_t len)
            {
                WritePrimitiveArrayRaw<int16_t>(val, len, PortableUtils::WriteInt16Array, IGNITE_TYPE_ARRAY_SHORT);
            }

            void PortableWriterImpl::WriteInt16(const char* fieldName, const int16_t val)
            {
                WritePrimitive<int16_t>(fieldName, val, PortableUtils::WriteInt16, IGNITE_TYPE_SHORT, 2);
            }

            void PortableWriterImpl::WriteInt16Array(const char* fieldName, const int16_t* val, const int32_t len)
            {
                WritePrimitiveArray<int16_t>(fieldName, val, len, PortableUtils::WriteInt16Array, IGNITE_TYPE_ARRAY_SHORT, 1);
            }

            void PortableWriterImpl::WriteUInt16(const uint16_t val)
            {
                WritePrimitiveRaw<uint16_t>(val, PortableUtils::WriteUInt16);
            }

            void PortableWriterImpl::WriteUInt16Array(const uint16_t* val, const int32_t len)
            {
                WritePrimitiveArrayRaw<uint16_t>(val, len, PortableUtils::WriteUInt16Array, IGNITE_TYPE_ARRAY_CHAR);
            }

            void PortableWriterImpl::WriteUInt16(const char* fieldName, const uint16_t val)
            {
                WritePrimitive<uint16_t>(fieldName, val, PortableUtils::WriteUInt16, IGNITE_TYPE_CHAR, 2);
            }

            void PortableWriterImpl::WriteUInt16Array(const char* fieldName, const uint16_t* val, const int32_t len)
            {
                WritePrimitiveArray<uint16_t>(fieldName, val, len, PortableUtils::WriteUInt16Array, IGNITE_TYPE_ARRAY_CHAR, 1);
            }

            void PortableWriterImpl::WriteInt32(const int32_t val)
            {
                WritePrimitiveRaw<int32_t>(val, PortableUtils::WriteInt32);
            }

            void PortableWriterImpl::WriteInt32Array(const int32_t* val, const int32_t len)
            {
                WritePrimitiveArrayRaw<int32_t>(val, len, PortableUtils::WriteInt32Array, IGNITE_TYPE_ARRAY_INT);
            }

            void PortableWriterImpl::WriteInt32(const char* fieldName, const int32_t val)
            {
                WritePrimitive<int32_t>(fieldName, val, PortableUtils::WriteInt32, IGNITE_TYPE_INT, 4);
            }

            void PortableWriterImpl::WriteInt32Array(const char* fieldName, const int32_t* val, const int32_t len)
            {
                WritePrimitiveArray<int32_t>(fieldName, val, len, PortableUtils::WriteInt32Array, IGNITE_TYPE_ARRAY_INT, 2);
            }

            void PortableWriterImpl::WriteInt64(const int64_t val)
            {
                WritePrimitiveRaw<int64_t>(val, PortableUtils::WriteInt64);
            }

            void PortableWriterImpl::WriteInt64Array(const int64_t* val, const int32_t len)
            {
                WritePrimitiveArrayRaw<int64_t>(val, len, PortableUtils::WriteInt64Array, IGNITE_TYPE_ARRAY_LONG);
            }

            void PortableWriterImpl::WriteInt64(const char* fieldName, const int64_t val)
            {
                WritePrimitive<int64_t>(fieldName, val, PortableUtils::WriteInt64, IGNITE_TYPE_LONG, 8);
            }

            void PortableWriterImpl::WriteInt64Array(const char* fieldName, const int64_t* val, const int32_t len)
            {
                WritePrimitiveArray<int64_t>(fieldName, val, len, PortableUtils::WriteInt64Array, IGNITE_TYPE_ARRAY_LONG, 3);
            }

            void PortableWriterImpl::WriteFloat(const float val)
            {
                WritePrimitiveRaw<float>(val, PortableUtils::WriteFloat);
            }

            void PortableWriterImpl::WriteFloatArray(const float* val, const int32_t len)
            {
                WritePrimitiveArrayRaw<float>(val, len, PortableUtils::WriteFloatArray, IGNITE_TYPE_ARRAY_FLOAT);
            }

            void PortableWriterImpl::WriteFloat(const char* fieldName, const float val)
            {
                WritePrimitive<float>(fieldName, val, PortableUtils::WriteFloat, IGNITE_TYPE_FLOAT, 4);
            }

            void PortableWriterImpl::WriteFloatArray(const char* fieldName, const float* val, const int32_t len)
            {
                WritePrimitiveArray<float>(fieldName, val, len, PortableUtils::WriteFloatArray, IGNITE_TYPE_ARRAY_FLOAT, 2);
            }

            void PortableWriterImpl::WriteDouble(const double val)
            {
                WritePrimitiveRaw<double>(val, PortableUtils::WriteDouble);
            }

            void PortableWriterImpl::WriteDoubleArray(const double* val, const int32_t len)
            {
                WritePrimitiveArrayRaw<double>(val, len, PortableUtils::WriteDoubleArray, IGNITE_TYPE_ARRAY_DOUBLE);
            }

            void PortableWriterImpl::WriteDouble(const char* fieldName, const double val)
            {
                WritePrimitive<double>(fieldName, val, PortableUtils::WriteDouble, IGNITE_TYPE_DOUBLE, 8);
            }

            void PortableWriterImpl::WriteDoubleArray(const char* fieldName, const double* val, const int32_t len)
            {
                WritePrimitiveArray<double>(fieldName, val, len, PortableUtils::WriteDoubleArray, IGNITE_TYPE_ARRAY_DOUBLE, 3);
            }

            void PortableWriterImpl::WriteGuid(const Guid val)
            {                
                CheckRawMode(true);
                CheckSingleMode(true);

                stream->WriteInt8(IGNITE_TYPE_UUID);

                PortableUtils::WriteGuid(stream, val);
            }

            void PortableWriterImpl::WriteGuidArray(const Guid* val, const int32_t len)
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
                        PortableUtils::WriteGuid(stream, elem);
                    }
                }
                else
                    stream->WriteInt8(IGNITE_HDR_NULL);
            }

            void PortableWriterImpl::WriteGuid(const char* fieldName, const Guid val)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                WriteFieldIdAndLength(fieldName, IGNITE_TYPE_UUID, 1 + 16);

                stream->WriteInt8(IGNITE_TYPE_UUID);

                PortableUtils::WriteGuid(stream, val);
            }

            void PortableWriterImpl::WriteGuidArray(const char* fieldName, const Guid* val, const int32_t len)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                WriteFieldId(fieldName, IGNITE_TYPE_ARRAY_UUID);

                if (val)
                {
                    stream->WriteInt32(5 + len * 17);
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
                    stream->WriteInt32(1);
                    stream->WriteInt8(IGNITE_HDR_NULL);
                }
            }

            void PortableWriterImpl::WriteString(const char* val, const int32_t len)
            {
                CheckRawMode(true);
                CheckSingleMode(true);

                if (val) 
                {
                    stream->WriteInt8(IGNITE_TYPE_STRING);

                    PortableUtils::WriteString(stream, val, len);
                }
                else
                    stream->WriteInt8(IGNITE_HDR_NULL);
            }

            void PortableWriterImpl::WriteString(const char* fieldName, const char* val, const int32_t len)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                WriteFieldId(fieldName, IGNITE_TYPE_STRING);
                
                if (val)
                {
                    int32_t lenPos = stream->Position();
                    stream->Position(lenPos + 4);

                    stream->WriteInt8(IGNITE_TYPE_STRING);
                    stream->WriteBool(false);
                    stream->WriteInt32(len);

                    for (int i = 0; i < len; i++)
                        stream->WriteUInt16(*(val + i));

                    stream->WriteInt32(lenPos, stream->Position() - lenPos - 4);
                }
                else
                {
                    stream->WriteInt32(1);
                    stream->WriteInt8(IGNITE_HDR_NULL);
                }
            }

            int32_t PortableWriterImpl::WriteStringArray()
            {
                StartContainerSession(true);

                stream->WriteInt8(IGNITE_TYPE_ARRAY_STRING);
                stream->Position(stream->Position() + 4);

                return elemId;
            }

            int32_t PortableWriterImpl::WriteStringArray(const char* fieldName)
            {
                StartContainerSession(false);

                WriteFieldIdSkipLength(fieldName, IGNITE_TYPE_ARRAY_STRING);

                stream->WriteInt8(IGNITE_TYPE_ARRAY_STRING);
                stream->Position(stream->Position() + 4);

                return elemId;
            }

            void PortableWriterImpl::WriteStringElement(int32_t id, const char* val, int32_t len)
            {
                CheckSession(id);

                if (val)
                {
                    stream->WriteInt8(IGNITE_TYPE_STRING);

                    PortableUtils::WriteString(stream, val, len);
                }
                else
                    stream->WriteInt8(IGNITE_HDR_NULL);

                elemCnt++;
            }

            void PortableWriterImpl::WriteNull()
            {
                CheckRawMode(true);
                CheckSingleMode(true);

                stream->WriteInt8(IGNITE_HDR_NULL);
            }

            void PortableWriterImpl::WriteNull(const char* fieldName)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                WriteFieldIdAndLength(fieldName, IGNITE_TYPE_OBJECT, 1);
                stream->WriteInt8(IGNITE_HDR_NULL);
            }

            int32_t PortableWriterImpl::WriteArray()
            {
                StartContainerSession(true);
                
                stream->WriteInt8(IGNITE_TYPE_ARRAY);
                stream->Position(stream->Position() + 4);

                return elemId;
            }

            int32_t PortableWriterImpl::WriteArray(const char* fieldName)
            {
                StartContainerSession(false);

                WriteFieldIdSkipLength(fieldName, IGNITE_TYPE_ARRAY);

                stream->WriteInt8(IGNITE_TYPE_ARRAY);
                stream->Position(stream->Position() + 4);

                return elemId;
            }

            int32_t PortableWriterImpl::WriteCollection(CollectionType typ)
            {
                StartContainerSession(true);

                stream->WriteInt8(IGNITE_TYPE_COLLECTION);
                stream->Position(stream->Position() + 4);
                stream->WriteInt8(typ);

                return elemId;
            }

            int32_t PortableWriterImpl::WriteCollection(const char* fieldName, CollectionType typ)
            {
                StartContainerSession(false);
                
                WriteFieldIdSkipLength(fieldName, IGNITE_TYPE_COLLECTION);

                stream->WriteInt8(IGNITE_TYPE_COLLECTION);
                stream->Position(stream->Position() + 4);
                stream->WriteInt8(typ);

                return elemId;
            }

            int32_t PortableWriterImpl::WriteMap(ignite::portable::MapType typ)
            {
                StartContainerSession(true);

                stream->WriteInt8(IGNITE_TYPE_MAP);
                stream->Position(stream->Position() + 4);
                stream->WriteInt8(typ);

                return elemId;
            }

            int32_t PortableWriterImpl::WriteMap(const char* fieldName, ignite::portable::MapType typ)
            {
                StartContainerSession(false);

                WriteFieldIdSkipLength(fieldName, IGNITE_TYPE_MAP);
                
                stream->WriteInt8(IGNITE_TYPE_MAP);
                stream->Position(stream->Position() + 4);
                stream->WriteInt8(typ);

                return elemId;
            }

            void PortableWriterImpl::CommitContainer(int32_t id)
            {
                CheckSession(id);

                if (rawPos == -1)
                {
                    int32_t len = stream->Position() - elemPos - 4;

                    stream->WriteInt32(elemPos + 4, len);
                    stream->WriteInt32(elemPos + 9, elemCnt);
                }
                else
                    stream->WriteInt32(elemPos + 1, elemCnt);

                elemId = 0;
                elemCnt = 0;
                elemPos = -1;
            }
            
            void PortableWriterImpl::SetRawMode()
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                rawPos = stream->Position();
            }

            int32_t PortableWriterImpl::GetRawPosition()
            {
                return rawPos == -1 ? stream->Position() : rawPos;
            }

            void PortableWriterImpl::CheckRawMode(bool expected)
            {
                bool rawMode = rawPos != -1;

                if (expected && !rawMode) {
                    IGNITE_ERROR_1(IgniteError::IGNITE_ERR_PORTABLE, "Operation can be performed only in raw mode.");
                }
                else if (!expected && rawMode) {
                    IGNITE_ERROR_1(IgniteError::IGNITE_ERR_PORTABLE, "Operation cannot be performed in raw mode.");
                }
            }

            void PortableWriterImpl::CheckSingleMode(bool expected)
            {
                if (expected && elemId != 0) {
                    IGNITE_ERROR_1(IgniteError::IGNITE_ERR_PORTABLE, "Operation cannot be performed when container is being written.");
                }
                else if (!expected && elemId == 0) {
                    IGNITE_ERROR_1(IgniteError::IGNITE_ERR_PORTABLE, "Operation can be performed only when container is being written.");
                }
            }

            void PortableWriterImpl::StartContainerSession(bool expRawMode)
            {
                CheckRawMode(expRawMode);
                CheckSingleMode(true);

                elemId = ++elemIdGen;
                elemPos = stream->Position();
            }

            void PortableWriterImpl::CheckSession(int32_t expSes)
            {
                if (elemId != expSes) 
                {
                    IGNITE_ERROR_1(IgniteError::IGNITE_ERR_PORTABLE, "Containter write session has been finished or is not started yet.");
                }
            }

            void PortableWriterImpl::WriteFieldId(const char* fieldName, int32_t fieldTypeId)
            {
                int32_t fieldId = idRslvr->GetFieldId(typeId, fieldName);
                
                stream->WriteInt32(fieldId);

                if (metaHnd)
                    metaHnd->OnFieldWritten(fieldId, fieldName, fieldTypeId);
            }

            void PortableWriterImpl::WriteFieldIdSkipLength(const char* fieldName, int32_t fieldTypeId)
            {
                WriteFieldId(fieldName, fieldTypeId);

                stream->Position(stream->Position() + 4);
            }

            void PortableWriterImpl::WriteFieldIdAndLength(const char* fieldName, int32_t fieldTypeId, int32_t len)
            {
                WriteFieldId(fieldName, fieldTypeId);

                stream->WriteInt32(len);
            }
            
            template <>
            void PortableWriterImpl::WriteTopObject<int8_t>(const int8_t& obj)
            {
                WriteTopObject0<int8_t>(obj, PortableUtils::WriteInt8, IGNITE_TYPE_BYTE);
            }

            template <>
            void PortableWriterImpl::WriteTopObject<bool>(const bool& obj)
            {
                WriteTopObject0<bool>(obj, PortableUtils::WriteBool, IGNITE_TYPE_BOOL);
            }

            template <>
            void PortableWriterImpl::WriteTopObject<int16_t>(const int16_t& obj)
            {
                WriteTopObject0<int16_t>(obj, PortableUtils::WriteInt16, IGNITE_TYPE_SHORT);
            }

            template <>
            void PortableWriterImpl::WriteTopObject<uint16_t>(const uint16_t& obj)
            {
                WriteTopObject0<uint16_t>(obj, PortableUtils::WriteUInt16, IGNITE_TYPE_CHAR);
            }

            template <>
            void PortableWriterImpl::WriteTopObject<int32_t>(const int32_t& obj)
            {
                WriteTopObject0<int32_t>(obj, PortableUtils::WriteInt32, IGNITE_TYPE_INT);
            }

            template <>
            void PortableWriterImpl::WriteTopObject<int64_t>(const int64_t& obj)
            {
                WriteTopObject0<int64_t>(obj, PortableUtils::WriteInt64, IGNITE_TYPE_LONG);
            }

            template <>
            void PortableWriterImpl::WriteTopObject<float>(const float& obj)
            {
                WriteTopObject0<float>(obj, PortableUtils::WriteFloat, IGNITE_TYPE_FLOAT);
            }

            template <>
            void PortableWriterImpl::WriteTopObject<double>(const double& obj)
            {
                WriteTopObject0<double>(obj, PortableUtils::WriteDouble, IGNITE_TYPE_DOUBLE);
            }

            template <>
            void PortableWriterImpl::WriteTopObject<Guid>(const Guid& obj)
            {
                WriteTopObject0<Guid>(obj, PortableUtils::WriteGuid, IGNITE_TYPE_UUID);
            }

            InteropOutputStream* PortableWriterImpl::GetStream()
            {
                return stream;
            }
        }
    }
}