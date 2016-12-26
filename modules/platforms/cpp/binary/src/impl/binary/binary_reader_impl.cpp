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

#include "ignite/impl/interop/interop.h"
#include "ignite/impl/interop/interop_stream_position_guard.h"
#include "ignite/impl/binary/binary_common.h"
#include "ignite/impl/binary/binary_id_resolver.h"
#include "ignite/impl/binary/binary_reader_impl.h"
#include "ignite/impl/binary/binary_utils.h"
#include "ignite/binary/binary_type.h"

using namespace ignite::impl::interop;
using namespace ignite::impl::binary;
using namespace ignite::binary;

namespace ignite
{
    namespace impl
    {
        namespace binary
        {
            BinaryReaderImpl::BinaryReaderImpl(InteropInputStream* stream, BinaryIdResolver* idRslvr,
                int32_t pos, bool usrType, int32_t typeId, int32_t hashCode, int32_t len, int32_t rawOff,
                int32_t footerBegin, int32_t footerEnd, BinaryOffsetType schemaType) :
                stream(stream), idRslvr(idRslvr), pos(pos), usrType(usrType), typeId(typeId), 
                hashCode(hashCode), len(len), rawOff(rawOff), rawMode(false), elemIdGen(0), elemId(0),
                elemCnt(-1), elemRead(0), footerBegin(footerBegin), footerEnd(footerEnd), schemaType(schemaType)
            {
                // No-op.
            }

            BinaryReaderImpl::BinaryReaderImpl(InteropInputStream* stream) :
                stream(stream), idRslvr(NULL), pos(0), usrType(false), typeId(0), hashCode(0), len(0),
                rawOff(0), rawMode(true), elemIdGen(0), elemId(0), elemCnt(-1), elemRead(0), footerBegin(-1),
                footerEnd(-1), schemaType(OFFSET_TYPE_FOUR_BYTES)
            {
                // No-op.
            }

            int8_t BinaryReaderImpl::ReadInt8()
            {
                return ReadRaw<int8_t>(BinaryUtils::ReadInt8);                
            }
            
            int32_t BinaryReaderImpl::ReadInt8Array(int8_t* res, const int32_t len)
            {
                return ReadRawArray<int8_t>(res, len, BinaryUtils::ReadInt8Array, IGNITE_TYPE_ARRAY_BYTE);
            }

            int8_t BinaryReaderImpl::ReadInt8(const char* fieldName)
            {
                return Read(fieldName, BinaryUtils::ReadInt8, IGNITE_TYPE_BYTE, static_cast<int8_t>(0));
            }

            int32_t BinaryReaderImpl::ReadInt8Array(const char* fieldName, int8_t* res, const int32_t len)
            {
                return ReadArray<int8_t>(fieldName, res, len,BinaryUtils::ReadInt8Array, IGNITE_TYPE_ARRAY_BYTE);
            }

            bool BinaryReaderImpl::ReadBool()
            {
                return ReadRaw<bool>(BinaryUtils::ReadBool);
            }

            int32_t BinaryReaderImpl::ReadBoolArray(bool* res, const int32_t len)
            {
                return ReadRawArray<bool>(res, len, BinaryUtils::ReadBoolArray, IGNITE_TYPE_ARRAY_BOOL);
            }

            bool BinaryReaderImpl::ReadBool(const char* fieldName)
            {
                return Read(fieldName, BinaryUtils::ReadBool, IGNITE_TYPE_BOOL, static_cast<bool>(0));
            }

            int32_t BinaryReaderImpl::ReadBoolArray(const char* fieldName, bool* res, const int32_t len)
            {
                return ReadArray<bool>(fieldName, res, len,BinaryUtils::ReadBoolArray, IGNITE_TYPE_ARRAY_BOOL);
            }

            int16_t BinaryReaderImpl::ReadInt16()
            {
                return ReadRaw<int16_t>(BinaryUtils::ReadInt16);
            }

            int32_t BinaryReaderImpl::ReadInt16Array(int16_t* res, const int32_t len)
            {
                return ReadRawArray<int16_t>(res, len, BinaryUtils::ReadInt16Array, IGNITE_TYPE_ARRAY_SHORT);
            }

            int16_t BinaryReaderImpl::ReadInt16(const char* fieldName)
            {
                return Read(fieldName, BinaryUtils::ReadInt16, IGNITE_TYPE_SHORT, static_cast<int16_t>(0));
            }

            int32_t BinaryReaderImpl::ReadInt16Array(const char* fieldName, int16_t* res, const int32_t len)
            {
                return ReadArray<int16_t>(fieldName, res, len, BinaryUtils::ReadInt16Array, IGNITE_TYPE_ARRAY_SHORT);
            }

            uint16_t BinaryReaderImpl::ReadUInt16()
            {
                return ReadRaw<uint16_t>(BinaryUtils::ReadUInt16);
            }

            int32_t BinaryReaderImpl::ReadUInt16Array(uint16_t* res, const int32_t len)
            {
                return ReadRawArray<uint16_t>(res, len, BinaryUtils::ReadUInt16Array, IGNITE_TYPE_ARRAY_CHAR);
            }

            uint16_t BinaryReaderImpl::ReadUInt16(const char* fieldName)
            {
                return Read(fieldName, BinaryUtils::ReadUInt16, IGNITE_TYPE_CHAR, static_cast<uint16_t>(0));
            }

            int32_t BinaryReaderImpl::ReadUInt16Array(const char* fieldName, uint16_t* res, const int32_t len)
            {
                return ReadArray<uint16_t>(fieldName, res, len,BinaryUtils::ReadUInt16Array, IGNITE_TYPE_ARRAY_CHAR);
            }

            int32_t BinaryReaderImpl::ReadInt32()
            {
                return ReadRaw<int32_t>(BinaryUtils::ReadInt32);
            }

            int32_t BinaryReaderImpl::ReadInt32Array(int32_t* res, const int32_t len)
            {
                return ReadRawArray<int32_t>(res, len, BinaryUtils::ReadInt32Array, IGNITE_TYPE_ARRAY_INT);
            }

            int32_t BinaryReaderImpl::ReadInt32(const char* fieldName)
            {
                return Read(fieldName, BinaryUtils::ReadInt32, IGNITE_TYPE_INT, static_cast<int32_t>(0));
            }

            int32_t BinaryReaderImpl::ReadInt32Array(const char* fieldName, int32_t* res, const int32_t len)
            {
                return ReadArray<int32_t>(fieldName, res, len,BinaryUtils::ReadInt32Array, IGNITE_TYPE_ARRAY_INT);
            }

            int64_t BinaryReaderImpl::ReadInt64()
            {
                return ReadRaw<int64_t>(BinaryUtils::ReadInt64);
            }

            int32_t BinaryReaderImpl::ReadInt64Array(int64_t* res, const int32_t len)
            {
                return ReadRawArray<int64_t>(res, len, BinaryUtils::ReadInt64Array, IGNITE_TYPE_ARRAY_LONG);
            }

            int64_t BinaryReaderImpl::ReadInt64(const char* fieldName)
            {
                return Read(fieldName, BinaryUtils::ReadInt64, IGNITE_TYPE_LONG, static_cast<int64_t>(0));
            }

            int32_t BinaryReaderImpl::ReadInt64Array(const char* fieldName, int64_t* res, const int32_t len)
            {
                return ReadArray<int64_t>(fieldName, res, len,BinaryUtils::ReadInt64Array, IGNITE_TYPE_ARRAY_LONG);
            }

            float BinaryReaderImpl::ReadFloat()
            {
                return ReadRaw<float>(BinaryUtils::ReadFloat);
            }

            int32_t BinaryReaderImpl::ReadFloatArray(float* res, const int32_t len)
            {
                return ReadRawArray<float>(res, len, BinaryUtils::ReadFloatArray, IGNITE_TYPE_ARRAY_FLOAT);
            }

            float BinaryReaderImpl::ReadFloat(const char* fieldName)
            {
                return Read(fieldName, BinaryUtils::ReadFloat, IGNITE_TYPE_FLOAT, static_cast<float>(0));
            }

            int32_t BinaryReaderImpl::ReadFloatArray(const char* fieldName, float* res, const int32_t len)
            {
                return ReadArray<float>(fieldName, res, len,BinaryUtils::ReadFloatArray, IGNITE_TYPE_ARRAY_FLOAT);
            }

            double BinaryReaderImpl::ReadDouble()
            {
                return ReadRaw<double>(BinaryUtils::ReadDouble);
            }

            int32_t BinaryReaderImpl::ReadDoubleArray(double* res, const int32_t len)
            {
                return ReadRawArray<double>(res, len, BinaryUtils::ReadDoubleArray, IGNITE_TYPE_ARRAY_DOUBLE);
            }

            double BinaryReaderImpl::ReadDouble(const char* fieldName)
            {
                return Read(fieldName, BinaryUtils::ReadDouble, IGNITE_TYPE_DOUBLE, static_cast<double>(0));
            }

            int32_t BinaryReaderImpl::ReadDoubleArray(const char* fieldName, double* res, const int32_t len)
            {
                return ReadArray<double>(fieldName, res, len,BinaryUtils::ReadDoubleArray, IGNITE_TYPE_ARRAY_DOUBLE);
            }

            Guid BinaryReaderImpl::ReadGuid()
            {
                CheckRawMode(true);
                CheckSingleMode(true);

                return ReadNullable(stream, BinaryUtils::ReadGuid, IGNITE_TYPE_UUID);
            }

            int32_t BinaryReaderImpl::ReadGuidArray(Guid* res, const int32_t len)
            {
                CheckRawMode(true);
                CheckSingleMode(true);

                return ReadArrayInternal<Guid>(res, len, stream, ReadGuidArrayInternal, IGNITE_TYPE_ARRAY_UUID);
            }

            Guid BinaryReaderImpl::ReadGuid(const char* fieldName)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                int32_t fieldId = idRslvr->GetFieldId(typeId, fieldName);
                int32_t fieldPos = FindField(fieldId);

                if (fieldPos <= 0)
                    return Guid();

                stream->Position(fieldPos);

                return ReadNullable(stream, BinaryUtils::ReadGuid, IGNITE_TYPE_UUID);
            }

            int32_t BinaryReaderImpl::ReadGuidArray(const char* fieldName, Guid* res, const int32_t len)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                int32_t fieldId = idRslvr->GetFieldId(typeId, fieldName);
                int32_t fieldPos = FindField(fieldId);

                if (fieldPos <= 0)
                    return -1;

                stream->Position(fieldPos);

                int32_t realLen = ReadArrayInternal<Guid>(res, len, stream, ReadGuidArrayInternal, IGNITE_TYPE_ARRAY_UUID);

                return realLen;
            }

            void BinaryReaderImpl::ReadGuidArrayInternal(InteropInputStream* stream, Guid* res, const int32_t len)
            {
                for (int i = 0; i < len; i++)
                    *(res + i) = ReadNullable<Guid>(stream, BinaryUtils::ReadGuid, IGNITE_TYPE_UUID);
            }

            Date BinaryReaderImpl::ReadDate()
            {
                CheckRawMode(true);
                CheckSingleMode(true);

                return ReadNullable(stream, BinaryUtils::ReadDate, IGNITE_TYPE_DATE);
            }

            int32_t BinaryReaderImpl::ReadDateArray(Date* res, int32_t len)
            {
                CheckRawMode(true);
                CheckSingleMode(true);

                return ReadArrayInternal<Date>(res, len, stream, ReadDateArrayInternal, IGNITE_TYPE_ARRAY_DATE);
            }

            Date BinaryReaderImpl::ReadDate(const char* fieldName)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                int32_t fieldId = idRslvr->GetFieldId(typeId, fieldName);
                int32_t fieldPos = FindField(fieldId);

                if (fieldPos <= 0)
                    return Date();

                stream->Position(fieldPos);

                return ReadNullable(stream, BinaryUtils::ReadDate, IGNITE_TYPE_DATE);
            }

            int32_t BinaryReaderImpl::ReadDateArray(const char* fieldName, Date* res, const int32_t len)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                int32_t fieldId = idRslvr->GetFieldId(typeId, fieldName);
                int32_t fieldPos = FindField(fieldId);

                if (fieldPos <= 0)
                    return -1;

                stream->Position(fieldPos);

                int32_t realLen = ReadArrayInternal<Date>(res, len, stream, ReadDateArrayInternal, IGNITE_TYPE_ARRAY_DATE);

                return realLen;
            }

            void BinaryReaderImpl::ReadDateArrayInternal(InteropInputStream* stream, Date* res, const int32_t len)
            {
                for (int i = 0; i < len; i++)
                    *(res + i) = ReadNullable<Date>(stream, BinaryUtils::ReadDate, IGNITE_TYPE_DATE);
            }

            Timestamp BinaryReaderImpl::ReadTimestamp()
            {
                CheckRawMode(true);
                CheckSingleMode(true);

                return ReadNullable(stream, BinaryUtils::ReadTimestamp, IGNITE_TYPE_TIMESTAMP);
            }

            int32_t BinaryReaderImpl::ReadTimestampArray(Timestamp* res, int32_t len)
            {
                CheckRawMode(true);
                CheckSingleMode(true);

                return ReadArrayInternal<Timestamp>(res, len, stream, ReadTimestampArrayInternal, IGNITE_TYPE_ARRAY_TIMESTAMP);
            }

            Timestamp BinaryReaderImpl::ReadTimestamp(const char* fieldName)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                int32_t fieldId = idRslvr->GetFieldId(typeId, fieldName);
                int32_t fieldPos = FindField(fieldId);

                if (fieldPos <= 0)
                    return Timestamp();

                stream->Position(fieldPos);

                return ReadNullable(stream, BinaryUtils::ReadTimestamp, IGNITE_TYPE_TIMESTAMP);
            }

            int32_t BinaryReaderImpl::ReadTimestampArray(const char* fieldName, Timestamp* res, const int32_t len)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                int32_t fieldId = idRslvr->GetFieldId(typeId, fieldName);
                int32_t fieldPos = FindField(fieldId);

                if (fieldPos <= 0)
                    return -1;

                stream->Position(fieldPos);

                int32_t realLen = ReadArrayInternal<Timestamp>(res, len, stream, ReadTimestampArrayInternal, IGNITE_TYPE_ARRAY_TIMESTAMP);

                return realLen;
            }

            void BinaryReaderImpl::ReadTimestampArrayInternal(interop::InteropInputStream* stream, Timestamp* res, const int32_t len)
            {
                for (int i = 0; i < len; i++)
                    *(res + i) = ReadNullable<Timestamp>(stream, BinaryUtils::ReadTimestamp, IGNITE_TYPE_TIMESTAMP);
            }

            int32_t BinaryReaderImpl::ReadString(char* res, const int32_t len)
            {
                CheckRawMode(true);
                CheckSingleMode(true);

                return ReadStringInternal(res, len);
            }

            int32_t BinaryReaderImpl::ReadString(const char* fieldName, char* res, const int32_t len)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                int32_t fieldId = idRslvr->GetFieldId(typeId, fieldName);
                int32_t fieldPos = FindField(fieldId);

                if (fieldPos <= 0)
                    return -1;

                stream->Position(fieldPos);

                int32_t realLen = ReadStringInternal(res, len);

                return realLen;
            }

            int32_t BinaryReaderImpl::ReadStringArray(int32_t* size)
            {
                return StartContainerSession(true, IGNITE_TYPE_ARRAY_STRING, size);
            }

            int32_t BinaryReaderImpl::ReadStringArray(const char* fieldName, int32_t* size)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                int32_t fieldId = idRslvr->GetFieldId(typeId, fieldName);
                int32_t fieldPos = FindField(fieldId);

                if (fieldPos <= 0)
                {
                    *size = -1;

                    return ++elemIdGen;
                }

                stream->Position(fieldPos);

                return StartContainerSession(false, IGNITE_TYPE_ARRAY_STRING, size);
            }

            int32_t BinaryReaderImpl::ReadStringElement(int32_t id, char* res, const int32_t len)
            {
                CheckSession(id);

                int32_t posBefore = stream->Position();

                int32_t realLen = ReadStringInternal(res, len);

                int32_t posAfter = stream->Position();

                if (posAfter > posBefore && ++elemRead == elemCnt) {
                    elemId = 0;
                    elemCnt = -1;
                    elemRead = 0;
                }

                return realLen;
            }

            int32_t BinaryReaderImpl::ReadStringInternal(char* res, const int32_t len)
            {
                int8_t hdr = stream->ReadInt8();

                if (hdr == IGNITE_TYPE_STRING) {
                    int32_t realLen = stream->ReadInt32();

                    if (res && len >= realLen) {
                        stream->ReadInt8Array(reinterpret_cast<int8_t*>(res), realLen);

                        if (len > realLen)
                            *(res + realLen) = 0; // Set NULL terminator if possible.
                    }
                    else
                        stream->Position(stream->Position() - 4 - 1);

                    return realLen;
                }
                else if (hdr != IGNITE_HDR_NULL)
                    ThrowOnInvalidHeader(IGNITE_TYPE_ARRAY, hdr);

                return -1;
            }

            int32_t BinaryReaderImpl::ReadArray(int32_t* size)
            {
                return StartContainerSession(true, IGNITE_TYPE_ARRAY, size);
            }

            int32_t BinaryReaderImpl::ReadArray(const char* fieldName, int32_t* size)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                int32_t fieldId = idRslvr->GetFieldId(typeId, fieldName);
                int32_t fieldPos = FindField(fieldId);

                if (fieldPos <= 0)
                {
                    *size = -1;

                    return ++elemIdGen;
                }

                stream->Position(fieldPos);

                return StartContainerSession(false, IGNITE_TYPE_ARRAY, size);
            }

            int32_t BinaryReaderImpl::ReadCollection(CollectionType* typ, int32_t* size)
            {
                int32_t id = StartContainerSession(true, IGNITE_TYPE_COLLECTION, size);

                if (*size == -1)
                    *typ = IGNITE_COLLECTION_UNDEFINED;
                else
                    *typ = static_cast<CollectionType>(stream->ReadInt8());

                return id;
            }

            int32_t BinaryReaderImpl::ReadCollection(const char* fieldName, CollectionType* typ, int32_t* size)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                int32_t fieldId = idRslvr->GetFieldId(typeId, fieldName);
                int32_t fieldPos = FindField(fieldId);

                if (fieldPos <= 0)
                {
                    *typ = IGNITE_COLLECTION_UNDEFINED;
                    *size = -1;

                    return ++elemIdGen;
                }

                stream->Position(fieldPos);

                int32_t id = StartContainerSession(false, IGNITE_TYPE_COLLECTION, size);

                if (*size == -1)
                    *typ = IGNITE_COLLECTION_UNDEFINED;
                else
                    *typ = static_cast<CollectionType>(stream->ReadInt8());

                return id;
            }

            int32_t BinaryReaderImpl::ReadMap(MapType* typ, int32_t* size)
            {
                int32_t id = StartContainerSession(true, IGNITE_TYPE_MAP, size);

                if (*size == -1)
                    *typ = IGNITE_MAP_UNDEFINED;
                else
                    *typ = static_cast<MapType>(stream->ReadInt8());

                return id;
            }

            int32_t BinaryReaderImpl::ReadMap(const char* fieldName, MapType* typ, int32_t* size)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                int32_t fieldId = idRslvr->GetFieldId(typeId, fieldName);
                int32_t fieldPos = FindField(fieldId);

                if (fieldPos <= 0)
                {
                    *typ = IGNITE_MAP_UNDEFINED;
                    *size = -1;

                    return ++elemIdGen;
                }

                stream->Position(fieldPos);

                int32_t id = StartContainerSession(false, IGNITE_TYPE_MAP, size);

                if (*size == -1)
                    *typ = IGNITE_MAP_UNDEFINED;
                else
                    *typ = static_cast<MapType>(stream->ReadInt8());

                return id;
            }

            CollectionType BinaryReaderImpl::ReadCollectionTypeUnprotected()
            {
                int32_t size = ReadCollectionSizeUnprotected();
                if (size == -1)
                    return IGNITE_COLLECTION_UNDEFINED;

                CollectionType typ = static_cast<CollectionType>(stream->ReadInt8());

                return typ;
            }

            CollectionType BinaryReaderImpl::ReadCollectionType()
            {
                InteropStreamPositionGuard<InteropInputStream> positionGuard(*stream);
                
                return ReadCollectionTypeUnprotected();
            }

            CollectionType BinaryReaderImpl::ReadCollectionType(const char* fieldName)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                InteropStreamPositionGuard<InteropInputStream> positionGuard(*stream);

                int32_t fieldId = idRslvr->GetFieldId(typeId, fieldName);
                int32_t fieldPos = FindField(fieldId);

                if (fieldPos <= 0)
                    return IGNITE_COLLECTION_UNDEFINED;

                stream->Position(fieldPos);

                return ReadCollectionTypeUnprotected();
            }

            int32_t BinaryReaderImpl::ReadCollectionSizeUnprotected()
            {
                int8_t hdr = stream->ReadInt8();

                if (hdr != IGNITE_TYPE_COLLECTION)
                {
                    if (hdr != IGNITE_HDR_NULL)
                        ThrowOnInvalidHeader(IGNITE_TYPE_COLLECTION, hdr);

                    return -1;
                }

                int32_t size = stream->ReadInt32();

                return size;
            }

            int32_t BinaryReaderImpl::ReadCollectionSize()
            {
                InteropStreamPositionGuard<InteropInputStream> positionGuard(*stream);

                return ReadCollectionSizeUnprotected();
            }

            int32_t BinaryReaderImpl::ReadCollectionSize(const char* fieldName)
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                InteropStreamPositionGuard<InteropInputStream> positionGuard(*stream);

                int32_t fieldId = idRslvr->GetFieldId(typeId, fieldName);
                int32_t fieldPos = FindField(fieldId);

                if (fieldPos <= 0)
                    return -1;

                stream->Position(fieldPos);

                return ReadCollectionSizeUnprotected();
            }

            bool BinaryReaderImpl::HasNextElement(int32_t id) const
            {
                return elemId == id && elemRead < elemCnt;
            }

            bool BinaryReaderImpl::SkipIfNull()
            {
                CheckRawMode(true);
                CheckSingleMode(true);

                InteropStreamPositionGuard<InteropInputStream> positionGuard(*stream);

                int8_t hdr = stream->ReadInt8();

                if (hdr != IGNITE_HDR_NULL)
                    return false;

                positionGuard.Release();

                return true;
            }

            void BinaryReaderImpl::SetRawMode()
            {
                CheckRawMode(false);
                CheckSingleMode(true);

                stream->Position(pos + rawOff);
                rawMode = true;
            }

            template <>
            int8_t BinaryReaderImpl::ReadTopObject<int8_t>()
            {
                return ReadTopObject0(IGNITE_TYPE_BYTE, BinaryUtils::ReadInt8,
                    BinaryUtils::GetDefaultValue<int8_t>());
            }

            template <>
            bool BinaryReaderImpl::ReadTopObject<bool>()
            {
                return ReadTopObject0(IGNITE_TYPE_BOOL, BinaryUtils::ReadBool,
                    BinaryUtils::GetDefaultValue<bool>());
            }

            template <>
            int16_t BinaryReaderImpl::ReadTopObject<int16_t>()
            {
                return ReadTopObject0(IGNITE_TYPE_SHORT, BinaryUtils::ReadInt16,
                    BinaryUtils::GetDefaultValue<int16_t>());
            }

            template <>
            uint16_t BinaryReaderImpl::ReadTopObject<uint16_t>()
            {
                return ReadTopObject0(IGNITE_TYPE_CHAR, BinaryUtils::ReadUInt16,
                    BinaryUtils::GetDefaultValue<uint16_t>());
            }

            template <>
            int32_t BinaryReaderImpl::ReadTopObject<int32_t>()
            {
                return ReadTopObject0(IGNITE_TYPE_INT, BinaryUtils::ReadInt32,
                    BinaryUtils::GetDefaultValue<int32_t>());
            }

            template <>
            int64_t BinaryReaderImpl::ReadTopObject<int64_t>()
            {
                return ReadTopObject0(IGNITE_TYPE_LONG, BinaryUtils::ReadInt64,
                    BinaryUtils::GetDefaultValue<int64_t>());
            }

            template <>
            float BinaryReaderImpl::ReadTopObject<float>()
            {
                return ReadTopObject0(IGNITE_TYPE_FLOAT, BinaryUtils::ReadFloat,
                    BinaryUtils::GetDefaultValue<float>());
            }

            template <>
            double BinaryReaderImpl::ReadTopObject<double>()
            {
                return ReadTopObject0(IGNITE_TYPE_DOUBLE, BinaryUtils::ReadDouble,
                    BinaryUtils::GetDefaultValue<double>());
            }

            template <>
            Guid BinaryReaderImpl::ReadTopObject<Guid>()
            {
                int8_t typeId = stream->ReadInt8();

                if (typeId == IGNITE_TYPE_UUID)
                    return BinaryUtils::ReadGuid(stream);
                else if (typeId == IGNITE_HDR_NULL)
                    return BinaryUtils::GetDefaultValue<Guid>();
                else {
                    int32_t pos = stream->Position() - 1;

                    IGNITE_ERROR_FORMATTED_3(IgniteError::IGNITE_ERR_BINARY, "Invalid header", "position", pos, "expected", (int)IGNITE_TYPE_UUID, "actual", (int)typeId)
                }
            }

            template <>
            Date BinaryReaderImpl::ReadTopObject<Date>()
            {
                int8_t typeId = stream->ReadInt8();

                if (typeId == IGNITE_TYPE_DATE)
                    return BinaryUtils::ReadDate(stream);
                else if (typeId == IGNITE_TYPE_TIMESTAMP)
                    return Date(BinaryUtils::ReadTimestamp(stream).GetMilliseconds());
                else if (typeId == IGNITE_HDR_NULL)
                    return BinaryUtils::GetDefaultValue<Date>();
                else {
                    int32_t pos = stream->Position() - 1;

                    IGNITE_ERROR_FORMATTED_3(IgniteError::IGNITE_ERR_BINARY, "Invalid header", "position", pos, "expected", (int)IGNITE_TYPE_DATE, "actual", (int)typeId)
                }
            }

            template <>
            Timestamp BinaryReaderImpl::ReadTopObject<Timestamp>()
            {
                int8_t typeId = stream->ReadInt8();

                if (typeId == IGNITE_TYPE_TIMESTAMP)
                    return BinaryUtils::ReadTimestamp(stream);
                else if (typeId == IGNITE_HDR_NULL)
                    return BinaryUtils::GetDefaultValue<Timestamp>();
                else {
                    int32_t pos = stream->Position() - 1;

                    IGNITE_ERROR_FORMATTED_3(IgniteError::IGNITE_ERR_BINARY, "Invalid header", "position", pos, "expected", (int)IGNITE_TYPE_TIMESTAMP, "actual", (int)typeId)
                }
            }

            InteropInputStream* BinaryReaderImpl::GetStream()
            {
                return stream;
            }

            int32_t BinaryReaderImpl::FindField(const int32_t fieldId)
            {
                InteropStreamPositionGuard<InteropInputStream> streamGuard(*stream);

                stream->Position(footerBegin);

                switch (schemaType)
                {
                    case OFFSET_TYPE_ONE_BYTE:
                    {
                        for (int32_t schemaPos = footerBegin; schemaPos < footerEnd; schemaPos += 5)
                        {
                            int32_t currentFieldId = stream->ReadInt32(schemaPos);

                            if (fieldId != currentFieldId)
                                continue;
                            else
                                return static_cast<uint8_t>(stream->ReadInt8(schemaPos + 4)) + pos;
                        }
                        break;
                    }

                    case OFFSET_TYPE_TWO_BYTES:
                    {
                        for (int32_t schemaPos = footerBegin; schemaPos < footerEnd; schemaPos += 6)
                        {
                            int32_t currentFieldId = stream->ReadInt32(schemaPos);

                            if (fieldId != currentFieldId)
                                continue;
                            else
                                return static_cast<uint16_t>(stream->ReadInt16(schemaPos + 4)) + pos;
                        }
                        break;
                    }

                    case OFFSET_TYPE_FOUR_BYTES:
                    {
                        for (int32_t schemaPos = footerBegin; schemaPos < footerEnd; schemaPos += 8)
                        {
                            int32_t currentFieldId = stream->ReadInt32(schemaPos);

                            if (fieldId != currentFieldId)
                                continue;
                            else
                                return stream->ReadInt32(schemaPos + 4) + pos;
                        }
                        break;
                    }
                }

                return -1;
            }

            void BinaryReaderImpl::CheckRawMode(bool expected) const
            {
                if (expected && !rawMode) {
                    IGNITE_ERROR_1(IgniteError::IGNITE_ERR_BINARY, "Operation can be performed only in raw mode.")
                }
                else if (!expected && rawMode) {
                    IGNITE_ERROR_1(IgniteError::IGNITE_ERR_BINARY, "Operation cannot be performed in raw mode.")
                }
            }

            void BinaryReaderImpl::CheckSingleMode(bool expected) const
            {
                if (expected && elemId != 0) {
                    IGNITE_ERROR_1(IgniteError::IGNITE_ERR_BINARY, "Operation cannot be performed when container is being read.");
                }
                else if (!expected && elemId == 0) {
                    IGNITE_ERROR_1(IgniteError::IGNITE_ERR_BINARY, "Operation can be performed only when container is being read.");
                }
            }

            int32_t BinaryReaderImpl::StartContainerSession(bool expRawMode, int8_t expHdr, int32_t* size)
            {
                CheckRawMode(expRawMode);
                CheckSingleMode(true);

                int8_t hdr = stream->ReadInt8();

                if (hdr == expHdr)
                {
                    int32_t cnt = stream->ReadInt32();

                    if (cnt != 0) 
                    {
                        elemId = ++elemIdGen;
                        elemCnt = cnt;
                        elemRead = 0;

                        *size = cnt;

                        return elemId;
                    }
                    else
                    {
                        *size = 0;

                        return ++elemIdGen;
                    }
                }
                else if (hdr == IGNITE_HDR_NULL) {
                    *size = -1;

                    return ++elemIdGen;
                }
                else {
                    ThrowOnInvalidHeader(expHdr, hdr);

                    return 0;
                }
            }

            void BinaryReaderImpl::CheckSession(int32_t expSes) const
            {
                if (elemId != expSes) {
                    IGNITE_ERROR_1(IgniteError::IGNITE_ERR_BINARY, "Containter read session has been finished or is not started yet.");
                }
            }

            void BinaryReaderImpl::ThrowOnInvalidHeader(int32_t pos, int8_t expHdr, int8_t hdr)
            {
                IGNITE_ERROR_FORMATTED_3(IgniteError::IGNITE_ERR_BINARY, "Invalid header", "position", pos, "expected", (int)expHdr, "actual", (int)hdr)
            }

            void BinaryReaderImpl::ThrowOnInvalidHeader(int8_t expHdr, int8_t hdr) const
            {
                int32_t pos = stream->Position() - 1;

                ThrowOnInvalidHeader(pos, expHdr, hdr);
            }
        }
    }
}
