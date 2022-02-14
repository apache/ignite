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

#include "ignite/binary/binary_reader.h"
#include "ignite/impl/binary/binary_reader_impl.h"

using namespace ignite::impl::binary;

namespace ignite
{
    namespace binary
    {
        BinaryReader::BinaryReader(BinaryReaderImpl* impl) : impl(impl)
        {
            // No-op.
        }
        
        int8_t BinaryReader::ReadInt8(const char* fieldName)
        {
            return impl->ReadInt8(fieldName);
        }

        int32_t BinaryReader::ReadInt8Array(const char* fieldName, int8_t* res, int32_t len)
        {
            return impl->ReadInt8Array(fieldName, res, len);
        }

        bool BinaryReader::ReadBool(const char* fieldName)
        {
            return impl->ReadBool(fieldName);
        }

        int32_t BinaryReader::ReadBoolArray(const char* fieldName, bool* res, int32_t len)
        {
            return impl->ReadBoolArray(fieldName, res, len);
        }

        int16_t BinaryReader::ReadInt16(const char* fieldName)
        {
            return impl->ReadInt16(fieldName);
        }

        int32_t BinaryReader::ReadInt16Array(const char* fieldName, int16_t* res, int32_t len)
        {
            return impl->ReadInt16Array(fieldName, res, len);
        }

        uint16_t BinaryReader::ReadUInt16(const char* fieldName)
        {
            return impl->ReadUInt16(fieldName);
        }

        int32_t BinaryReader::ReadUInt16Array(const char* fieldName, uint16_t* res, int32_t len)
        {
            return impl->ReadUInt16Array(fieldName, res, len);
        }

        int32_t BinaryReader::ReadInt32(const char* fieldName)
        {
            return impl->ReadInt32(fieldName);
        }

        int32_t BinaryReader::ReadInt32Array(const char* fieldName, int32_t* res, int32_t len)
        {
            return impl->ReadInt32Array(fieldName, res, len);
        }

        int64_t BinaryReader::ReadInt64(const char* fieldName)
        {
            return impl->ReadInt64(fieldName);
        }

        int32_t BinaryReader::ReadInt64Array(const char* fieldName, int64_t* res, int32_t len)
        {
            return impl->ReadInt64Array(fieldName, res, len);
        }

        float BinaryReader::ReadFloat(const char* fieldName)
        {
            return impl->ReadFloat(fieldName);
        }

        int32_t BinaryReader::ReadFloatArray(const char* fieldName, float* res, int32_t len)
        {
            return impl->ReadFloatArray(fieldName, res, len);
        }

        double BinaryReader::ReadDouble(const char* fieldName)
        {
            return impl->ReadDouble(fieldName);
        }

        int32_t BinaryReader::ReadDoubleArray(const char* fieldName, double* res, int32_t len)
        {
            return impl->ReadDoubleArray(fieldName, res, len);
        }

        Guid BinaryReader::ReadGuid(const char* fieldName)
        {
            return impl->ReadGuid(fieldName);
        }

        int32_t BinaryReader::ReadGuidArray(const char* fieldName, Guid* res, int32_t len)
        {
            return impl->ReadGuidArray(fieldName, res, len);
        }

        Date BinaryReader::ReadDate(const char * fieldName)
        {
            return impl->ReadDate(fieldName);
        }

        int32_t BinaryReader::ReadDateArray(const char * fieldName, Date * res, const int32_t len)
        {
            return impl->ReadDateArray(fieldName, res, len);
        }

        Timestamp BinaryReader::ReadTimestamp(const char * fieldName)
        {
            return impl->ReadTimestamp(fieldName);
        }

        int32_t BinaryReader::ReadTimestampArray(const char * fieldName, Timestamp * res, const int32_t len)
        {
            return impl->ReadTimestampArray(fieldName, res, len);
        }

        Time BinaryReader::ReadTime(const char* fieldName)
        {
            return impl->ReadTime(fieldName);
        }

        int32_t BinaryReader::ReadTimeArray(const char* fieldName, Time* res, const int32_t len)
        {
            return impl->ReadTimeArray(fieldName, res, len);
        }

        int32_t BinaryReader::ReadString(const char* fieldName, char* res, int32_t len)
        {
            return impl->ReadString(fieldName, res, len);
        }

        BinaryStringArrayReader BinaryReader::ReadStringArray(const char* fieldName)
        {
            int32_t size;

            int32_t id = impl->ReadStringArray(fieldName, &size);

            return BinaryStringArrayReader(impl, id, size);
        }

        BinaryEnumEntry BinaryReader::ReadBinaryEnum(const char* fieldName)
        {
            return impl->ReadBinaryEnum(fieldName);
        }

        CollectionType::Type BinaryReader::ReadCollectionType(const char* fieldName)
        {
            return impl->ReadCollectionType(fieldName);
        }

        int32_t BinaryReader::ReadCollectionSize(const char* fieldName)
        {
            return impl->ReadCollectionSize(fieldName);
        }

        BinaryRawReader BinaryReader::RawReader()
        {
            impl->SetRawMode();

            return BinaryRawReader(impl);
        }
    }
}
