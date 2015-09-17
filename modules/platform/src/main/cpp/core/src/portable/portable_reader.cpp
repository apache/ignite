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
#include "ignite/impl/portable/portable_reader_impl.h"
#include "ignite/portable/portable_reader.h"

using namespace ignite::impl::portable;

namespace ignite
{
    namespace portable
    {
        PortableReader::PortableReader(PortableReaderImpl* impl) : impl(impl)
        {
            // No-op.
        }
        
        int8_t PortableReader::ReadInt8(const char* fieldName)
        {
            return impl->ReadInt8(fieldName);
        }

        int32_t PortableReader::ReadInt8Array(const char* fieldName, int8_t* res, const int32_t len)
        {
            return impl->ReadInt8Array(fieldName, res, len);
        }

        bool PortableReader::ReadBool(const char* fieldName)
        {
            return impl->ReadBool(fieldName);
        }

        int32_t PortableReader::ReadBoolArray(const char* fieldName, bool* res, const int32_t len)
        {
            return impl->ReadBoolArray(fieldName, res, len);
        }

        int16_t PortableReader::ReadInt16(const char* fieldName)
        {
            return impl->ReadInt16(fieldName);
        }

        int32_t PortableReader::ReadInt16Array(const char* fieldName, int16_t* res, const int32_t len)
        {
            return impl->ReadInt16Array(fieldName, res, len);
        }

        uint16_t PortableReader::ReadUInt16(const char* fieldName)
        {
            return impl->ReadUInt16(fieldName);
        }

        int32_t PortableReader::ReadUInt16Array(const char* fieldName, uint16_t* res, const int32_t len)
        {
            return impl->ReadUInt16Array(fieldName, res, len);
        }

        int32_t PortableReader::ReadInt32(const char* fieldName)
        {
            return impl->ReadInt32(fieldName);
        }

        int32_t PortableReader::ReadInt32Array(const char* fieldName, int32_t* res, const int32_t len)
        {
            return impl->ReadInt32Array(fieldName, res, len);
        }

        int64_t PortableReader::ReadInt64(const char* fieldName)
        {
            return impl->ReadInt64(fieldName);
        }

        int32_t PortableReader::ReadInt64Array(const char* fieldName, int64_t* res, const int32_t len)
        {
            return impl->ReadInt64Array(fieldName, res, len);
        }

        float PortableReader::ReadFloat(const char* fieldName)
        {
            return impl->ReadFloat(fieldName);
        }

        int32_t PortableReader::ReadFloatArray(const char* fieldName, float* res, const int32_t len)
        {
            return impl->ReadFloatArray(fieldName, res, len);
        }

        double PortableReader::ReadDouble(const char* fieldName)
        {
            return impl->ReadDouble(fieldName);
        }

        int32_t PortableReader::ReadDoubleArray(const char* fieldName, double* res, const int32_t len)
        {
            return impl->ReadDoubleArray(fieldName, res, len);
        }

        Guid PortableReader::ReadGuid(const char* fieldName)
        {
            return impl->ReadGuid(fieldName);
        }

        int32_t PortableReader::ReadGuidArray(const char* fieldName, Guid* res, const int32_t len)
        {
            return impl->ReadGuidArray(fieldName, res, len);
        }
        
        int32_t PortableReader::ReadString(const char* fieldName, char* res, const int32_t len)
        {
            return impl->ReadString(fieldName, res, len);
        }

        PortableStringArrayReader PortableReader::ReadStringArray(const char* fieldName)
        {
            int32_t size;

            int32_t id = impl->ReadStringArray(fieldName, &size);

            return PortableStringArrayReader(impl, id, size);
        }

        PortableRawReader PortableReader::RawReader()
        {
            impl->SetRawMode();

            return PortableRawReader(impl);
        }
    }
}