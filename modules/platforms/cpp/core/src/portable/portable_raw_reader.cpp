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
#include "ignite/portable/portable_raw_reader.h"

using namespace ignite::impl::portable;

namespace ignite
{
    namespace portable
    {        
        PortableRawReader::PortableRawReader(PortableReaderImpl* impl) : impl(impl)
        {
            // No-op.
        }
        
        int8_t PortableRawReader::ReadInt8()
        {
            return impl->ReadInt8();
        }

        int32_t PortableRawReader::ReadInt8Array(int8_t* res, const int32_t len)
        {
            return impl->ReadInt8Array(res, len);
        }
        
        bool PortableRawReader::ReadBool()
        {
            return impl->ReadBool();
        }

        int32_t PortableRawReader::ReadBoolArray(bool* res, const int32_t len)
        {
            return impl->ReadBoolArray(res, len);
        }

        int16_t PortableRawReader::ReadInt16()
        {
            return impl->ReadInt16();
        }
        
        int32_t PortableRawReader::ReadInt16Array(int16_t* res, const int32_t len)
        {
            return impl->ReadInt16Array(res, len);
        }

        uint16_t PortableRawReader::ReadUInt16()
        {
            return impl->ReadUInt16();
        }

        int32_t PortableRawReader::ReadUInt16Array(uint16_t* res, const int32_t len)
        {
            return impl->ReadUInt16Array(res, len);
        }

        int32_t PortableRawReader::ReadInt32()
        {
            return impl->ReadInt32();
        }
        
        int32_t PortableRawReader::ReadInt32Array(int32_t* res, const int32_t len)
        {
            return impl->ReadInt32Array(res, len);
        }

        int64_t PortableRawReader::ReadInt64()
        {
            return impl->ReadInt64();
        }

        int32_t PortableRawReader::ReadInt64Array(int64_t* res, const int32_t len)
        {
            return impl->ReadInt64Array(res, len);
        }

        float PortableRawReader::ReadFloat()
        {
            return impl->ReadFloat();
        }
        
        int32_t PortableRawReader::ReadFloatArray(float* res, const int32_t len)
        {
            return impl->ReadFloatArray(res, len);
        }

        double PortableRawReader::ReadDouble()
        {
            return impl->ReadDouble();
        }
        
        int32_t PortableRawReader::ReadDoubleArray(double* res, const int32_t len)
        {
            return impl->ReadDoubleArray(res, len);
        }
        
        Guid PortableRawReader::ReadGuid()
        {
            return impl->ReadGuid();
        }

        int32_t PortableRawReader::ReadGuidArray(Guid* res, const int32_t len)
        {
            return impl->ReadGuidArray(res, len);
        }        

        int32_t PortableRawReader::ReadString(char* res, const int32_t len)
        {
            return impl->ReadString(res, len);
        }

        PortableStringArrayReader PortableRawReader::ReadStringArray()
        {
            int32_t size;

            int32_t id = impl->ReadStringArray(&size);

            return PortableStringArrayReader(impl, id, size);
        }
    }
}