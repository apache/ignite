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
#include "ignite/portable/portable_raw_writer.h"

using namespace ignite::impl::portable;

namespace ignite
{
    namespace portable
    {
        PortableRawWriter::PortableRawWriter(PortableWriterImpl* impl) : impl(impl)
        {
            // No-op.
        }

        void PortableRawWriter::WriteInt8(const int8_t val)
        {
            impl->WriteInt8(val);
        }

        void PortableRawWriter::WriteInt8Array(const int8_t* val, const int32_t len)
        {
            impl->WriteInt8Array(val, len);
        }

        void PortableRawWriter::WriteBool(const bool val)
        {
            impl->WriteBool(val);
        }

        void PortableRawWriter::WriteBoolArray(const bool* val, const int32_t len)
        {            
            impl->WriteBoolArray(val, len);
        }

        void PortableRawWriter::WriteInt16(const int16_t val)
        {
            impl->WriteInt16(val);
        }

        void PortableRawWriter::WriteInt16Array(const int16_t* val, const int32_t len)
        {
            impl->WriteInt16Array(val, len);
        }

        void PortableRawWriter::WriteUInt16(const uint16_t val)
        {
            impl->WriteUInt16(val);
        }

        void PortableRawWriter::WriteUInt16Array(const uint16_t* val, const int32_t len)
        {
            impl->WriteUInt16Array(val, len);
        }

        void PortableRawWriter::WriteInt32(const int32_t val)
        {
            impl->WriteInt32(val);
        }

        void PortableRawWriter::WriteInt32Array(const int32_t* val, const int32_t len)
        {
            impl->WriteInt32Array(val, len);
        }

        void PortableRawWriter::WriteInt64(const int64_t val)
        {
            impl->WriteInt64(val);
        }

        void PortableRawWriter::WriteInt64Array(const int64_t* val, const int32_t len)
        {
            impl->WriteInt64Array(val, len);
        }

        void PortableRawWriter::WriteFloat(const float val)
        {
            impl->WriteFloat(val);
        }

        void PortableRawWriter::WriteFloatArray(const float* val, const int32_t len)
        {
            impl->WriteFloatArray(val, len);
        }

        void PortableRawWriter::WriteDouble(const double val)
        {
            impl->WriteDouble(val);
        }

        void PortableRawWriter::WriteDoubleArray(const double* val, const int32_t len)
        {
            impl->WriteDoubleArray(val, len);
        }

        void PortableRawWriter::WriteGuid(const Guid val)
        {
            impl->WriteGuid(val);
        }

        void PortableRawWriter::WriteGuidArray(const Guid* val, const int32_t len)
        {
            impl->WriteGuidArray(val, len);
        }

        void PortableRawWriter::WriteString(const char* val)
        {
            if (val)
                WriteString(val, static_cast<int32_t>(strlen(val)));
            else
                WriteNull();
        }

        void PortableRawWriter::WriteString(const char* val, const int32_t len)
        {
            impl->WriteString(val, len);
        }

        PortableStringArrayWriter PortableRawWriter::WriteStringArray()
        {
            int32_t id = impl->WriteStringArray();

            return PortableStringArrayWriter(impl, id);
        }

        void PortableRawWriter::WriteNull()
        {
            impl->WriteNull();
        }
    }
}