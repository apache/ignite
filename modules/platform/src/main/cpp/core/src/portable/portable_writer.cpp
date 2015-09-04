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
#include "ignite/portable/portable_writer.h"

using namespace ignite::impl::portable;

namespace ignite
{
    namespace portable
    {
        PortableWriter::PortableWriter(PortableWriterImpl* impl) : impl(impl)
        {
            // No-op.
        }

        void PortableWriter::WriteInt8(const char* fieldName, const int8_t val)
        {
            impl->WriteInt8(fieldName, val);
        }

        void PortableWriter::WriteInt8Array(const char* fieldName, const int8_t* val, const int32_t len)
        {
            impl->WriteInt8Array(fieldName, val, len);
        }

        void PortableWriter::WriteBool(const char* fieldName, const bool val)
        {
            impl->WriteBool(fieldName, val);
        }

        void PortableWriter::WriteBoolArray(const char* fieldName, const bool* val, const int32_t len)
        {
            impl->WriteBoolArray(fieldName, val, len);
        }

        void PortableWriter::WriteInt16(const char* fieldName, const int16_t val)
        {
            impl->WriteInt16(fieldName, val);
        }

        void PortableWriter::WriteInt16Array(const char* fieldName, const int16_t* val, const int32_t len)
        {
            impl->WriteInt16Array(fieldName, val, len);
        }

        void PortableWriter::WriteUInt16(const char* fieldName, const uint16_t val)
        {
            impl->WriteUInt16(fieldName, val);
        }

        void PortableWriter::WriteUInt16Array(const char* fieldName, const uint16_t* val, const int32_t len)
        {
            impl->WriteUInt16Array(fieldName, val, len);
        }

        void PortableWriter::WriteInt32(const char* fieldName, const int32_t val)
        {
            impl->WriteInt32(fieldName, val);
        }

        void PortableWriter::WriteInt32Array(const char* fieldName, const int32_t* val, const int32_t len)
        {
            impl->WriteInt32Array(fieldName, val, len);
        }

        void PortableWriter::WriteInt64(const char* fieldName, const int64_t val)
        {
            impl->WriteInt64(fieldName, val);
        }

        void PortableWriter::WriteInt64Array(const char* fieldName, const int64_t* val, const int32_t len)
        {
            impl->WriteInt64Array(fieldName, val, len);
        }

        void PortableWriter::WriteFloat(const char* fieldName, const float val)
        {
            impl->WriteFloat(fieldName, val);
        }

        void PortableWriter::WriteFloatArray(const char* fieldName, const float* val, const int32_t len)
        {
            impl->WriteFloatArray(fieldName, val, len);
        }

        void PortableWriter::WriteDouble(const char* fieldName, const double val)
        {
            impl->WriteDouble(fieldName, val);
        }

        void PortableWriter::WriteDoubleArray(const char* fieldName, const double* val, const int32_t len)
        {
            impl->WriteDoubleArray(fieldName, val, len);
        }

        void PortableWriter::WriteGuid(const char* fieldName, const Guid val)
        {
            impl->WriteGuid(fieldName, val);
        }

        void PortableWriter::WriteGuidArray(const char* fieldName, const Guid* val, const int32_t len)
        {
            impl->WriteGuidArray(fieldName, val, len);
        }

        void PortableWriter::WriteString(const char* fieldName, const char* val)
        {
            if (val)
                WriteString(fieldName, val, static_cast<int32_t>(strlen(val)));
            else
                WriteNull(fieldName);
        }

        void PortableWriter::WriteString(const char* fieldName, const char* val, const int32_t len)
        {
            impl->WriteString(fieldName, val, len);
        }

        PortableStringArrayWriter PortableWriter::WriteStringArray(const char* fieldName)
        {
            int32_t id = impl->WriteStringArray(fieldName);

            return PortableStringArrayWriter(impl, id);
        }

        void PortableWriter::WriteNull(const char* fieldName)
        {
            impl->WriteNull(fieldName);
        }

        PortableRawWriter PortableWriter::RawWriter()
        {
            impl->SetRawMode();

            return PortableRawWriter(impl);
        }
    }
}