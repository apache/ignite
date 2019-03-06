/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

#include "ignite/impl/binary/binary_writer_impl.h"
#include "ignite/binary/binary_raw_writer.h"

using namespace ignite::impl::binary;

namespace ignite
{
    namespace binary
    {
        BinaryRawWriter::BinaryRawWriter(BinaryWriterImpl* impl) : impl(impl)
        {
            // No-op.
        }

        void BinaryRawWriter::WriteInt8(int8_t val)
        {
            impl->WriteInt8(val);
        }

        void BinaryRawWriter::WriteInt8Array(const int8_t* val, int32_t len)
        {
            impl->WriteInt8Array(val, len);
        }

        void BinaryRawWriter::WriteBool(bool val)
        {
            impl->WriteBool(val);
        }

        void BinaryRawWriter::WriteBoolArray(const bool* val, int32_t len)
        {            
            impl->WriteBoolArray(val, len);
        }

        void BinaryRawWriter::WriteInt16(int16_t val)
        {
            impl->WriteInt16(val);
        }

        void BinaryRawWriter::WriteInt16Array(const int16_t* val, int32_t len)
        {
            impl->WriteInt16Array(val, len);
        }

        void BinaryRawWriter::WriteUInt16(uint16_t val)
        {
            impl->WriteUInt16(val);
        }

        void BinaryRawWriter::WriteUInt16Array(const uint16_t* val, int32_t len)
        {
            impl->WriteUInt16Array(val, len);
        }

        void BinaryRawWriter::WriteInt32(int32_t val)
        {
            impl->WriteInt32(val);
        }

        void BinaryRawWriter::WriteInt32Array(const int32_t* val, int32_t len)
        {
            impl->WriteInt32Array(val, len);
        }

        void BinaryRawWriter::WriteInt64(int64_t val)
        {
            impl->WriteInt64(val);
        }

        void BinaryRawWriter::WriteInt64Array(const int64_t* val, int32_t len)
        {
            impl->WriteInt64Array(val, len);
        }

        void BinaryRawWriter::WriteFloat(float val)
        {
            impl->WriteFloat(val);
        }

        void BinaryRawWriter::WriteFloatArray(const float* val, int32_t len)
        {
            impl->WriteFloatArray(val, len);
        }

        void BinaryRawWriter::WriteDouble(double val)
        {
            impl->WriteDouble(val);
        }

        void BinaryRawWriter::WriteDoubleArray(const double* val, int32_t len)
        {
            impl->WriteDoubleArray(val, len);
        }

        void BinaryRawWriter::WriteGuid(const Guid& val)
        {
            impl->WriteGuid(val);
        }

        void BinaryRawWriter::WriteGuidArray(const Guid* val, int32_t len)
        {
            impl->WriteGuidArray(val, len);
        }

        void BinaryRawWriter::WriteDate(const Date& val)
        {
            impl->WriteDate(val);
        }

        void BinaryRawWriter::WriteDateArray(const Date* val, int32_t len)
        {
            impl->WriteDateArray(val, len);
        }

        void BinaryRawWriter::WriteTimestamp(const Timestamp& val)
        {
            impl->WriteTimestamp(val);
        }

        void BinaryRawWriter::WriteTimestampArray(const Timestamp* val, int32_t len)
        {
            impl->WriteTimestampArray(val, len);
        }

        void BinaryRawWriter::WriteTime(const Time& val)
        {
            impl->WriteTime(val);
        }

        void BinaryRawWriter::WriteTimeArray(const Time* val, const int32_t len)
        {
            impl->WriteTimeArray(val, len);
        }

        void BinaryRawWriter::WriteString(const char* val)
        {
            if (val)
                WriteString(val, static_cast<int32_t>(strlen(val)));
            else
                WriteNull();
        }

        void BinaryRawWriter::WriteString(const char* val, int32_t len)
        {
            impl->WriteString(val, len);
        }

        BinaryStringArrayWriter BinaryRawWriter::WriteStringArray()
        {
            int32_t id = impl->WriteStringArray();

            return BinaryStringArrayWriter(impl, id);
        }

        void BinaryRawWriter::WriteNull()
        {
            impl->WriteNull();
        }
    }
}