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

#include "ignite/binary/binary_raw_reader.h"
#include "ignite/impl/binary/binary_reader_impl.h"

using namespace ignite::impl::binary;

namespace ignite
{
    namespace binary
    {        
        BinaryRawReader::BinaryRawReader(BinaryReaderImpl* impl) : impl(impl)
        {
            // No-op.
        }
        
        int8_t BinaryRawReader::ReadInt8()
        {
            return impl->ReadInt8();
        }

        int32_t BinaryRawReader::ReadInt8Array(int8_t* res, const int32_t len)
        {
            return impl->ReadInt8Array(res, len);
        }
        
        bool BinaryRawReader::ReadBool()
        {
            return impl->ReadBool();
        }

        int32_t BinaryRawReader::ReadBoolArray(bool* res, const int32_t len)
        {
            return impl->ReadBoolArray(res, len);
        }

        int16_t BinaryRawReader::ReadInt16()
        {
            return impl->ReadInt16();
        }
        
        int32_t BinaryRawReader::ReadInt16Array(int16_t* res, const int32_t len)
        {
            return impl->ReadInt16Array(res, len);
        }

        uint16_t BinaryRawReader::ReadUInt16()
        {
            return impl->ReadUInt16();
        }

        int32_t BinaryRawReader::ReadUInt16Array(uint16_t* res, const int32_t len)
        {
            return impl->ReadUInt16Array(res, len);
        }

        int32_t BinaryRawReader::ReadInt32()
        {
            return impl->ReadInt32();
        }
        
        int32_t BinaryRawReader::ReadInt32Array(int32_t* res, const int32_t len)
        {
            return impl->ReadInt32Array(res, len);
        }

        int64_t BinaryRawReader::ReadInt64()
        {
            return impl->ReadInt64();
        }

        int32_t BinaryRawReader::ReadInt64Array(int64_t* res, const int32_t len)
        {
            return impl->ReadInt64Array(res, len);
        }

        float BinaryRawReader::ReadFloat()
        {
            return impl->ReadFloat();
        }
        
        int32_t BinaryRawReader::ReadFloatArray(float* res, const int32_t len)
        {
            return impl->ReadFloatArray(res, len);
        }

        double BinaryRawReader::ReadDouble()
        {
            return impl->ReadDouble();
        }
        
        int32_t BinaryRawReader::ReadDoubleArray(double* res, const int32_t len)
        {
            return impl->ReadDoubleArray(res, len);
        }
        
        Guid BinaryRawReader::ReadGuid()
        {
            return impl->ReadGuid();
        }

        int32_t BinaryRawReader::ReadGuidArray(Guid* res, const int32_t len)
        {
            return impl->ReadGuidArray(res, len);
        }

        Date BinaryRawReader::ReadDate()
        {
            return impl->ReadDate();
        }

        int32_t BinaryRawReader::ReadDateArray(Date* res, int32_t len)
        {
            return impl->ReadDateArray(res, len);
        }

        Timestamp BinaryRawReader::ReadTimestamp()
        {
            return impl->ReadTimestamp();
        }

        int32_t BinaryRawReader::ReadTimestampArray(Timestamp * res, int32_t len)
        {
            return impl->ReadTimestampArray(res, len);
        }

        Time BinaryRawReader::ReadTime()
        {
            return impl->ReadTime();
        }

        int32_t BinaryRawReader::ReadTimeArray(Time* res, int32_t len)
        {
            return impl->ReadTimeArray(res, len);
        }

        int32_t BinaryRawReader::ReadString(char* res, const int32_t len)
        {
            return impl->ReadString(res, len);
        }

        BinaryStringArrayReader BinaryRawReader::ReadStringArray()
        {
            int32_t size;

            int32_t id = impl->ReadStringArray(&size);

            return BinaryStringArrayReader(impl, id, size);
        }

        CollectionType::Type BinaryRawReader::ReadCollectionType()
        {
            return impl->ReadCollectionType();
        }

        int32_t BinaryRawReader::ReadCollectionSize()
        {
            return impl->ReadCollectionSize();
        }
    }
}