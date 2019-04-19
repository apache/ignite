/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 #include <ignite/binary/binary_raw_reader.h>
 #include <ignite/binary/binary_raw_writer.h>

#include <ignite/impl/binary/binary_field_meta.h>

namespace ignite
{
    namespace impl
    {
        namespace binary
        {
            void BinaryFieldMeta::Write(ignite::binary::BinaryRawWriter& writer) const
            {
                writer.WriteInt32(typeId);
                writer.WriteInt32(fieldId);
            }

            void BinaryFieldMeta::Read(ignite::binary::BinaryRawReader& reader)
            {
                typeId = reader.ReadInt32();
                fieldId = reader.ReadInt32();
            }
        }
    }
}
