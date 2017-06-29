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

#include <cstring>

#include <ignite/binary/binary_reader.h>
#include <ignite/binary/binary_writer.h>
#include <ignite/impl/binary/binary_type_impl.h>

namespace ignite
{
    namespace binary
    {
        int32_t BinaryType<IgniteError>::GetTypeId()
        {
            return GetBinaryStringHashCode("IgniteError");
        }

        int32_t BinaryType<IgniteError>::GetFieldId(const char* name)
        {
            return GetBinaryStringHashCode(name);
        }

        void BinaryType<IgniteError>::GetNull(IgniteError& dst)
        {
            dst = IgniteError(0, 0);
        }

        void BinaryType<IgniteError>::Write(BinaryWriter& writer, const IgniteError& obj)
        {
            BinaryRawWriter raw = writer.RawWriter();

            raw.WriteInt32(obj.GetCode());
            raw.WriteString(obj.GetText(), static_cast<int32_t>(strlen(obj.GetText())));
        }

        void BinaryType<IgniteError>::Read(BinaryReader& reader, IgniteError& dst)
        {
            BinaryRawReader raw = reader.RawReader();

            int32_t code = raw.ReadInt32();
            std::string msg = raw.ReadObject<std::string>();

            dst = IgniteError(code, msg.c_str());
        }
    }
}
