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
namespace ignite
{
    struct CityKey
    {
        CityKey() : id(0)
        {
            // No-op.
        }

        CityKey(int32_t id, const std::string& name) :
            id(id),
            name(name)
        {
            // No-op.
        }

        std::string ToString() const
        {
            std::ostringstream oss;

            oss << "CityKey [id=" << id
                << ", name=" << name << ']';

            return oss.str();
        }

        int32_t id;
        std::string name;
    };
}

namespace ignite
{
    namespace binary
    {
        IGNITE_BINARY_TYPE_START(ignite::CityKey)

            typedef ignite::CityKey CityKey;

        IGNITE_BINARY_GET_TYPE_ID_AS_HASH(CityKey)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(CityKey)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_IS_NULL_FALSE(CityKey)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(CityKey)

            static void Write(BinaryWriter& writer, const ignite::CityKey& obj)
        {
            writer.WriteInt64("id", obj.id);
            writer.WriteString("name", obj.name);
        }

        static void Read(BinaryReader& reader, ignite::CityKey& dst)
        {
            dst.id = reader.ReadInt32("id");
            dst.name = reader.ReadString("name");
        }

        IGNITE_BINARY_TYPE_END
    }
};
