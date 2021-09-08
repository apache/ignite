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
    struct City
    {
        City() : population(0)
        {
            // No-op.
        }

        City(const int32_t population) :
            population(population)
        {
            // No-op.
        }

        std::string ToString() const
        {
            std::ostringstream oss;
            oss << "City [population=" << population << ']';
            return oss.str();
        }

        int32_t population;
    };
}

namespace ignite
{
    namespace binary
    {
        IGNITE_BINARY_TYPE_START(ignite::City)

            typedef ignite::City City;

        IGNITE_BINARY_GET_TYPE_ID_AS_HASH(City)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(City)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_IS_NULL_FALSE(City)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(City)

            static void Write(BinaryWriter& writer, const ignite::City& obj)
        {
            writer.WriteInt32("population", obj.population);
        }

        static void Read(BinaryReader& reader, ignite::City& dst)
        {
            dst.population = reader.ReadInt32("population");
        }

        IGNITE_BINARY_TYPE_END
    }
};
