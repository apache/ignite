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

#ifndef _IGNITE_EXAMPLES_CITY
#define _IGNITE_EXAMPLES_CITY

#include <string>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

namespace ignite
{
    namespace examples
    {
        struct City
        {
            City() : cityId(0), population(0)
            {
                // No-op.
            }

            City(int64_t cityId, const std::string& cityName, int32_t population) :
                cityId(cityId),
                cityName(cityName),
                population(population)
            {
                // No-op.
            }

            int64_t cityId;
            std::string cityName;
            int32_t population;
        };
    }
}

namespace ignite
{
    namespace binary
    {
        IGNITE_BINARY_TYPE_START(ignite::examples::City)

            typedef ignite::examples::City City;

            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(City)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(City)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_GET_HASH_CODE_ZERO(City)
            IGNITE_BINARY_IS_NULL_FALSE(City)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(City)

            void Write(BinaryWriter& writer, City obj)
            {
                writer.WriteInt64("cityId", obj.cityId);
                writer.WriteString("cityName", obj.cityName);
                writer.WriteInt32("population", obj.population);
            }

            City Read(BinaryReader& reader)
            {
                int64_t cityId = reader.ReadInt64("cityId");
                std::string cityName = reader.ReadString("cityName");
                int32_t population = reader.ReadInt32("population");

                return City(cityId, cityName, population);
            }

        IGNITE_BINARY_TYPE_END
    }
};

#endif // _IGNITE_EXAMPLES_CITY