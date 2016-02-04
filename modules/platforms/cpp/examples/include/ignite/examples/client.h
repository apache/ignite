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

#ifndef _IGNITE_EXAMPLES_CLIENT
#define _IGNITE_EXAMPLES_CLIENT

#include <string>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

#include "ignite/examples/city.h"

namespace ignite
{
    namespace examples
    {
        struct Client
        {
            Client() : clientId(0), cityId(0)
            {
                // No-op.
            }

            Client(int64_t clientId, int64_t cityId, const std::string& firstName,
                const std::string& lastName) :
                clientId(clientId),
                cityId(cityId),
                firstName(firstName),
                lastName(lastName)
            {
                // No-op.
            }

            Client(int64_t clientId, const City& city, const std::string& firstName,
                const std::string& lastName) :
                clientId(clientId),
                cityId(city.cityId),
                firstName(firstName),
                lastName(lastName)
            {
                // No-op.
            }

            int64_t clientId;
            int64_t cityId;
            std::string firstName;
            std::string lastName;
        };
    }
}

namespace ignite
{
    namespace binary
    {
        IGNITE_BINARY_TYPE_START(ignite::examples::Client)

            typedef ignite::examples::Client Client;

            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(Client)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(Client)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_GET_HASH_CODE_ZERO(Client)
            IGNITE_BINARY_IS_NULL_FALSE(Client)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(Client)

            void Write(BinaryWriter& writer, Client obj)
            {
                writer.WriteInt64("clientId", obj.clientId);
                writer.WriteInt64("cityId", obj.cityId);
                writer.WriteString("firstName", obj.firstName);
                writer.WriteString("lastName", obj.lastName);
            }

            Client Read(BinaryReader& reader)
            {
                int64_t clientId = reader.ReadInt64("clientId");
                int64_t cityId = reader.ReadInt64("cityId");
                std::string firstName = reader.ReadString("firstName");
                std::string lastName = reader.ReadString("lastName");

                return Client(clientId, cityId, firstName, lastName);
            }

        IGNITE_BINARY_TYPE_END
    }
};

#endif // _IGNITE_EXAMPLES_CLIENT