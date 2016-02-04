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

#ifndef _IGNITE_EXAMPLES_CLIENT_PRODUCT_RELATION
#define _IGNITE_EXAMPLES_CLIENT_PRODUCT_RELATION

#include <string>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

namespace ignite
{
    namespace examples
    {
        struct ClientProductRelation
        {
            ClientProductRelation() : 
                clientProductId(0), clientId(0), productId(0)
            {
                // No-op.
            }

            ClientProductRelation(int64_t clientProductId, int64_t clientId, int64_t productId) :
                clientProductId(clientProductId),
                clientId(clientId),
                productId(productId)
            {
                // No-op.
            }

            int64_t clientProductId;
            int64_t clientId;
            int64_t productId;
        };
    }
}

namespace ignite
{
    namespace binary
    {
        IGNITE_BINARY_TYPE_START(ignite::examples::ClientProductRelation)

            typedef ignite::examples::ClientProductRelation ClientProductRelation;

            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(ClientProductRelation)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(ClientProductRelation)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_GET_HASH_CODE_ZERO(ClientProductRelation)
            IGNITE_BINARY_IS_NULL_FALSE(ClientProductRelation)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(ClientProductRelation)

            void Write(BinaryWriter& writer, ClientProductRelation obj)
            {
                writer.WriteInt64("clientProductId", obj.clientProductId);
                writer.WriteInt64("clientId", obj.clientId);
                writer.WriteInt64("productId", obj.productId);
            }

            ClientProductRelation Read(BinaryReader& reader)
            {
                int64_t clientProductId = reader.ReadInt64("clientProductId");
                int64_t clientId = reader.ReadInt64("clientId");
                int64_t productId = reader.ReadInt64("productId");

                return ClientProductRelation(clientProductId, clientId, productId);
            }

        IGNITE_BINARY_TYPE_END
    }
};

#endif // _IGNITE_EXAMPLES_CLIENT_PRODUCT_RELATION