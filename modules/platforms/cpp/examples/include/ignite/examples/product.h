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

#ifndef _IGNITE_EXAMPLES_PRODUCT
#define _IGNITE_EXAMPLES_PRODUCT

#include <string>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

namespace ignite
{
    namespace examples
    {
        struct Product
        {
            Product() : productId(0), productPrice(0.0)
            {
                // No-op.
            }

            Product(int64_t productId, const std::string& productName, double productPrice) :
                productId(productId),
                productName(productName),
                productPrice(productPrice)
            {
                // No-op.
            }

            int64_t productId;
            std::string productName;
            double productPrice;
        };
    }
}

namespace ignite
{
    namespace binary
    {
        IGNITE_BINARY_TYPE_START(ignite::examples::Product)

            typedef ignite::examples::Product Product;

            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(Product)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(Product)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_GET_HASH_CODE_ZERO(Product)
            IGNITE_BINARY_IS_NULL_FALSE(Product)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(Product)

            void Write(BinaryWriter& writer, Product obj)
            {
                writer.WriteInt64("productId", obj.productId);
                writer.WriteString("productName", obj.productName);
                writer.WriteDouble("productPrice", obj.productPrice);
            }

            Product Read(BinaryReader& reader)
            {
                int64_t productId = reader.ReadInt64("productId");
                std::string productName = reader.ReadString("productName");
                double productPrice = reader.ReadDouble("productPrice");

                return Product(productId, productName, productPrice);
            }

        IGNITE_BINARY_TYPE_END
    }
};

#endif // _IGNITE_EXAMPLES_CLIENT