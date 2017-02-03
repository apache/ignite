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

#include <ignite/binary/binary_object.h>
#include <ignite/impl/binary/binary_utils.h>

#include <ignite/binary/binary_array_identity_resolver.h>

namespace ignite
{
    namespace binary
    {
        BinaryArrayIdentityResolver::BinaryArrayIdentityResolver()
        {
            // No-op.
        }

        BinaryArrayIdentityResolver::~BinaryArrayIdentityResolver()
        {
            // No-op.
        }

        int32_t BinaryArrayIdentityResolver::GetHashCode(const BinaryObject& obj)
        {
            return impl::binary::BinaryUtils::GetDataHashCode(obj.GetData(), obj.GetLength());
        }
    }
}
