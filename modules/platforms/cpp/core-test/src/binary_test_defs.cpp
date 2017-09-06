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

#include "ignite/impl/interop/interop.h"
#include "ignite/binary/binary.h"

#include "ignite/binary_test_defs.h"

using namespace ignite;
using namespace ignite::impl::interop;
using namespace ignite::impl::binary;
using namespace ignite::binary;

namespace ignite_test
{
    namespace core
    {
        namespace binary
        {
            BinaryInner::BinaryInner() : val(0)
            {
                // No-op.
            }

            BinaryInner::BinaryInner(int32_t val) : val(val)
            {
                // No-op.
            }

            int32_t BinaryInner::GetValue() const
            {
                return val;
            }

            BinaryOuter::BinaryOuter()
            {
                // No-op.
            }

            BinaryOuter::BinaryOuter(int32_t valIn, int32_t valOut) : inner(valIn), val(valOut)
            {
                // No-op.
            }

            BinaryInner BinaryOuter::GetInner() const
            {
                return inner;
            }

            int32_t BinaryOuter::GetValue() const
            {
                return val;
            }
        }
    }
}