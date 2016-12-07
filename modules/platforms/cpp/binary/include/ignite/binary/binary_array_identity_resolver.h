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

/**
 * @file
 * Declares ignite::binary::BinaryArrayIdentityResolver class template.
 */

#ifndef _IGNITE_BINARY_BINARY_ARRAY_IDENTITY_RESOLVER
#define _IGNITE_BINARY_BINARY_ARRAY_IDENTITY_RESOLVER

#include <stdint.h>

#include <ignite/common/common.h>

#include <ignite/ignite_error.h>

#include <ignite/binary/binary_type.h>
#include <ignite/impl/interop/interop_memory.h>
#include <ignite/impl/interop/interop_output_stream.h>
#include <ignite/impl/binary/binary_writer_impl.h>

namespace ignite
{
    namespace binary
    {
        class BinaryWriter;
        class BinaryReader;

        /**
         * Binary type structure. Defines a set of functions required for type to be serialized and deserialized.
         */
        template<typename T>
        struct BinaryArrayIdentityResolver
        {
            /**
             * Get binary object hash code.
             *
             * @param obj Binary object.
             * @return Hash code.
             */
            int32_t GetHashCode(const T& obj)
            {
                using namespace impl::interop;
                using namespace impl::binary;

                BinaryType<T> bt;

                InteropUnpooledMemory mem(1024);
                InteropOutputStream out(&mem);

                BinaryWriterImpl writerImpl(&out, 0, 0, 0, 0);
                BinaryWriter writer(&writerImpl);

                bt.Write(writer, obj);

                out.Synchronize();

                GetDataHashCode(mem.Data(), mem.Length());

                return 0;
            }
        };
    }
}

#endif //_IGNITE_BINARY_BINARY_ARRAY_IDENTITY_RESOLVER
