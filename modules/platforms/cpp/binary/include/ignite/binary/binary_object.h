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
 * Declares ignite::binary::BinaryObject class.
 */

#ifndef _IGNITE_BINARY_BINARY_OBJECT
#define _IGNITE_BINARY_BINARY_OBJECT

#include <stdint.h>

#include <ignite/impl/interop/interop.h>
#include <ignite/impl/binary/binary_reader_impl.h>


namespace ignite
{
    namespace binary
    {
        class BinaryObject
        {
        public:
            BinaryObject(impl::interop::InteropMemory& mem, int32_t start) :
                mem(mem),
                start(start)
            {
                // No-op.
            }

            template<typename T>
            T Deserialize()
            {
                impl::interop::InteropInputStream stream(&mem);

                stream.Position(start);
                impl::binary::BinaryReaderImpl reader(&stream);

                return reader.ReadObject<T>();
            }

        private:
            /** Underlying object memory. */
            impl::interop::InteropMemory& mem;

            /** Object starting position in memory. */
            int32_t start;
        };
    }
}

#endif //_IGNITE_BINARY_BINARY_OBJECT