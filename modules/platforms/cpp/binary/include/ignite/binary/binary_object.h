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

#include <ignite/impl/binary/binary_object_impl.h>

namespace ignite
{
    namespace binary
    {
        class BinaryArrayIdentityResolver;

        /**
         * Binary object.
         *
         * This is a thin wrapper over the memory area that contains serialized
         * binary object. Provides method that allows deserialize object.
         */
        class IGNITE_IMPORT_EXPORT BinaryObject
        {
            friend class BinaryArrayIdentityResolver;
        public:
            /// @cond INTERNAL
            /**
             * Constructor.
             *
             * @param impl Implementation.
             */
            BinaryObject(const impl::binary::BinaryObjectImpl& impl) :
                impl(impl)
            {
                // No-op.
            };

            /**
             * Constructor.
             *
             * @param mem Binary object memory.
             * @param start Object starting position in memory.
             */
            BinaryObject(impl::interop::InteropMemory& mem, int32_t start) :
                impl(mem, start)
            {
                // No-op.
            };
            /// @endcond

            /**
             * Deserialize object.
             * @throw IgniteError if the object can not be deserialized to specified type.
             *
             * @return Deserialized value.
             */
            template<typename T>
            T Deserialize() const
            {
                return impl.Deserialize<T>();
            }

        private:
            /** Implementation. */
            impl::binary::BinaryObjectImpl impl;

            IGNITE_NO_COPY_ASSIGNMENT(BinaryObject)
        };
    }
}

#endif //_IGNITE_BINARY_BINARY_OBJECT