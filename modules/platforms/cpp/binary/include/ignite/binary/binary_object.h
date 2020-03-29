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
    namespace impl
    {
        namespace binary
        {
            class BinaryWriterImpl;
        }
    }

    namespace binary
    {
        /**
         * Binary object.
         *
         * This is a thin wrapper over the memory area that contains serialized
         * binary object. Provides method that allows deserialize object.
         */
        class IGNITE_IMPORT_EXPORT BinaryObject
        {
            friend class ignite::impl::binary::BinaryWriterImpl;
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
            }

            /**
             * Direct constructor.
             * Constructs binary object without any safety checks.
             *
             * @param mem Binary object memory.
             * @param start Object starting position in memory.
             * @param idRslvr ID resolver.
             * @param metaMgr Metadata manager.
             */
            BinaryObject(impl::interop::InteropMemory& mem, int32_t start,
                impl::binary::BinaryIdResolver* idRslvr, impl::binary::BinaryTypeManager* metaMgr) :
                impl(mem, start, idRslvr, metaMgr)
            {
                // No-op.
            }
            /// @endcond

            /**
             * Copy constructor.
             *
             * @param other Another instance.
             */
            BinaryObject(const BinaryObject& other) :
                impl(other.impl)
            {
                // No-op.
            }

            /**
             * Assignment operator.
             *
             * @param other Another instance.
             * @return *this.
             */
            BinaryObject& operator=(const BinaryObject& other)
            {
                impl = other.impl;

                return *this;
            }

            /**
             * Deserialize object.
             * @throw IgniteError if the object can not be deserialized to
             *     specified type.
             *
             * @return Deserialized value.
             */
            template<typename T>
            T Deserialize() const
            {
                return impl.Deserialize<T>();
            }

            /**
             * Get field.
             * @throw IgniteError if the there is no specified field or if it
             *     is not of the specified type.
             *
             * @param name Field name.
             * @return Field value.
             */
            template<typename T>
            T GetField(const char* name) const
            {
                return impl.GetField<T>(name);
            }

            /**
             * Check if the binary object has the specified field.
             *
             * @param name Field name.
             * @return True if the binary object has the specified field and
             *     false otherwise.
             */
            bool HasField(const char* name) const
            {
                return impl.HasField(name);
            }

        private:
            /** Implementation. */
            impl::binary::BinaryObjectImpl impl;
        };

        /* Specialization */
        template<>
        inline BinaryObject BinaryObject::GetField(const char* name) const
        {
            return BinaryObject(impl.GetField<impl::binary::BinaryObjectImpl>(name));
        }
    }
}

#endif //_IGNITE_BINARY_BINARY_OBJECT