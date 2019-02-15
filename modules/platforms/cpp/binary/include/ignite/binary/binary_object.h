/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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