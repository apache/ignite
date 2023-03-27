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

#ifndef _IGNITE_IMPL_BINARY_BINARY_OBJECT_IMPL
#define _IGNITE_IMPL_BINARY_BINARY_OBJECT_IMPL

#include <stdint.h>

#include <ignite/binary/binary_type.h>

#include <ignite/impl/interop/interop_input_stream.h>
#include <ignite/impl/binary/binary_reader_impl.h>
#include <ignite/impl/binary/binary_type_manager.h>

namespace ignite
{
    namespace impl
    {
        namespace binary
        {
            /**
             * Binary object implementation.
             *
             * This is a thin wrapper over the memory area that contains serialized
             * binary object. Provides some methods that allow to access object's
             * data without deserialization. Also provides method that allows
             * deserialize object.
             */
            class IGNITE_IMPORT_EXPORT BinaryObjectImpl
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param mem Binary object memory.
                 * @param start Object starting position in memory.
                 * @param idRslvr ID resolver. If null metaMgr is going to be
                 *  used to extract meta for the type.
                 * @param metaMgr Metadata manager. Can be null if you are not
                 *  going to access fields by name.
                 */
                BinaryObjectImpl(interop::InteropMemory& mem, int32_t start, BinaryIdResolver* idRslvr, BinaryTypeManager* metaMgr);

                /**
                 * Destructor.
                 */
                ~BinaryObjectImpl();

                /**
                 * Copy constructor.
                 *
                 * @param other Another instance.
                 */
                BinaryObjectImpl(const BinaryObjectImpl& other);

                /**
                 * Assignment operator.
                 *
                 * @param other Another instance.
                 * @return *this.
                 */
                BinaryObjectImpl& operator=(const BinaryObjectImpl& other);

                /**
                 * Swap contents with another instance.
                 *
                 * @param other Another instance.
                 */
                void Swap(BinaryObjectImpl& other);

                /**
                 * Create from InteropMemory instance.
                 * @throw IgniteError if the memory at the specified offset
                 *    is not a valid BinaryObject.
                 *
                 * @param mem Memory.
                 * @param offset Offset in memory.
                 * @param metaMgr Metadata manager. Can be null if you are not
                 *  going to access fields by name.
                 * @return BinaryObjectImpl instance.
                 */
                static BinaryObjectImpl FromMemory(interop::InteropMemory& mem, int32_t offset, BinaryTypeManager* metaMgr);

                /**
                 * Deserialize object.
                 * @throw IgniteError if the object can not be deserialized to specified type.
                 *
                 * @return Deserialized value.
                 */
                template<typename T>
                T Deserialize() const
                {
                    int32_t actualTypeId = GetTypeId();
                    int32_t requestedTypeId = ignite::binary::BinaryType<T>::GetTypeId();

                    if (requestedTypeId != actualTypeId)
                    {
                        IGNITE_ERROR_FORMATTED_3(ignite::IgniteError::IGNITE_ERR_BINARY,
                            "Trying to deserialize binary object to a different type", "memPtr", mem->PointerLong(),
                            "actualTypeId", actualTypeId, "requestedTypeId", requestedTypeId);
                    }

                    interop::InteropInputStream stream(mem);

                    stream.Position(start);
                    BinaryReaderImpl reader(&stream);

                    return reader.ReadTopObject<T>();
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
                    CheckIdResolver();

                    int32_t fieldId = idRslvr->GetFieldId(GetTypeId(), name);
                    int32_t pos = FindField(fieldId);

                    if (pos == -1)
                        return T();

                    interop::InteropInputStream stream(mem);

                    stream.Position(pos);
                    BinaryReaderImpl reader(&stream);

                    return reader.ReadTopObject<T>();
                }

                /**
                 * Check if the binary object has the specified field.
                 *
                 * @param name Field name.
                 * @return True if the binary object has the specified field and
                 *     false otherwise.
                 */
                bool HasField(const char* name) const;

                /**
                 * Gets the value of underlying enum in int form.
                 *
                 * @return The value of underlying enum in int form.
                 */
                int32_t GetEnumValue() const;

                /**
                 * Get binary object field.
                 *
                 * @warning Works only if all object fields are objects.
                 *     Otherwise behavior is undefined.
                 *
                 * @param idx Field index. Starts from 0.
                 * @return Binary object field.
                 */
                BinaryObjectImpl GetField(int32_t idx);

                /**
                 * Get object data.
                 *
                 * @return Pointer to object data.
                 */
                const int8_t* GetData() const;

                /**
                 * Get object length.
                 * @throw IgniteError if the object is not in a valid state.
                 *
                 * @return Object length.
                 */
                int32_t GetLength() const;

                /**
                 * Get object hash code.
                 * @throw IgniteError if the object is not in a valid state.
                 *
                 * @return Object hash code.
                 */
                int32_t GetHashCode() const;

                /**
                 * Get type ID.
                 * @throw IgniteError if the object is not in a valid state.
                 *
                 * @return Type ID.
                 */
                int32_t GetTypeId() const;

            private:
                /**
                 * Find field position in memory.
                 *
                 * @param fieldId Field Identifier.
                 * @return Field position on success and negative value on failure.
                 */
                int32_t FindField(const int32_t fieldId) const;

                /**
                 * Checks that id resolver is set.
                 *
                 * @throw IgniteError if idRslvr is not set.
                 */
                void CheckIdResolver() const;

                /** Underlying object memory. */
                interop::InteropMemory* mem;

                /** Object starting position in memory. */
                int32_t start;

                /** ID resolver. */
                mutable BinaryIdResolver* idRslvr;

                /** Type manager. */
                BinaryTypeManager* metaMgr;

                /** If object is in binary wrapper. */
                bool binary;
            };

            /* Specialization */
            template<>
            IGNITE_IMPORT_EXPORT BinaryObjectImpl BinaryObjectImpl::GetField(const char* name) const;
        }
    }
}

#endif //_IGNITE_IMPL_BINARY_BINARY_OBJECT_IMPL
