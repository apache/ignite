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
 * Declares ignite::binary::BinaryType class template and helping macros
 * to declare binary type specialization for user types.
 */

#ifndef _IGNITE_BINARY_BINARY_TYPE
#define _IGNITE_BINARY_BINARY_TYPE

#include <stdint.h>

#include <ignite/common/common.h>

#include <ignite/impl/binary/binary_type_impl.h>

/**
 * @def IGNITE_BINARY_TYPE_START(T)
 * Start binary type definition.
 */
#define IGNITE_BINARY_TYPE_START(T) \
template<> \
struct BinaryType<T> \
{

/**
 * @def IGNITE_BINARY_TYPE_END
 * End binary type definition.
 */
#define IGNITE_BINARY_TYPE_END \
};

/**
 * @def IGNITE_BINARY_GET_TYPE_ID_AS_CONST(id)
 * Implementation of GetTypeId() which returns predefined constant.
 */
#define IGNITE_BINARY_GET_TYPE_ID_AS_CONST(id) \
static int32_t GetTypeId() \
{ \
    return id; \
}

/**
 * @def IGNITE_BINARY_GET_TYPE_ID_AS_HASH(typeName)
 * Implementation of GetTypeId() which returns hash of passed type name.
 */
#define IGNITE_BINARY_GET_TYPE_ID_AS_HASH(typeName) \
static int32_t GetTypeId() \
{ \
    return GetBinaryStringHashCode(#typeName); \
}

/**
 * @def IGNITE_BINARY_GET_TYPE_NAME_AS_IS(typeName)
 * Implementation of GetTypeName() which returns type name as is.
 */
#define IGNITE_BINARY_GET_TYPE_NAME_AS_IS(typeName) \
static void GetTypeName(std::string& dst) \
{ \
    dst = #typeName; \
}

/**
 * @def IGNITE_BINARY_GET_FIELD_ID_AS_HASH
 * Default implementation of GetFieldId() function which returns Java-way hash code of the string.
 */
#define IGNITE_BINARY_GET_FIELD_ID_AS_HASH \
static int32_t GetFieldId(const char* name) \
{ \
    return GetBinaryStringHashCode(name); \
}

/**
 * @def IGNITE_BINARY_IS_NULL_FALSE(T)
 * Implementation of IsNull() function which always returns false.
 */
#define IGNITE_BINARY_IS_NULL_FALSE(T) \
static bool IsNull(const T&) \
{ \
    return false; \
}

/**
 * @def IGNITE_BINARY_IS_NULL_IF_NULLPTR(T)
 * Implementation of IsNull() function which return true if passed object is null pointer.
 */
#define IGNITE_BINARY_IS_NULL_IF_NULLPTR(T) \
static bool IsNull(const T& obj) \
{ \
    return obj; \
}

/**
 * @def IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(T)
 * Implementation of GetNull() function which returns an instance created with default constructor.
 */
#define IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(T) \
static void GetNull(T& dst) \
{ \
    dst = T(); \
}

/**
 * @def IGNITE_BINARY_GET_NULL_NULLPTR(T)
 * Implementation of GetNull() function which returns NULL pointer.
 */
#define IGNITE_BINARY_GET_NULL_NULLPTR(T) \
static void GetNull(T& dst) \
{ \
    dst = 0; \
}


namespace ignite
{
    namespace binary
    {
        class BinaryWriter;
        class BinaryReader;

        /**
         * Get binary string hash code.
         *
         * @param val Value.
         * @return Hash code.
         */
        IGNITE_IMPORT_EXPORT int32_t GetBinaryStringHashCode(const char* val);

        /**
         * Binary type structure. Defines a set of functions required for type to be serialized and deserialized.
         */
        template<typename T>
        struct IGNITE_IMPORT_EXPORT BinaryType { };

        /**
         * Default implementations of BinaryType hashing functions.
         */
        template<typename T>
        struct IGNITE_IMPORT_EXPORT BinaryTypeDefaultHashing
        {
            /**
             * Get binary object type ID.
             *
             * @return Type ID.
             */
            static int32_t GetTypeId()
            {
                std::string typeName;
                BinaryType<T>::GetTypeName(typeName);

                return GetBinaryStringHashCode(typeName.c_str());
            }

            /**
             * Get binary object field ID.
             *
             * @param name Field name.
             * @return Field ID.
             */
            static int32_t GetFieldId(const char* name)
            {
                return GetBinaryStringHashCode(name);
            }
        };

        /**
         * Default implementations of BinaryType methods for non-null type.
         */
        template<typename T>
        struct IGNITE_IMPORT_EXPORT BinaryTypeNonNullableType
        {
            /**
             * Check whether passed binary object should be interpreted as NULL.
             *
             * @return True if binary object should be interpreted as NULL.
             */
            static bool IsNull(const T&)
            {
                return false;
            }

            /**
             * Get NULL value for the given binary type.
             *
             * @param dst Null value for the type.
             */
            static void GetNull(T& dst)
            {
                dst = T();
            }
        };

        /**
         * Default implementations of BinaryType hashing functions and non-null type behaviour.
         */
        template<typename T>
        struct IGNITE_IMPORT_EXPORT BinaryTypeDefaultAll :
            BinaryTypeDefaultHashing<T>,
            BinaryTypeNonNullableType<T> { };

        /**
         * BinaryType template specialization for pointers.
         */
        template <typename T>
        struct IGNITE_IMPORT_EXPORT BinaryType<T*>
        {
            /** Actual type. */
            typedef BinaryType<T> BinaryTypeDereferenced;

            /**
             * Get binary object type ID.
             *
             * @return Type ID.
             */
            static int32_t GetTypeId()
            {
                return BinaryTypeDereferenced::GetTypeId();
            }

            /**
             * Get binary object type name.
             *
             * @param dst Output type name.
             */
            static void GetTypeName(std::string& dst)
            {
                BinaryTypeDereferenced::GetTypeName(dst);
            }

            /**
             * Get binary object field ID.
             *
             * @param name Field name.
             * @return Field ID.
             */
            static int32_t GetFieldId(const char* name)
            {
                return BinaryTypeDereferenced::GetFieldId(name);
            }

            /**
             * Write binary object.
             *
             * @param writer Writer.
             * @param obj Object.
             */
            static void Write(BinaryWriter& writer, T* const& obj)
            {
                BinaryTypeDereferenced::Write(writer, *obj);
            }

            /**
             * Read binary object.
             *
             * @param reader Reader.
             * @param dst Output object.
             */
            static void Read(BinaryReader& reader, T*& dst)
            {
                dst = new T();

                BinaryTypeDereferenced::Read(reader, *dst);
            }

            /**
             * Check whether passed binary object should be interpreted as NULL.
             *
             * @param obj Binary object to test.
             * @return True if binary object should be interpreted as NULL.
             */
            static bool IsNull(T* const& obj)
            {
                return !obj || BinaryTypeDereferenced::IsNull(*obj);
            }

            /**
             * Get NULL value for the given binary type.
             *
             * @param dst NULL value for the type.
             */
            static void GetNull(T*& dst)
            {
                dst = 0;
            }
        };
    }
}

#endif //_IGNITE_BINARY_BINARY_TYPE
