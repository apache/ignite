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
 * to declare binary type specialisation for user types.
 */

#ifndef _IGNITE_BINARY_BINARY_TYPE
#define _IGNITE_BINARY_BINARY_TYPE

#include <stdint.h>

#include <ignite/common/common.h>

#include <ignite/ignite_error.h>

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
int32_t GetTypeId() \
{ \
    return id; \
}

/**
 * @def IGNITE_BINARY_GET_TYPE_ID_AS_HASH(typeName)
 * Implementation of GetTypeId() which returns hash of passed type name.
 */
#define IGNITE_BINARY_GET_TYPE_ID_AS_HASH(typeName) \
int32_t GetTypeId() \
{ \
    return GetBinaryStringHashCode(#typeName); \
}

/**
 * @def IGNITE_BINARY_GET_TYPE_NAME_AS_IS(typeName)
 * Implementation of GetTypeName() which returns type name as is.
 */
#define IGNITE_BINARY_GET_TYPE_NAME_AS_IS(typeName) \
std::string GetTypeName() \
{ \
    return #typeName; \
}

/**
 * @def IGNITE_BINARY_GET_FIELD_ID_AS_HASH
 * Default implementation of GetFieldId() function which returns Java-way hash code of the string.
 */
#define IGNITE_BINARY_GET_FIELD_ID_AS_HASH \
int32_t GetFieldId(const char* name) \
{ \
    return GetBinaryStringHashCode(name); \
}

/**
 * @def IGNITE_BINARY_GET_HASH_CODE_ZERO(T)
 * Implementation of GetHashCode() function which always returns 0.
 */
#define IGNITE_BINARY_GET_HASH_CODE_ZERO(T) \
int32_t GetHashCode(const T& obj) \
{ \
    return 0; \
}

/**
 * @def IGNITE_BINARY_IS_NULL_FALSE(T)
 * Implementation of IsNull() function which always returns false.
 */
#define IGNITE_BINARY_IS_NULL_FALSE(T) \
bool IsNull(const T& obj) \
{ \
    return false; \
}

/**
 * @def IGNITE_BINARY_IS_NULL_IF_NULLPTR(T)
 * Implementation of IsNull() function which return true if passed object is null pointer.
 */
#define IGNITE_BINARY_IS_NULL_IF_NULLPTR(T) \
bool IsNull(const T& obj) \
{ \
    return obj; \
}

/**
 * @def IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(T)
 * Implementation of GetNull() function which returns an instance created with defult constructor.
 */
#define IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(T) \
T GetNull() \
{ \
    return T(); \
}

/**
 * @def IGNITE_BINARY_GET_NULL_NULLPTR(T)
 * Implementation of GetNull() function which returns NULL pointer.
 */
#define IGNITE_BINARY_GET_NULL_NULLPTR(T) \
T GetNull() \
{ \
    return NULL; \
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
        struct IGNITE_IMPORT_EXPORT BinaryType
        {
            /**
             * Get binary object type ID.
             *
             * @return Type ID.
             */
            int32_t GetTypeId()
            {
                IGNITE_ERROR_1(IgniteError::IGNITE_ERR_BINARY, "GetTypeId function is not defined for binary type.");
            }

            /**
             * Get binary object type name.
             *
             * @return Type name.
             */
            std::string GetTypeName() 
            {
                IGNITE_ERROR_1(IgniteError::IGNITE_ERR_BINARY, "GetTypeName function is not defined for binary type.");
            }

            /**
             * Get binary object field ID.
             *
             * @param name Field name.
             * @return Field ID.
             */
            int32_t GetFieldId(const char* name)
            {
                return GetBinaryStringHashCode(name);
            }

            /**
             * Write binary object.
             *
             * @param writer Writer.
             * @param obj Object.
             */
            void Write(BinaryWriter& writer, const T& obj)
            {
                IGNITE_ERROR_1(IgniteError::IGNITE_ERR_BINARY, "Write function is not defined for binary type.");
            }

            /**
             * Read binary object.
             *
             * @param reader Reader.
             * @return Object.
             */
            T Read(BinaryReader& reader)
            {
                IGNITE_ERROR_1(IgniteError::IGNITE_ERR_BINARY, "Read function is not defined for binary type.");
            }

            /**
             * Check whether passed binary object should be interpreted as NULL.
             *
             * @param obj Binary object to test.
             * @return True if binary object should be interpreted as NULL.
             */
            bool IsNull(const T& obj)
            {
                return false;
            }

            /**
             * Get NULL value for the given binary type.
             *
             * @return NULL value.
             */
            T GetNull()
            {
                IGNITE_ERROR_1(IgniteError::IGNITE_ERR_BINARY, "GetNull function is not defined for binary type.");
            }
        };

        /**
         * Templated binary type specification for pointers.
         */
        template <typename T>
        struct IGNITE_IMPORT_EXPORT BinaryType<T*>
        {
            /** Actual type. */
            BinaryType<T> typ;

            /**
             * Constructor.
             */
            BinaryType()
            {
                typ = BinaryType<T>();
            }

            int32_t GetTypeId()
            {
                return typ.GetTypeId();
            }

            std::string GetTypeName()
            {
                return typ.GetTypeName();
            }

            int32_t GetFieldId(const char* name)
            {
                return typ.GetFieldId(name);
            }

            void Write(BinaryWriter& writer, T* const& obj)
            {
                typ.Write(writer, *obj);
            }

            T* Read(BinaryReader& reader)
            {
                T* res = new T();

                *res = typ.Read(reader);

                return res;
            }

            bool IsNull(T* const& obj)
            {
                return !obj || typ.IsNull(*obj);
            }

            T* GetNull()
            {
                return NULL;
            }
        };
    }
}

#endif //_IGNITE_BINARY_BINARY_TYPE
