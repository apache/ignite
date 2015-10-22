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

#ifndef _IGNITE_PORTABLE_TYPE
#define _IGNITE_PORTABLE_TYPE

#include <stdint.h>

#include <ignite/common/common.h>

#include "ignite/ignite_error.h"

/**
 * Start portable type definition.
 */
#define IGNITE_PORTABLE_TYPE_START(T) \
template<> \
struct PortableType<T> \
{

/**
 * End portable type definition.
 */
#define IGNITE_PORTABLE_TYPE_END \
};

/**
 * Implementation of GetTypeId() which returns predefined constant.
 */
#define IGNITE_PORTABLE_GET_TYPE_ID_AS_CONST(id) \
int32_t GetTypeId() \
{ \
    return id; \
}

/**
 * Implementation of GetTypeId() which returns hash of passed type name.
 */
#define IGNITE_PORTABLE_GET_TYPE_ID_AS_HASH(typeName) \
int32_t GetTypeId() \
{ \
    return GetPortableStringHashCode(#typeName); \
}

/**
 * Implementation of GetTypeName() which returns type name as is.
 */
#define IGNITE_PORTABLE_GET_TYPE_NAME_AS_IS(typeName) \
std::string GetTypeName() \
{ \
    return #typeName; \
}

/**
 * Default implementation of GetFieldId() function which returns Java-way hash code of the string.
 */
#define IGNITE_PORTABLE_GET_FIELD_ID_AS_HASH \
int32_t GetFieldId(const char* name) \
{ \
    return GetPortableStringHashCode(name); \
}

/**
 * Implementation of GetHashCode() function which always returns 0.
 */
#define IGNITE_PORTABLE_GET_HASH_CODE_ZERO(T) \
int32_t GetHashCode(const T& obj) \
{ \
    return 0; \
}

/**
 * Implementation of IsNull() function which always returns false.
 */
#define IGNITE_PORTABLE_IS_NULL_FALSE(T) \
bool IsNull(const T& obj) \
{ \
    return false; \
}

/**
 * Implementation of IsNull() function which return true if passed object is null pointer.
 */
#define IGNITE_PORTABLE_IS_NULL_IF_NULLPTR(T) \
bool IsNull(const T& obj) \
{ \
    return obj; \
}

/**
 * Implementation of GetNull() function which returns an instance created with defult constructor.
 */
#define IGNITE_PORTABLE_GET_NULL_DEFAULT_CTOR(T) \
T GetNull() \
{ \
    return T(); \
}

/**
 * Implementation of GetNull() function which returns NULL pointer.
 */
#define IGNITE_PORTABLE_GET_NULL_NULLPTR(T) \
T GetNull() \
{ \
    return NULL; \
}

namespace ignite
{
    namespace portable
    {
        class PortableWriter;
        class PortableReader;

        /**
         * Get portable string hash code.
         *
         * @param val Value.
         * @return Hash code.
         */
        IGNITE_IMPORT_EXPORT int32_t GetPortableStringHashCode(const char* val);

        /**
         * Portable type structure. Defines a set of functions required for type to be serialized and deserialized.
         */
        template<typename T>
        struct IGNITE_IMPORT_EXPORT PortableType
        {
            /**
             * Get portable object type ID.
             *
             * @return Type ID.
             */
            int32_t GetTypeId()
            {
                IGNITE_ERROR_1(IgniteError::IGNITE_ERR_PORTABLE, "GetTypeId function is not defined for portable type.");
            }

            /**
             * Get portable object type name.
             *
             * @return Type name.
             */
            std::string GetTypeName() 
            {
                IGNITE_ERROR_1(IgniteError::IGNITE_ERR_PORTABLE, "GetTypeName function is not defined for portable type.");
            }

            /**
             * Get portable object field ID.
             *
             * @param name Field name.
             * @return Field ID.
             */
            int32_t GetFieldId(const char* name)
            {
                return GetPortableStringHashCode(name);
            }

            /**
             * Get portable object hash code.
             *
             * @param obj Portable object.
             * @return Hash code.
             */
            int32_t GetHashCode(const T& obj)
            {
                return 0;
            }

            /**
             * Write portable object.
             *
             * @param writer Writer.
             * @param obj Object.
             */
            void Write(PortableWriter& writer, const T& obj)
            {
                IGNITE_ERROR_1(IgniteError::IGNITE_ERR_PORTABLE, "Write function is not defined for portable type.");
            }

            /**
             * Read portable object.
             *
             * @param reader Reader.
             * @return Object.
             */
            T Read(PortableReader& reader)
            {
                IGNITE_ERROR_1(IgniteError::IGNITE_ERR_PORTABLE, "Read function is not defined for portable type.");
            }

            /**
             * Check whether passed portable object should be interpreted as NULL.
             *
             * @param obj Portable object to test.
             * @return True if portable object should be interpreted as NULL.
             */
            bool IsNull(const T& obj)
            {
                return false;
            }

            /**
             * Get NULL value for the given portable type.
             *
             * @return NULL value.
             */
            T GetNull()
            {
                IGNITE_ERROR_1(IgniteError::IGNITE_ERR_PORTABLE, "GetNull function is not defined for portable type.");
            }
        };

        /*
         * Templated portable type for pointers.
         */
        template <typename T>
        struct IGNITE_IMPORT_EXPORT PortableType<T*>
        {
            /** Actual type. */
            PortableType<T> typ;

            /**
             * Constructor.
             */
            PortableType()
            {
                typ = PortableType<T>();
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

            int32_t GetHashCode(T* const& obj)
            {
                return typ.GetHashCode(*obj);
            }

            void Write(PortableWriter& writer, T* const& obj)
            {
                typ.Write(writer, *obj);
            }

            T* Read(PortableReader& reader)
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

#endif
