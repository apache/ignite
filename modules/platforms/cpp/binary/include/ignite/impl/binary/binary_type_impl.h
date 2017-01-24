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

#ifndef _IGNITE_IMPL_BINARY_BINARY_TYPE_IMP
#define _IGNITE_IMPL_BINARY_BINARY_TYPE_IMP

#include <stdint.h>

//#include "ignite/common/utils.h"

//#include "ignite/guid.h"
//#include "ignite/date.h"
//#include "ignite/timestamp.h"

#include <ignite/binary/binary_type.h>
#include <ignite/binary/binary_object.h>
#include <ignite/binary/binary_array_identity_resolver.h>

#include <ignite/reference.h>

/**
 * Some SFINAE magic to check existence of the specified method with the
 * specified signature in the BinaryType<T> class.
 *
 * This macro declares checker for the method.
 */
#define IGNITE_DECLARE_BINARY_TYPE_METHOD_CHECKER(method, sign)                         \
    template<typename T>                                                                \
    class IsDeclaredBinaryType##method                                                  \
    {                                                                                   \
        typedef char one;                                                               \
        typedef char two[2];                                                            \
                                                                                        \
        template<class U, U> struct test;                                               \
                                                                                        \
        template<typename C> static one& helper(test<sign, &C::##method>*);             \
        template<typename C> static two& helper(...);                                   \
                                                                                        \
    public:                                                                             \
        const static bool value =                                                       \
            (sizeof(helper< ignite::binary::BinaryType<T> >(0)) == sizeof(one));        \
    }

namespace ignite
{
    namespace impl
    {
        namespace binary
        {
            IGNITE_DECLARE_BINARY_TYPE_METHOD_CHECKER(GetHashCode, void(*)(const T&));
            IGNITE_DECLARE_BINARY_TYPE_METHOD_CHECKER(GetIdentityResolver, ignite::Reference<ignite::binary::BinaryIdentityResolver>(*)());

            template<typename T>
            struct HashCodeGetterDefault
            {
                static int32_t get(const T&, const ignite::binary::BinaryObject& obj)
                {
                    ignite::binary::BinaryArrayIdentityResolver arrayResolver;

                    return arrayResolver.GetHashCode(obj);
                }
            };

            template<typename T>
            struct HashCodeGetterHashCode
            {
                static int32_t get(const T& obj, const ignite::binary::BinaryObject&)
                {
                    ignite::binary::BinaryType<T> bt;

                    return bt.GetHashCode();
                }
            };

            template<typename T>
            struct HashCodeGetterResolver
            {
                static int32_t get(const T&, const ignite::binary::BinaryObject& obj)
                {
                    ignite::binary::BinaryType<T> bt;
                    ignite::Reference<ignite::binary::BinaryIdentityResolver> resolver = bt.GetIdentityResolver();

                    return resolver.Get().GetHashCode(obj);
                }
            };

            template<bool, typename T1, typename T2>
            struct Conditional
            {
                typedef T1 type;
            };

            template<typename T1, typename T2>
            struct Conditional<false, T1, T2>
            {
                typedef T2 type;
            };

            template<typename T>
            int32_t GetHashCode(const T& obj, const ignite::binary::BinaryObject& binObj)
            {
                typedef typename Conditional<
                    IsDeclaredBinaryTypeGetIdentityResolver<T>::value,
                    HashCodeGetterResolver<T>,
                    typename Conditional<
                        IsDeclaredBinaryTypeGetHashCode<T>::value,
                        HashCodeGetterHashCode<T>,
                        HashCodeGetterDefault<T> 
                    >::type
                >::type HashCodeGetter;

                return HashCodeGetter::get(obj, binObj);
            }
        }
    }
}

#endif //_IGNITE_IMPL_BINARY_BINARY_TYPE_IMP
