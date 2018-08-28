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

#ifndef _IGNITE_IMPL_BINARY_BINARY_TYPE_IMPL
#define _IGNITE_IMPL_BINARY_BINARY_TYPE_IMPL

#include <memory>
#include <stdint.h>

#include <ignite/ignite_error.h>

namespace ignite
{
    namespace binary
    {
        class BinaryReader;
        class BinaryWriter;

        template<typename T>
        struct BinaryType;

        template<>
        struct BinaryType<IgniteError>
        {
            static int32_t GetTypeId();

            static void GetTypeName(std::string& dst)
            {
                dst = "IgniteError";
            }

            static int32_t GetFieldId(const char* name);

            static bool IsNull(const IgniteError& obj)
            {
                return false;
            }

            static void GetNull(IgniteError& dst);

            static void Write(BinaryWriter& writer, const IgniteError& obj);

            static void Read(BinaryReader& reader, IgniteError& dst);
        };

        /**
         * Write helper. Takes care of proper writing of pointers.
         */
        template<typename T>
        struct WriteHelper
        {
            template<typename W>
            static void Write(W& writer, const T& val)
            {
                writer.template WriteTopObject0<BinaryWriter>(val);
            }
        };

        /**
         * Specialization for the pointer case.
         */
        template<typename T>
        struct WriteHelper<T*>
        {
            template<typename W>
            static void Write(W& writer, const T* val)
            {
                if (!val)
                    writer.WriteNull0();
                else
                    writer.template WriteTopObject0<BinaryWriter>(*val);
            }
        };

        /**
         * Read helper. Takes care of proper reading of pointers.
         */
        template<typename T>
        struct ReadHelper
        {
            template<typename R>
            static T Read(R& reader)
            {
                T res;

                reader.template ReadTopObject0<ignite::binary::BinaryReader, T>(res);

                return res;
            }
        };

        /**
         * Specialization for the pointer case.
         */
        template<typename T>
        struct ReadHelper<T*>
        {
            template<typename R>
            static T* Read(R& reader)
            {
                if (reader.SkipIfNull())
                    return 0;

                std::auto_ptr<T> res(new T());

                reader.template ReadTopObject0<ignite::binary::BinaryReader, T>(*res);

                return res.release();
            }
        };
    }
}

#endif //_IGNITE_IMPL_BINARY_BINARY_TYPE_IMPL
