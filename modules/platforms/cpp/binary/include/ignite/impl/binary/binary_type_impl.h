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

                Read<R>(reader, res);

                return res;
            }

            template<typename R>
            static void Read(R& reader, T& val)
            {
                reader.template ReadTopObject0<BinaryReader, T>(val);
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

                reader.template ReadTopObject0<BinaryReader, T>(*res);

                return res.release();
            }

            template<typename R>
            static void Read(R& reader, T*& ptr)
            {
                ptr = Read<R>(reader);
            }
        };
    }
}

#endif //_IGNITE_IMPL_BINARY_BINARY_TYPE_IMPL
