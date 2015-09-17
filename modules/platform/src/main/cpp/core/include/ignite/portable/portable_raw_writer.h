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

#ifndef _IGNITE_PORTABLE_RAW_WRITER
#define _IGNITE_PORTABLE_RAW_WRITER

#include <stdint.h>

#include <ignite/common/common.h>

#include "ignite/impl/portable/portable_writer_impl.h"
#include "ignite/portable/portable_consts.h"
#include "ignite/portable/portable_containers.h"
#include "ignite/guid.h"

namespace ignite
{
    namespace portable
    {
        /**
         * Portable raw writer.
         */
        class IGNITE_IMPORT_EXPORT PortableRawWriter
        {
        public:
            /**
             * Constructor.
             *
             * @param impl Implementation.
             */
            PortableRawWriter(ignite::impl::portable::PortableWriterImpl* impl);

            /**
             * Write 8-byte signed integer. Maps to "byte" type in Java.
             *
             * @param val Value.
             */
            void WriteInt8(const int8_t val);

            /**
             * Write array of 8-byte signed integers. Maps to "byte[]" type in Java.
             *
             * @param val Array.
             * @param len Array length.
             */
            void WriteInt8Array(const int8_t* val, const int32_t len);

            /**
             * Write bool. Maps to "short" type in Java.
             *
             * @param val Value.
             */
            void WriteBool(const bool val);

            /**
             * Write array of bools. Maps to "bool[]" type in Java.
             *
             * @param val Array.
             * @param len Array length.
             */
            void WriteBoolArray(const bool* val, const int32_t len);

            /**
             * Write 16-byte signed integer. Maps to "short" type in Java.
             *
             * @param val Value.
             */
            void WriteInt16(const int16_t val);

            /**
             * Write array of 16-byte signed integers. Maps to "short[]" type in Java.
             *
             * @param val Array.
             * @param len Array length.
             */
            void WriteInt16Array(const int16_t* val, const int32_t len);

            /**
             * Write 16-byte unsigned integer. Maps to "char" type in Java.
             *
             * @param val Value.
             */
            void WriteUInt16(const uint16_t val);

            /**
             * Write array of 16-byte unsigned integers. Maps to "char[]" type in Java.
             *
             * @param val Array.
             * @param len Array length.
             */
            void WriteUInt16Array(const uint16_t* val, const int32_t len);

            /**
             * Write 32-byte signed integer. Maps to "int" type in Java.
             *
             * @param val Value.
             */
            void WriteInt32(const int32_t val);

            /**
             * Write array of 32-byte signed integers. Maps to "int[]" type in Java.
             *
             * @param val Array.
             * @param len Array length.
             */
            void WriteInt32Array(const int32_t* val, const int32_t len);

            /**
             * Write 64-byte signed integer. Maps to "long" type in Java.
             *
             * @param val Value.
             */
            void WriteInt64(const int64_t val);

            /**
             * Write array of 64-byte signed integers. Maps to "long[]" type in Java.
             *
             * @param val Array.
             * @param len Array length.
             */
            void WriteInt64Array(const int64_t* val, const int32_t len);

            /**
             * Write float. Maps to "float" type in Java.
             *
             * @param val Value.
             */
            void WriteFloat(const float val);

            /**
             * Write array of floats. Maps to "float[]" type in Java.
             *
             * @param val Array.
             * @param len Array length.
             */
            void WriteFloatArray(const float* val, const int32_t len);

            /**
             * Write double. Maps to "double" type in Java.
             *
             * @param val Value.
             */
            void WriteDouble(const double val);

            /**
             * Write array of doubles. Maps to "double[]" type in Java.
             *
             * @param val Array.
             * @param len Array length.
             */
            void WriteDoubleArray(const double* val, const int32_t len);

            /**
             * Write Guid. Maps to "UUID" type in Java.
             *
             * @param val Value.
             */
            void WriteGuid(const Guid val);

            /**
             * Write array of Guids. Maps to "UUID[]" type in Java.
             *
             * @param val Array.
             * @param len Array length.
             */
            void WriteGuidArray(const Guid* val, const int32_t len);

            /**
             * Write string.
             *
             * @param val Null-terminated character array.
             */
            void WriteString(const char* val);

            /**
             * Write string.
             *
             * @param val String.
             * @param len String length (characters).
             */
            void WriteString(const char* val, const int32_t len);
            
            /**
             * Write string.
             *
             * @param val String.
             */
            void WriteString(const std::string& val)
            {
                WriteString(val.c_str());
            }
            
            /**
             * Start string array write.
             *
             * @return String array writer.
             */
            PortableStringArrayWriter WriteStringArray();

            /**
             * Write NULL value.
             */
            void WriteNull();

            /**
             * Start array write.
             *
             * @return Array writer.
             */
            template<typename T>
            PortableArrayWriter<T> WriteArray()
            {
                int32_t id = impl->WriteArray();

                return PortableArrayWriter<T>(impl, id);
            }

            /**
             * Start collection write.
             *
             * @return Collection writer.
             */
            template<typename T>
            PortableCollectionWriter<T> WriteCollection()
            {
                return WriteCollection<T>(IGNITE_COLLECTION_UNDEFINED);
            }

            /**
             * Start collection write.
             *
             * @param type Collection type.
             * @return Collection writer.
             */
            template<typename T>
            PortableCollectionWriter<T> WriteCollection(ignite::portable::CollectionType typ)
            {
                int32_t id = impl->WriteCollection(typ);

                return PortableCollectionWriter<T>(impl, id);
            }

            /**
             * Start map write.
             *
             * @param typ Map type.
             * @return Map writer.
             */
            template<typename K, typename V>
            PortableMapWriter<K, V> WriteMap()
            {
                return WriteMap<K, V>(IGNITE_MAP_UNDEFINED);
            }

            /**
             * Start map write.
             *
             * @param typ Map type.
             * @return Map writer.
             */
            template<typename K, typename V>
            PortableMapWriter<K, V> WriteMap(ignite::portable::MapType typ)
            {
                int32_t id = impl->WriteMap(typ);

                return PortableMapWriter<K, V>(impl, id);
            }

            /**
             * Write object.
             *
             * @param val Object.
             */
            template<typename T>
            void WriteObject(T val)
            {
                impl->WriteObject<T>(val);
            }
        private:
            /** Implementation delegate. */
            ignite::impl::portable::PortableWriterImpl* impl; 
        };
    }
}

#endif