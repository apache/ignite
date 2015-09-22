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

#ifndef _IGNITE_PORTABLE_WRITER
#define _IGNITE_PORTABLE_WRITER

#include <string>
#include <stdint.h>

#include <ignite/common/common.h>

#include "ignite/portable/portable_raw_writer.h"

namespace ignite
{
    namespace portable 
    {
        /**
         * Portable writer.
         */
        class IGNITE_IMPORT_EXPORT PortableWriter
        {
        public:
            /**
             * Constructor.
             *
             * @param impl Implementation.
             */
            PortableWriter(ignite::impl::portable::PortableWriterImpl* impl);

            /**
             * Write 8-byte signed integer. Maps to "byte" type in Java.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            void WriteInt8(const char* fieldName, const int8_t val);

            /**
             * Write array of 8-byte signed integers. Maps to "byte[]" type in Java.
             *
             * @param fieldName Field name.
             * @param val Array.
             * @param len Array length.
             */
            void WriteInt8Array(const char* fieldName, const int8_t* val, const int32_t len);

            /**
             * Write bool. Maps to "short" type in Java.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            void WriteBool(const char* fieldName, const bool val);

            /**
             * Write array of bools. Maps to "bool[]" type in Java.
             *
             * @param fieldName Field name.
             * @param val Array.
             * @param len Array length.
             */
            void WriteBoolArray(const char* fieldName, const bool* val, const int32_t len);

            /**
             * Write 16-byte signed integer. Maps to "short" type in Java.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            void WriteInt16(const char* fieldName, const int16_t val);

            /**
             * Write array of 16-byte signed integers. Maps to "short[]" type in Java.
             *
             * @param fieldName Field name.
             * @param val Array.
             * @param len Array length.
             */
            void WriteInt16Array(const char* fieldName, const int16_t* val, const int32_t len);

            /**
             * Write 16-byte unsigned integer. Maps to "char" type in Java.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            void WriteUInt16(const char* fieldName, const uint16_t val);

            /**
             * Write array of 16-byte unsigned integers. Maps to "char[]" type in Java.
             *
             * @param fieldName Field name.
             * @param val Array.
             * @param len Array length.
             */
            void WriteUInt16Array(const char* fieldName, const uint16_t* val, const int32_t len);

            /**
             * Write 32-byte signed integer. Maps to "int" type in Java.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            void WriteInt32(const char* fieldName, const int32_t val);

            /**
             * Write array of 32-byte signed integers. Maps to "int[]" type in Java.
             *
             * @param fieldName Field name.
             * @param val Array.
             * @param len Array length.
             */
            void WriteInt32Array(const char* fieldName, const int32_t* val, const int32_t len);

            /**
             * Write 64-byte signed integer. Maps to "long" type in Java.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            void WriteInt64(const char* fieldName, const int64_t val);

            /**
             * Write array of 64-byte signed integers. Maps to "long[]" type in Java.
             *
             * @param fieldName Field name.
             * @param val Array.
             * @param len Array length.
             */
            void WriteInt64Array(const char* fieldName, const int64_t* val, const int32_t len);

            /**
             * Write float. Maps to "float" type in Java.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            void WriteFloat(const char* fieldName, const float val);

            /**
             * Write array of floats. Maps to "float[]" type in Java.
             *
             * @param fieldName Field name.
             * @param val Array.
             * @param len Array length.
             */
            void WriteFloatArray(const char* fieldName, const float* val, const int32_t len);

            /**
             * Write double. Maps to "double" type in Java.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            void WriteDouble(const char* fieldName, const double val);

            /**
             * Write array of doubles. Maps to "double[]" type in Java.
             *
             * @param fieldName Field name.
             * @param val Array.
             * @param len Array length.
             */
            void WriteDoubleArray(const char* fieldName, const double* val, const int32_t len);

            /**
             * Write Guid. Maps to "UUID" type in Java.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            void WriteGuid(const char* fieldName, const Guid val);

            /**
             * Write array of Guids. Maps to "UUID[]" type in Java.
             *
             * @param fieldName Field name.
             * @param val Array.
             * @param len Array length.
             */
            void WriteGuidArray(const char* fieldName, const Guid* val, const int32_t len);

            /**
             * Write string.
             *
             * @param fieldName Field name.
             * @param val Null-terminated character sequence.
             */
            void WriteString(const char* fieldName, const char* val);

            /**
             * Write string.
             *
             * @param fieldName Field name.
             * @param val String.
             * @param len String length (characters).
             */
            void WriteString(const char* fieldName, const char* val, const int32_t len);

            /**
             * Write string.
             *
             * @param fieldName Field name.
             * @param val String.
             */
            void WriteString(const char* fieldName, const std::string& val)
            {
                WriteString(fieldName, val.c_str());
            }

            /**
             * Start string array write.
             *
             * @param fieldName Field name.
             * @return String array writer.
             */
            PortableStringArrayWriter WriteStringArray(const char* fieldName);

            /**
             * Write NULL value.
             *
             * @param fieldName Field name.
             */
            void WriteNull(const char* fieldName);

            /**
             * Start array write.
             *
             * @param fieldName Field name.
             * @return Array writer.
             */
            template<typename T>
            PortableArrayWriter<T> WriteArray(const char* fieldName)
            {
                int32_t id = impl->WriteArray(fieldName);

                return PortableArrayWriter<T>(impl, id);
            }

            /**
             * Start collection write.
             *
             * @param fieldName Field name.
             * @return Collection writer.
             */
            template<typename T>
            PortableCollectionWriter<T> WriteCollection(const char* fieldName)
            {
                return WriteCollection<T>(fieldName, IGNITE_COLLECTION_UNDEFINED);
            }

            /**
             * Start collection write.
             *
             * @param fieldName Field name.
             * @param type Collection type.
             * @return Collection writer.
             */
            template<typename T>
            PortableCollectionWriter<T> WriteCollection(const char* fieldName, ignite::portable::CollectionType typ)
            {
                int32_t id = impl->WriteCollection(fieldName, typ);

                return PortableCollectionWriter<T>(impl, id);
            }

            /**
             * Start map write.
             *
             * @param fieldName Field name.
             * @param typ Map type.
             * @return Map writer.
             */
            template<typename K, typename V>
            PortableMapWriter<K, V> WriteMap(const char* fieldName)
            {
                return WriteMap<K, V>(fieldName, IGNITE_MAP_UNDEFINED);
            }

            /**
             * Start map write.
             *
             * @param fieldName Field name.
             * @param typ Map type.
             * @return Map writer.
             */
            template<typename K, typename V>
            PortableMapWriter<K, V> WriteMap(const char* fieldName, ignite::portable::MapType typ)
            {
                int32_t id = impl->WriteMap(fieldName, typ);

                return PortableMapWriter<K, V>(impl, id);
            }

            /**
             * Write object.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            template<typename T>
            void WriteObject(const char* fieldName, T val)
            {
                impl->WriteObject<T>(fieldName, val);
            }

            /**
             * Get raw writer for this reader.
             *
             * @return Raw writer.
             */
            PortableRawWriter RawWriter();
        private:
            /** Implementation delegate. */
            ignite::impl::portable::PortableWriterImpl* impl;
        };
    }
}

#endif