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
 * Declares ignite::binary::BinaryWriter class.
 */

#ifndef _IGNITE_BINARY_BINARY_WRITER
#define _IGNITE_BINARY_BINARY_WRITER

#include <string>
#include <stdint.h>

#include <ignite/common/common.h>

#include "ignite/binary/binary_raw_writer.h"

namespace ignite
{
    namespace binary 
    {
        /**
         * Binary writer.
         *
         * This class is implemented as a reference to an implementation so copying
         * of this class instance will only create another reference to the same
         * underlying object.
         *
         * @note User should not store copy of this instance as it can be
         *     invalidated as soon as the initially passed to user instance has
         *     been destructed. For example this means that if user received an
         *     instance of this class as a function argument then he should not
         *     store and use copy of this class out of the scope of this
         *     function.
         */
        class IGNITE_IMPORT_EXPORT BinaryWriter
        {
        public:
            /**
             * Constructor.
             *
             * Internal method. Should not be used by user.
             *
             * @param impl Implementation.
             */
            BinaryWriter(impl::binary::BinaryWriterImpl* impl);

            /**
             * Write 8-byte signed integer. Maps to "byte" type in Java.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            void WriteInt8(const char* fieldName, int8_t val);

            /**
             * Write array of 8-byte signed integers. Maps to "byte[]" type in Java.
             *
             * @param fieldName Field name.
             * @param val Array.
             * @param len Array length.
             */
            void WriteInt8Array(const char* fieldName, const int8_t* val, int32_t len);

            /**
             * Write bool. Maps to "short" type in Java.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            void WriteBool(const char* fieldName, bool val);

            /**
             * Write array of bools. Maps to "bool[]" type in Java.
             *
             * @param fieldName Field name.
             * @param val Array.
             * @param len Array length.
             */
            void WriteBoolArray(const char* fieldName, const bool* val, int32_t len);

            /**
             * Write 16-byte signed integer. Maps to "short" type in Java.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            void WriteInt16(const char* fieldName, int16_t val);

            /**
             * Write array of 16-byte signed integers. Maps to "short[]" type in Java.
             *
             * @param fieldName Field name.
             * @param val Array.
             * @param len Array length.
             */
            void WriteInt16Array(const char* fieldName, const int16_t* val, int32_t len);

            /**
             * Write 16-byte unsigned integer. Maps to "char" type in Java.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            void WriteUInt16(const char* fieldName, uint16_t val);

            /**
             * Write array of 16-byte unsigned integers. Maps to "char[]" type in Java.
             *
             * @param fieldName Field name.
             * @param val Array.
             * @param len Array length.
             */
            void WriteUInt16Array(const char* fieldName, const uint16_t* val, int32_t len);

            /**
             * Write 32-byte signed integer. Maps to "int" type in Java.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            void WriteInt32(const char* fieldName, int32_t val);

            /**
             * Write array of 32-byte signed integers. Maps to "int[]" type in Java.
             *
             * @param fieldName Field name.
             * @param val Array.
             * @param len Array length.
             */
            void WriteInt32Array(const char* fieldName, const int32_t* val, int32_t len);

            /**
             * Write 64-byte signed integer. Maps to "long" type in Java.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            void WriteInt64(const char* fieldName, int64_t val);

            /**
             * Write array of 64-byte signed integers. Maps to "long[]" type in Java.
             *
             * @param fieldName Field name.
             * @param val Array.
             * @param len Array length.
             */
            void WriteInt64Array(const char* fieldName, const int64_t* val, int32_t len);

            /**
             * Write float. Maps to "float" type in Java.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            void WriteFloat(const char* fieldName, float val);

            /**
             * Write array of floats. Maps to "float[]" type in Java.
             *
             * @param fieldName Field name.
             * @param val Array.
             * @param len Array length.
             */
            void WriteFloatArray(const char* fieldName, const float* val, int32_t len);

            /**
             * Write double. Maps to "double" type in Java.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            void WriteDouble(const char* fieldName, double val);

            /**
             * Write array of doubles. Maps to "double[]" type in Java.
             *
             * @param fieldName Field name.
             * @param val Array.
             * @param len Array length.
             */
            void WriteDoubleArray(const char* fieldName, const double* val, int32_t len);

            /**
             * Write Guid. Maps to "UUID" type in Java.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            void WriteGuid(const char* fieldName, const Guid& val);

            /**
             * Write array of Guids. Maps to "UUID[]" type in Java.
             *
             * @param fieldName Field name.
             * @param val Array.
             * @param len Array length.
             */
            void WriteGuidArray(const char* fieldName, const Guid* val, int32_t len);

            /**
             * Write Date. Maps to "Date" type in Java.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            void WriteDate(const char* fieldName, const Date& val);

            /**
             * Write array of Dates. Maps to "Date[]" type in Java.
             *
             * @param fieldName Field name.
             * @param val Array.
             * @param len Array length.
             */
            void WriteDateArray(const char* fieldName, const Date* val, int32_t len);

            /**
             * Write Timestamp. Maps to "Timestamp" type in Java.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            void WriteTimestamp(const char* fieldName, const Timestamp& val);

            /**
             * Write array of Timestamps. Maps to "Timestamp[]" type in Java.
             *
             * @param fieldName Field name.
             * @param val Array.
             * @param len Array length.
             */
            void WriteTimestampArray(const char* fieldName, const Timestamp* val, int32_t len);

            /**
             * Write Time. Maps to "Time" type in Java.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            void WriteTime(const char* fieldName, const Time& val);

            /**
             * Write array of Times. Maps to "Time[]" type in Java.
             *
             * @param fieldName Field name.
             * @param val Array.
             * @param len Array length.
             */
            void WriteTimeArray(const char* fieldName, const Time* val, int32_t len);

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
            void WriteString(const char* fieldName, const char* val, int32_t len);

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
             * Every time you get a BinaryStringArrayWriter from BinaryWriter
             * you start writing session. Only one single writing session can be
             * open at a time. So it is not allowed to start new writing session
             * without calling BinaryStringArrayWriter::Close() method prior on
             * obtained BinaryStringArrayWriter class instance.
             *
             * @param fieldName Field name.
             * @return String array writer.
             */
            BinaryStringArrayWriter WriteStringArray(const char* fieldName);

            /**
             * Write binary enum entry.
             *
             * @param fieldName Field name.
             * @param entry Binary enum entry.
             */
            void WriteBinaryEnum(const char* fieldName, BinaryEnumEntry entry);

            /**
             * Write NULL value.
             *
             * @param fieldName Field name.
             */
            void WriteNull(const char* fieldName);

            /**
             * Start array write.
             *
             * Every time you get a BinaryArrayWriter from BinaryWriter you
             * start writing session. Only one single writing session can be
             * open at a time. So it is not allowed to start new writing session
             * without calling BinaryArrayWriter::Close() method prior on
             * obtained BinaryArrayWriter class instance.
             *
             * @param fieldName Field name.
             * @return Array writer.
             */
            template<typename T>
            BinaryArrayWriter<T> WriteArray(const char* fieldName)
            {
                int32_t id = impl->WriteArray(fieldName);

                return BinaryArrayWriter<T>(impl, id);
            }

            /**
             * Start collection write.
             *
             * Every time you get a BinaryCollectionWriter from BinaryWriter you
             * start writing session. Only one single writing session can be
             * open at a time. So it is not allowed to start new writing session
             * without calling BinaryCollectionWriter::Close() method prior on
             * obtained BinaryCollectionWriter class instance.
             *
             * @param fieldName Field name.
             * @return Collection writer.
             */
            template<typename T>
            BinaryCollectionWriter<T> WriteCollection(const char* fieldName)
            {
                return WriteCollection<T>(fieldName, CollectionType::UNDEFINED);
            }

            /**
             * Start collection write.
             *
             * Every time you get a BinaryCollectionWriter from BinaryWriter you
             * start writing session. Only one single writing session can be
             * open at a time. So it is not allowed to start new writing session
             * without calling BinaryCollectionWriter::Close() method prior on
             * obtained BinaryCollectionWriter class instance.
             *
             * @param fieldName Field name.
             * @param typ Collection type.
             * @return Collection writer.
             */
            template<typename T>
            BinaryCollectionWriter<T> WriteCollection(const char* fieldName, CollectionType::Type typ)
            {
                int32_t id = impl->WriteCollection(fieldName, typ);

                return BinaryCollectionWriter<T>(impl, id);
            }

            /**
             * Write values in interval [first, last).
             *
             * @param fieldName Field name.
             * @param first Iterator pointing to the beginning of the interval.
             * @param last Iterator pointing to the end of the interval.
             */
            template<typename InputIterator>
            void WriteCollection(const char* fieldName, InputIterator first, InputIterator last)
            {
                WriteCollection(fieldName, first, last, CollectionType::UNDEFINED);
            }

            /**
             * Write values in interval [first, last).
             *
             * @param fieldName Field name.
             * @param first Iterator pointing to the beginning of the interval.
             * @param last Iterator pointing to the end of the interval.
             * @param typ Collection type.
             */
            template<typename InputIterator>
            void WriteCollection(const char* fieldName, InputIterator first, InputIterator last, CollectionType::Type typ)
            {
                impl->WriteCollection(fieldName, first, last, typ);
            }

            /**
             * Start map write.
             *
             * Every time you get a BinaryMapWriter from BinaryWriter you start
             * writing session. Only one single writing session can be open at
             * a time. So it is not allowed to start new writing session without
             * calling BinaryMapWriter::Close() method prior on obtained
             * BinaryMapWriter class instance.
             *
             * @param fieldName Field name.
             * @return Map writer.
             */
            template<typename K, typename V>
            BinaryMapWriter<K, V> WriteMap(const char* fieldName)
            {
                return WriteMap<K, V>(fieldName, MapType::UNDEFINED);
            }

            /**
             * Start map write.
             *
             * Every time you get a BinaryMapWriter from BinaryWriter you start
             * writing session. Only one single writing session can be open at
             * a time. So it is not allowed to start new writing session without
             * calling BinaryMapWriter::Close() method prior on obtained
             * BinaryMapWriter class instance.
             *
             * @param fieldName Field name.
             * @param typ Map type.
             * @return Map writer.
             */
            template<typename K, typename V>
            BinaryMapWriter<K, V> WriteMap(const char* fieldName, MapType::Type typ)
            {
                int32_t id = impl->WriteMap(fieldName, typ);

                return BinaryMapWriter<K, V>(impl, id);
            }

            /**
             * Write object.
             *
             * @param fieldName Field name.
             * @param val Value.
             */
            template<typename T>
            void WriteObject(const char* fieldName, const T& val)
            {
                impl->WriteObject<T>(fieldName, val);
            }

            /**
             * Write enum entry.
             *
             * @param fieldName Field name.
             * @param val Binary enum entry.
             *
             * @trapam T Enum type. BinaryEnum class template should be specialized for the type.
             */
            template<typename T>
            void WriteEnum(const char* fieldName, T val)
            {
                impl->WriteEnum(fieldName, val);
            }

            /**
             * Get raw writer for this reader.
             *
             * @return Raw writer.
             */
            BinaryRawWriter RawWriter();
        private:
            /** Implementation delegate. */
            ignite::impl::binary::BinaryWriterImpl* impl;
        };
    }
}

#endif //_IGNITE_BINARY_BINARY_WRITER
