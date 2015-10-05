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

#ifndef _IGNITE_PORTABLE_READER
#define _IGNITE_PORTABLE_READER

#include <stdint.h>
#include <string>

#include <ignite/common/common.h>

#include "ignite/portable/portable_raw_reader.h"
#include "ignite/guid.h"

namespace ignite
{    
    namespace portable
    {
        /**
         * Portable reader.
         */
        class IGNITE_IMPORT_EXPORT PortableReader
        {
        public:
            /**
             * Constructor.
             *
             * @param impl Implementation.
             */
            PortableReader(ignite::impl::portable::PortableReaderImpl* impl);

            /**
             * Read 8-byte signed integer. Maps to "byte" type in Java.
             *
             * @param fieldName Field name.
             * @param fieldName Field name.
             * @return Result.
             */
            int8_t ReadInt8(const char* fieldName);

            /**
             * Read array of 8-byte signed integers. Maps to "byte[]" type in Java.
             *
             * @param fieldName Field name.
             * @param res Array to store data to.
             * @param len Expected length of array.
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t ReadInt8Array(const char* fieldName, int8_t* res, const int32_t len);

            /**
             * Read bool. Maps to "short" type in Java.
             *
             * @param fieldName Field name.
             * @return Result.
             */
            bool ReadBool(const char* fieldName);

            /**
             * Read array of bools. Maps to "bool[]" type in Java.
             *
             * @param fieldName Field name.
             * @param res Array to store data to.
             * @param len Expected length of array.             
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t ReadBoolArray(const char* fieldName, bool* res, const int32_t len);

            /**
             * Read 16-byte signed integer. Maps to "short" type in Java.
             *
             * @param fieldName Field name.
             * @return Result.
             */
            int16_t ReadInt16(const char* fieldName);

            /**
             * Read array of 16-byte signed integers. Maps to "short[]" type in Java.
             *
             * @param fieldName Field name.
             * @param res Array to store data to.
             * @param len Expected length of array.
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t ReadInt16Array(const char* fieldName, int16_t* res, const int32_t len);

            /**
             * Read 16-byte unsigned integer. Maps to "char" type in Java.
             *
             * @param fieldName Field name.
             * @return Result.
             */
            uint16_t ReadUInt16(const char* fieldName);

            /**
             * Read array of 16-byte unsigned integers. Maps to "char[]" type in Java.
             *
             * @param fieldName Field name.
             * @param res Array to store data to.
             * @param len Expected length of array.             
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t ReadUInt16Array(const char* fieldName, uint16_t* res, const int32_t len);

            /**
             * Read 32-byte signed integer. Maps to "int" type in Java.
             *
             * @param fieldName Field name.
             * @return Result.
             */
            int32_t ReadInt32(const char* fieldName);

            /**
             * Read array of 32-byte signed integers. Maps to "int[]" type in Java.
             *
             * @param fieldName Field name.
             * @param res Array to store data to.
             * @param len Expected length of array.             
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t ReadInt32Array(const char* fieldName, int32_t* res, const int32_t len);

            /**
             * Read 64-byte signed integer. Maps to "long" type in Java.
             *
             * @param fieldName Field name.
             * @return Result.
             */
            int64_t ReadInt64(const char* fieldName);

            /**
             * Read array of 64-byte signed integers. Maps to "long[]" type in Java.
             *
             * @param fieldName Field name.
             * @param res Array to store data to.
             * @param len Expected length of array.             
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t ReadInt64Array(const char* fieldName, int64_t* res, const int32_t len);

            /**
             * Read float. Maps to "float" type in Java.
             *
             * @param fieldName Field name.
             * @return Result.
             */
            float ReadFloat(const char* fieldName);

            /**
             * Read array of floats. Maps to "float[]" type in Java.
             * 
             * @param fieldName Field name.
             * @param res Array to store data to.
             * @param len Expected length of array.             
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t ReadFloatArray(const char* fieldName, float* res, const int32_t len);

            /**
             * Read double. Maps to "double" type in Java.
             *
             * @param fieldName Field name.
             * @return Result.
             */
            double ReadDouble(const char* fieldName);

            /**
             * Read array of doubles. Maps to "double[]" type in Java.
             *
             * @param fieldName Field name.
             * @param res Array to store data to.
             * @param len Expected length of array.             
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t ReadDoubleArray(const char* fieldName, double* res, const int32_t len);

            /**
             * Read Guid. Maps to "UUID" type in Java.
             *
             * @param fieldName Field name.
             * @return Result.
             */
            Guid ReadGuid(const char* fieldName);

            /**
             * Read array of Guids. Maps to "UUID[]" type in Java.
             *
             * @param fieldName Field name.
             * @param res Array to store data to.
             * @param len Expected length of array.             
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t ReadGuidArray(const char* fieldName, Guid* res, const int32_t len);

            /**
             * Read string.
             *
             * @param fieldName Field name.
             * @param res Array to store data to.
             * @param len Expected length of string. NULL terminator will be set in case len is
             *     greater than real string length.             
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t ReadString(const char* fieldName, char* res, const int32_t len);

            /**
             * Read string from the stream.
             *
             * @param fieldName Field name.
             * @return String.
             */
            std::string ReadString(const char* fieldName)
            {
                int32_t len = ReadString(fieldName, NULL, 0);

                if (len != -1)
                {
                    ignite::impl::utils::SafeArray<char> arr(len + 1);

                    ReadString(fieldName, arr.target, len + 1);

                    return std::string(arr.target);
                }
                else
                    return std::string();
            }

            /**
             * Start string array read.
             *
             * @param fieldName Field name.
             * @return String array reader.
             */
            PortableStringArrayReader ReadStringArray(const char* fieldName);

            /**
             * Start array read.
             *
             * @param fieldName Field name.
             * @return Array reader.
             */
            template<typename T>
            PortableArrayReader<T> ReadArray(const char* fieldName)
            {
                int32_t size;

                int32_t id = impl->ReadArray(fieldName, &size);

                return PortableArrayReader<T>(impl, id, size);
            }

            /**
             * Start collection read.
             *
             * @param fieldName Field name.
             * @return Collection reader.
             */
            template<typename T>
            PortableCollectionReader<T> ReadCollection(const char* fieldName)
            {
                CollectionType typ;
                int32_t size;

                int32_t id = impl->ReadCollection(fieldName, &typ, &size);

                return PortableCollectionReader<T>(impl, id, typ, size);
            }

            /**
             * Start map read.
             *
             * @param fieldName Field name.
             * @return Map reader.
             */
            template<typename K, typename V>
            PortableMapReader<K, V> ReadMap(const char* fieldName)
            {
                MapType typ;
                int32_t size;

                int32_t id = impl->ReadMap(fieldName, &typ, &size);

                return PortableMapReader<K, V>(impl, id, typ, size);
            }

            /**
             * Read object.
             *
             * @param fieldName Field name.
             * @return Object.
             */
            template<typename T>
            T ReadObject(const char* fieldName)
            {
                return impl->ReadObject<T>(fieldName);
            }

            /**
             * Get raw reader for this reader.
             *
             * @return Raw reader.
             */
            PortableRawReader RawReader();
        private:
            /** Implementation delegate. */
            ignite::impl::portable::PortableReaderImpl* impl;
        };            
    }
}

#endif