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
 * Declares ignite::binary::BinaryRawReader class.
 */

#ifndef _IGNITE_BINARY_BINARY_RAW_READER
#define _IGNITE_BINARY_BINARY_RAW_READER

#include <stdint.h>
#include <string>

#include <ignite/common/common.h>

#include "ignite/impl/binary/binary_reader_impl.h"
#include "ignite/binary/binary_consts.h"
#include "ignite/binary/binary_containers.h"
#include "ignite/guid.h"
#include "ignite/date.h"
#include "ignite/timestamp.h"

namespace ignite
{    
    namespace binary
    {
        /**
         * Binary raw reader.
         *
         * This class implemented as a reference to an implementation so copying
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
        class IGNITE_IMPORT_EXPORT BinaryRawReader
        {
        public:
            /**
             * Constructor.
             *
             * Internal method. Should not be used by user.
             *
             * @param impl Implementation.
             */
            BinaryRawReader(ignite::impl::binary::BinaryReaderImpl* impl);
                        
            /**
             * Read 8-byte signed integer. Maps to "byte" type in Java.
             *
             * @return Result.
             */
            int8_t ReadInt8();

            /**
             * Read array of 8-byte signed integers. Maps to "byte[]" type in Java.
             *
             * @param res Array to store data to.
             * @param len Expected length of array.             
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written 
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t ReadInt8Array(int8_t* res, int32_t len);

            /**
             * Read bool. Maps to "boolean" type in Java.
             *
             * @return Result.
             */
            bool ReadBool();

            /**
             * Read array of bools. Maps to "boolean[]" type in Java.
             *
             * @param res Array to store data to.
             * @param len Expected length of array.             
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t ReadBoolArray(bool* res, int32_t len);
            
            /**
             * Read 16-byte signed integer. Maps to "short" type in Java.
             *
             * @return Result.
             */
            int16_t ReadInt16();

            /**
             * Read array of 16-byte signed integers. Maps to "short[]" type in Java.
             *
             * @param res Array to store data to.
             * @param len Expected length of array.             
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t ReadInt16Array(int16_t* res, int32_t len);

            /**
             * Read 16-byte unsigned integer. Maps to "char" type in Java.
             *
             * @return Result.
             */
            uint16_t ReadUInt16();

            /**
             * Read array of 16-byte unsigned integers. Maps to "char[]" type in Java.
             *
             * @param res Array to store data to.
             * @param len Expected length of array.             
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t ReadUInt16Array(uint16_t* res, int32_t len);

            /**
             * Read 32-byte signed integer. Maps to "int" type in Java.
             *
             * @return Result.
             */
            int32_t ReadInt32();
            
            /**
             * Read array of 32-byte signed integers. Maps to "int[]" type in Java.
             *
             * @param res Array to store data to.
             * @param len Expected length of array.             
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t ReadInt32Array(int32_t* res, int32_t len);

            /**
             * Read 64-byte signed integer. Maps to "long" type in Java.
             *
             * @return Result.
             */
            int64_t ReadInt64();

            /**
             * Read array of 64-byte signed integers. Maps to "long[]" type in Java.
             *
             * @param res Array to store data to.
             * @param len Expected length of array.
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t ReadInt64Array(int64_t* res, int32_t len);

            /**
             * Read float. Maps to "float" type in Java.
             *
             * @return Result.
             */
            float ReadFloat();
            
            /**
             * Read array of floats. Maps to "float[]" type in Java.
             *
             * @param res Array to store data to.
             * @param len Expected length of array.             
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t ReadFloatArray(float* res, int32_t len);

            /**
             * Read double. Maps to "double" type in Java.
             *
             * @return Result.
             */
            double ReadDouble();
            
            /**
             * Read array of doubles. Maps to "double[]" type in Java.
             *
             * @param res Array to store data to.
             * @param len Expected length of array.             
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t ReadDoubleArray(double* res, int32_t len);

            /**
             * Read Guid. Maps to "java.util.UUID" type in Java.
             *
             * @return Result.
             */
            Guid ReadGuid();

            /**
             * Read array of Guids. Maps to "java.util.UUID[]" type in Java.
             *
             * @param res Array to store data to.
             * @param len Expected length of array.             
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t ReadGuidArray(Guid* res, int32_t len);

            /**
             * Read Date. Maps to "java.util.Date" type in Java.
             *
             * @return Result.
             */
            Date ReadDate();

            /**
             * Read array of Dates. Maps to "java.util.Date[]" type in Java.
             *
             * @param res Array to store data to.
             * @param len Expected length of array.             
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t ReadDateArray(Date* res, int32_t len);

            /**
             * Read Timestamp. Maps to "java.sql.Timestamp" type in Java.
             *
             * @return Result.
             */
            Timestamp ReadTimestamp();

            /**
             * Read array of Timestamps. Maps to "java.sql.Timestamp[]" type in Java.
             *
             * @param res Array to store data to.
             * @param len Expected length of array.             
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t ReadTimestampArray(Timestamp* res, int32_t len);

            /**
             * Read string.
             *
             * @param res Array to store data to. 
             * @param len Expected length of string. NULL terminator will be set in case len is 
             *     greater than real string length.
             * @return Actual amount of elements read. If "len" argument is less than actual
             *     array size or resulting array is set to null, nothing will be written
             *     to resulting array and returned value will contain required array length.
             *     -1 will be returned in case array in stream was null.
             */
            int32_t ReadString(char* res, int32_t len);

            /**
             * Read string from the stream.
             *
             * @return String. 
             */
            std::string ReadString()
            {
                int32_t len = ReadString(NULL, 0);

                if (len != -1)
                {
                    ignite::common::FixedSizeArray<char> arr(len + 1);

                    ReadString(arr.GetData(), static_cast<int32_t>(arr.GetSize()));

                    return std::string(arr.GetData());
                }
                else
                    return std::string();
            }

            /**
             * Start string array read.
             *
             * @return String array reader.
             */
            BinaryStringArrayReader ReadStringArray();

            /**
             * Start array read.
             *
             * @return Array reader.
             */
            template<typename T>
            BinaryArrayReader<T> ReadArray()
            {
                int32_t size;

                int32_t id = impl->ReadArray(&size);

                return BinaryArrayReader<T>(impl, id, size);
            }

            /**
             * Start collection read.
             *
             * @return Collection reader.
             */
            template<typename T>
            BinaryCollectionReader<T> ReadCollection()
            {
                CollectionType typ;
                int32_t size;

                int32_t id = impl->ReadCollection(&typ, &size);

                return BinaryCollectionReader<T>(impl, id, typ, size);
            }

            /**
             * Read values and insert them to specified position.
             *
             * @param out Output iterator to the initial position in the destination sequence.
             * @return Number of elements that have been read.
             */
            template<typename T, typename OutputIterator>
            int32_t ReadCollection(OutputIterator out)
            {
                return impl->ReadCollection<T>(out);
            }

            /**
             * Start map read.
             *
             * @return Map reader.
             */
            template<typename K, typename V>
            BinaryMapReader<K, V> ReadMap()
            {
                MapType typ;
                int32_t size;

                int32_t id = impl->ReadMap(&typ, &size);

                return BinaryMapReader<K, V>(impl, id, typ, size);
            }

            /**
             * Read type of the collection.
             *
             * @return Collection type.
             */
            CollectionType ReadCollectionType();

            /**
             * Read type of the collection.
             *
             * @return Collection size.
             */
            int32_t ReadCollectionSize();

            /**
             * Read object.
             *
             * @return Object.
             */
            template<typename T>
            T ReadObject()
            {
                return impl->ReadObject<T>();
            }

            /**
             * Try read object.
             * Reads value, stores it to res and returns true if the value is
             * not null. Otherwise just returns false.
             *
             * @param res Read value is placed here if non-null.
             * @return True if the non-null value has been read and false
             *     otherwise.
             */
            template<typename T>
            bool TryReadObject(T& res)
            {
                if (impl->SkipIfNull())
                    return false;

                res = impl->ReadObject<T>();

                return true;
            }

        private:
            /** Implementation delegate. */
            ignite::impl::binary::BinaryReaderImpl* impl;  
        };
    }
}

#endif //_IGNITE_BINARY_BINARY_RAW_READER