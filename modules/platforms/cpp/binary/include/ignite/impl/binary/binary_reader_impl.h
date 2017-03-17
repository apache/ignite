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

#ifndef _IGNITE_IMPL_BINARY_BINARY_READER
#define _IGNITE_IMPL_BINARY_BINARY_READER

#include <stdint.h>

#include <ignite/common/common.h>
#include <ignite/common/fixed_size_array.h>

#include "ignite/impl/interop/interop_input_stream.h"
#include "ignite/impl/binary/binary_common.h"
#include "ignite/impl/binary/binary_id_resolver.h"
#include "ignite/impl/binary/binary_schema.h"
#include "ignite/common/utils.h"
#include "ignite/binary/binary_consts.h"
#include "ignite/binary/binary_type.h"
#include "ignite/guid.h"
#include "ignite/date.h"
#include "ignite/timestamp.h"

namespace ignite
{
    namespace impl
    {
        namespace binary
        {
            /**
             * Internal implementation of binary reader.
             */
            class IGNITE_IMPORT_EXPORT BinaryReaderImpl
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param stream Interop stream.
                 * @param idRslvr Binary ID resolver.
                 * @param pos Object position in the stream.
                 * @param usrType user type flag.
                 * @param typeId Type ID.
                 * @param hashcode Hash code.
                 * @param len Length in bytes.
                 * @param rawOff Raw data offset.
                 * @param footerBegin Footer beginning absolute position in stream.
                 * @param footerEnd Footer ending absolute position in stream.
                 */
                BinaryReaderImpl(interop::InteropInputStream* stream, BinaryIdResolver* idRslvr,
                    int32_t pos, bool usrType, int32_t typeId, int32_t hashCode, int32_t len, int32_t rawOff,
                    int32_t footerBegin, int32_t footerEnd, BinaryOffsetType schemaType);

                /**
                 * Constructor used to construct light-weight reader allowing only raw operations 
                 * and read of primitives.
                 *
                 * @param stream Interop stream.
                 */
                BinaryReaderImpl(interop::InteropInputStream* stream);

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
                int32_t ReadInt8Array(int8_t* res, const int32_t len);

                /**
                 * Read 8-byte signed integer. Maps to "byte" type in Java.
                 *
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
                 * Read bool. Maps to "boolean" type in Java.
                 *
                 * @return Result.
                 */
                bool ReadBool();

                /**
                 * Read bool array. Maps to "boolean[]" type in Java.
                 *
                 * @param res Array to store data to.
                 * @param len Expected length of array.                 
                 * @return Actual amount of elements read. If "len" argument is less than actual
                 *     array size or resulting array is set to null, nothing will be written
                 *     to resulting array and returned value will contain required array length.
                 *     -1 will be returned in case array in stream was null.
                 */
                int32_t ReadBoolArray(bool* res, const int32_t len);

                /**
                 * Read bool. Maps to "short" type in Java.
                 *
                 * @param fieldName Field name.
                 * @return Result.
                 */
                bool ReadBool(const char* fieldName);

                /**
                 * Read bool array. Maps to "bool[]" type in Java.
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
                int32_t ReadInt16Array(int16_t* res, const int32_t len);

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
                int32_t ReadUInt16Array(uint16_t* res, const int32_t len);

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
                int32_t ReadInt32Array(int32_t* res, const int32_t len);

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
                int32_t ReadInt64Array(int64_t* res, const int32_t len);

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
                 * @return Result.
                 */
                float ReadFloat();

                /**
                 * Read float array. Maps to "float[]" type in Java.
                 *
                 * @param res Array to store data to.
                 * @param len Expected length of array.                 
                 * @return Actual amount of elements read. If "len" argument is less than actual
                 *     array size or resulting array is set to null, nothing will be written
                 *     to resulting array and returned value will contain required array length.
                 *     -1 will be returned in case array in stream was null.
                 */
                int32_t ReadFloatArray(float* res, const int32_t len);

                /**
                 * Read float. Maps to "float" type in Java.
                 *
                 * @param fieldName Field name.
                 * @return Result.
                 */
                float ReadFloat(const char* fieldName);

                /**
                 * Read float array. Maps to "float[]" type in Java.
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
                 * @return Result.
                 */
                double ReadDouble();
                
                /**
                 * Read double array. Maps to "double[]" type in Java.
                 *
                 * @param res Array to store data to.
                 * @param len Expected length of array.                 
                 * @return Actual amount of elements read. If "len" argument is less than actual
                 *     array size or resulting array is set to null, nothing will be written
                 *     to resulting array and returned value will contain required array length.
                 *     -1 will be returned in case array in stream was null.
                 */
                int32_t ReadDoubleArray(double* res, const int32_t len);

                /**
                 * Read double. Maps to "double" type in Java.
                 *
                 * @param fieldName Field name.
                 * @return Result.
                 */
                double ReadDouble(const char* fieldName);

                /**
                 * Read double array. Maps to "double[]" type in Java.
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
                 * @return Result.
                 */
                Guid ReadGuid();

                /**
                 * Read array of Guids. Maps to "UUID[]" type in Java.
                 *
                 * @param res Array to store data to.
                 * @param len Expected length of array.                 
                 * @return Actual amount of elements read. If "len" argument is less than actual
                 *     array size or resulting array is set to null, nothing will be written
                 *     to resulting array and returned value will contain required array length.
                 *     -1 will be returned in case array in stream was null.
                 */
                int32_t ReadGuidArray(Guid* res, const int32_t len);

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
                 * Read Date. Maps to "Date" type in Java.
                 *
                 * @return Result.
                 */
                Date ReadDate();

                /**
                 * Read array of Dates. Maps to "Date[]" type in Java.
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
                 * Read Date. Maps to "Date" type in Java.
                 *
                 * @param fieldName Field name.
                 * @return Result.
                 */
                Date ReadDate(const char* fieldName);

                /**
                 * Read array of Dates. Maps to "Date[]" type in Java.
                 *
                 * @param fieldName Field name.
                 * @param res Array to store data to.
                 * @param len Expected length of array.
                 * @return Actual amount of elements read. If "len" argument is less than actual
                 *     array size or resulting array is set to null, nothing will be written
                 *     to resulting array and returned value will contain required array length.
                 *     -1 will be returned in case array in stream was null.
                 */
                int32_t ReadDateArray(const char* fieldName, Date* res, const int32_t len);

                /**
                 * Read Timestamp. Maps to "Timestamp" type in Java.
                 *
                 * @return Result.
                 */
                Timestamp ReadTimestamp();

                /**
                 * Read array of Timestamps. Maps to "Timestamp[]" type in Java.
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
                 * Read Timestamp. Maps to "Timestamp" type in Java.
                 *
                 * @param fieldName Field name.
                 * @return Result.
                 */
                Timestamp ReadTimestamp(const char* fieldName);

                /**
                 * Read array of Timestamps. Maps to "Timestamp[]" type in Java.
                 *
                 * @param fieldName Field name.
                 * @param res Array to store data to.
                 * @param len Expected length of array.
                 * @return Actual amount of elements read. If "len" argument is less than actual
                 *     array size or resulting array is set to null, nothing will be written
                 *     to resulting array and returned value will contain required array length.
                 *     -1 will be returned in case array in stream was null.
                 */
                int32_t ReadTimestampArray(const char* fieldName, Timestamp* res, const int32_t len);

                /**
                 * Read string.
                 *
                 * @param len Expected length of string.
                 * @param res Array to store data to (should be able to acocmodate null-terminator).
                 * @return Actual amount of elements read. If "len" argument is less than actual
                 *     array size or resulting array is set to null, nothing will be written
                 *     to resulting array and returned value will contain required array length.
                 *     -1 will be returned in case array in stream was null.
                 */
                int32_t ReadString(char* res, const int32_t len);

                /**
                 * Read string.
                 *
                 * @param fieldName Field name.                 
                 * @param res Array to store data to (should be able to acocmodate null-terminator).
                 * @param len Expected length of string.
                 * @return Actual amount of elements read. If "len" argument is less than actual
                 *     array size or resulting array is set to null, nothing will be written
                 *     to resulting array and returned value will contain required array length.
                 *     -1 will be returned in case array in stream was null.
                 */
                int32_t ReadString(const char* fieldName, char* res, const int32_t len);
                
                /**
                 * Start string array read.
                 *
                 * @param size Array size.
                 * @return Read session ID.
                 */
                int32_t ReadStringArray(int32_t* size);

                /**
                 * Start string array read.
                 *
                 * @param fieldName Field name.
                 * @param size Array size.
                 * @return Read session ID.
                 */
                int32_t ReadStringArray(const char* fieldName, int32_t* size);

                /**
                 * Read string element.
                 *
                 * @param id Session ID.
                 * @param len Expected length of string.
                 * @param res Array to store data to (should be able to acocmodate null-terminator).
                 * @return Actual amount of elements read. If "len" argument is less than actual
                 *     array size or resulting array is set to null, nothing will be written
                 *     to resulting array and returned value will contain required array length.
                 *     -1 will be returned in case array in stream was null.
                 */
                int32_t ReadStringElement(int32_t id, char* res, const int32_t len);

                /**
                 * Start array read.
                 *
                 * @param size Array size.
                 * @return Read session ID.
                 */
                int32_t ReadArray(int32_t* size);

                /**
                 * Start array read.
                 *
                 * @param fieldName Field name.
                 * @param size Array size.
                 * @return Read session ID.
                 */
                int32_t ReadArray(const char* fieldName, int32_t* size);

                /**
                 * Start collection read.
                 *
                 * @param typ Collection type.
                 * @param size Collection size.
                 * @return Read session ID.
                 */
                int32_t ReadCollection(ignite::binary::CollectionType* typ, int32_t* size);

                /**
                 * Start collection read.
                 *
                 * @param fieldName Field name.
                 * @param typ Collection type.
                 * @param size Collection size.
                 * @return Read session ID.
                 */
                int32_t ReadCollection(const char* fieldName, ignite::binary::CollectionType* typ, int32_t* size);

                /**
                 * Read values and insert them to specified position.
                 *
                 * @param out Output iterator to the initial position in the destination sequence.
                 * @return Actual amount of elements read.
                 */
                template<typename T, typename OutputIterator>
                int32_t ReadCollection(OutputIterator out)
                {
                    int32_t size;
                    int32_t id = StartContainerSession(true, IGNITE_TYPE_COLLECTION, &size);

                    // Reading collection type. We don't need it here but it should be read.
                    if (size != -1)
                        stream->ReadInt8();

                    while (HasNextElement(id))
                    {
                        *out = ReadElement<T>(id);
                        ++out;
                    }

                    return size;
                }

                /**
                 * Read values and insert them to specified position.
                 *
                 * @param fieldName Field name.
                 * @param out Output iterator to the initial position in the destination sequence.
                 * @return Actual amount of elements read.
                 */
                template<typename T, typename OutputIterator>
                int32_t ReadCollection(const char* fieldName, OutputIterator out)
                {
                    CheckRawMode(false);
                    CheckSingleMode(true);

                    int32_t fieldId = idRslvr->GetFieldId(typeId, fieldName);
                    int32_t fieldPos = FindField(fieldId);

                    if (fieldPos <= 0)
                        return -1;

                    stream->Position(fieldPos);

                    int32_t size;
                    int32_t id = StartContainerSession(false, IGNITE_TYPE_COLLECTION, &size);

                    // Reading collection type. We don't need it here but it should be read.
                    if (size != -1)
                        stream->ReadInt8();

                    while (HasNextElement(id))
                    {
                        *out = ReadElement<T>(id);
                        ++out;
                    }

                    return size;
                }

                /**
                 * Start map read.
                 *
                 * @param typ Map type.
                 * @param size Map size.
                 * @return Read session ID.
                 */
                int32_t ReadMap(ignite::binary::MapType* typ, int32_t* size);

                /**
                 * Start map read.
                 *
                 * @param fieldName Field name.
                 * @param typ Map type.
                 * @param size Map size.
                 * @return Read session ID.
                 */
                int32_t ReadMap(const char* fieldName, ignite::binary::MapType* typ, int32_t* size);

                /**
                 * Read type of the collection.
                 *
                 * @return Collection type.
                 */
                ignite::binary::CollectionType ReadCollectionType();

                /**
                 * Read type of the collection.
                 *
                 * @param fieldName Field name.
                 * @return Collection type.
                 */
                ignite::binary::CollectionType ReadCollectionType(const char* fieldName);

                /**
                 * Read size of the collection.
                 *
                 * @return Collection size.
                 */
                int32_t ReadCollectionSize();

                /**
                 * Read size of the collection.
                 *
                 * @param fieldName Field name.
                 * @return Collection size.
                 */
                int32_t ReadCollectionSize(const char* fieldName);

                /**
                 * Check whether next value exists.
                 *
                 * @param id Session ID.
                 * @return True if next element exists for the given session.
                 */
                bool HasNextElement(int32_t id) const;

                /**
                 * Skip next value if it is the null.
                 *
                 * @return True if the null value has been detected and skipped.
                 */
                bool SkipIfNull();

                /**
                 * Read element.
                 *
                 * @param id Session ID.
                 * @return Value.
                 */
                template<typename T>
                T ReadElement(const int32_t id)
                {
                    CheckSession(id);

                    if (++elemRead == elemCnt) {
                        elemId = 0;
                        elemCnt = -1;
                        elemRead = 0;
                    }

                    return ReadTopObject<T>();
                }

                /**
                 * Read element.
                 *
                 * @param id Session ID.
                 * @param key Key.
                 * @param val Value.
                 */
                template<typename K, typename V>
                void ReadElement(const int32_t id, K* key, V* val)
                {
                    CheckSession(id);

                    if (++elemRead == elemCnt) {
                        elemId = 0;
                        elemCnt = -1;
                        elemRead = 0;
                    }

                    *key = ReadTopObject<K>();
                    *val = ReadTopObject<V>();
                }

                /**
                 * Read object.
                 *
                 * @return Object.
                 */
                template<typename T>
                T ReadObject()
                {
                    CheckRawMode(true);

                    return ReadTopObject<T>();
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
                    CheckRawMode(false);

                    int32_t fieldId = idRslvr->GetFieldId(typeId, fieldName); 

                    int32_t fieldPos = FindField(fieldId);

                    if (fieldPos <= 0)
                        return GetNull<T>();

                    stream->Position(fieldPos);

                    return ReadTopObject<T>();
                }

                /**
                 * Set raw mode.
                 */
                void SetRawMode();

                /**
                 * Read object.
                 *
                 * @return Read object.
                 */
                template<typename T>
                T ReadTopObject()
                {
                    int32_t pos = stream->Position();
                    int8_t hdr = stream->ReadInt8();

                    switch (hdr)
                    {
                        case IGNITE_HDR_NULL:
                        {
                            return GetNull<T>();
                        }

                        case IGNITE_HDR_HND:
                        {
                            IGNITE_ERROR_1(ignite::IgniteError::IGNITE_ERR_BINARY, 
                                           "Circular references are not supported.");
                        }

                        case IGNITE_TYPE_BINARY:
                        {
                            int32_t portLen = stream->ReadInt32(); // Total length of binary object.
                            int32_t curPos = stream->Position();
                            int32_t portOff = stream->ReadInt32(curPos + portLen);

                            stream->Position(curPos + portOff); // Position stream right on the object.

                            T val = ReadTopObject<T>();

                            stream->Position(curPos + portLen + 4); // Position stream after binary.

                            return val;
                        }

                        case IGNITE_HDR_FULL:
                        {
                            int8_t protoVer = stream->ReadInt8();

                            if (protoVer != IGNITE_PROTO_VER) {
                                IGNITE_ERROR_2(ignite::IgniteError::IGNITE_ERR_BINARY, 
                                               "Unsupported binary protocol version: ", protoVer);
                            }

                            int16_t flags = stream->ReadInt16();

                            if (flags & IGNITE_BINARY_FLAG_COMPACT_FOOTER) {
                                IGNITE_ERROR_2(ignite::IgniteError::IGNITE_ERR_BINARY,
                                    "Unsupported binary protocol flag: IGNITE_BINARY_FLAG_COMPACT_FOOTER: ", 
                                    IGNITE_BINARY_FLAG_COMPACT_FOOTER);
                            }

                            int32_t typeId = stream->ReadInt32();
                            int32_t hashCode = stream->ReadInt32();
                            int32_t len = stream->ReadInt32();

                            // Ignoring Schema Id for now.
                            stream->ReadInt32();

                            int32_t schemaOrRawOff = stream->ReadInt32();

                            int32_t rawOff;
                            int32_t footerBegin;

                            if (flags & IGNITE_BINARY_FLAG_HAS_SCHEMA)
                                footerBegin = pos + schemaOrRawOff;
                            else
                                footerBegin = pos + len;

                            BinaryOffsetType schemaType;

                            if (flags & IGNITE_BINARY_FLAG_OFFSET_ONE_BYTE)
                                schemaType = OFFSET_TYPE_ONE_BYTE;
                            else if (flags & IGNITE_BINARY_FLAG_OFFSET_TWO_BYTES)
                                schemaType = OFFSET_TYPE_TWO_BYTES;
                            else
                                schemaType = OFFSET_TYPE_FOUR_BYTES;

                            int32_t footerEnd;

                            if (flags & IGNITE_BINARY_FLAG_HAS_RAW)
                            {
                                // 4 is the size of RawOffset field at the end of the packet.
                                footerEnd = pos + len - 4;

                                rawOff = stream->ReadInt32(footerEnd);
                            }
                            else
                            {
                                footerEnd = pos + len;

                                rawOff = schemaOrRawOff;
                            }

                            bool usrType = flags & IGNITE_BINARY_FLAG_USER_TYPE;

                            ignite::binary::BinaryType<T> type;
                            TemplatedBinaryIdResolver<T> idRslvr(type);
                            BinaryReaderImpl readerImpl(stream, &idRslvr, pos, usrType,
                                                        typeId, hashCode, len, rawOff,
                                                        footerBegin, footerEnd, schemaType);
                            ignite::binary::BinaryReader reader(&readerImpl);

                            T val = type.Read(reader);

                            stream->Position(pos + len);

                            return val;
                        }

                        default:
                        {
                            IGNITE_ERROR_2(ignite::IgniteError::IGNITE_ERR_BINARY, 
                                           "Unexpected header during deserialization: ", static_cast<int>(hdr));
                        }
                    }
                }

                /**
                 * Get NULL value for the given type.
                 */
                template<typename T>
                T GetNull() const
                {
                    ignite::binary::BinaryType<T> type;

                    return type.GetNull();
                }

                /**
                 * Get underlying stream.
                 *
                 * @return Stream.
                 */
                interop::InteropInputStream* GetStream();
            private:
                /** Underlying stream. */
                interop::InteropInputStream* stream;

                /** ID resolver. */
                BinaryIdResolver* idRslvr;

                /** Position in the stream where this object starts. */
                int32_t pos;

                /** Whether this is user type or system type. */
                bool usrType;

                /** Type ID as defined in the stream. */
                int32_t typeId;

                /** Hash code. */
                int32_t hashCode;

                /** Total object length in the stream. */
                int32_t len;

                /** Raw data offset. */
                int32_t rawOff;

                /** Raw mode flag. */
                bool rawMode;

                /** Elements read session ID generator. */
                int32_t elemIdGen;

                /** Elements read session ID. */
                int32_t elemId;

                /** Total amount of elements in collection. */
                int32_t elemCnt;

                /** Amount of elements read. */
                int32_t elemRead;

                /** Footer beginning position. */
                int32_t footerBegin;

                /** Footer ending position. */
                int32_t footerEnd;

                /** Object schema type. */
                BinaryOffsetType schemaType;

                IGNITE_NO_COPY_ASSIGNMENT(BinaryReaderImpl)
                    
                /**
                 * Internal routine to read Guid array.
                 *
                 * @param stream Stream.
                 * @param res Resulting array.
                 * @param len Length.
                 */
                static void ReadGuidArrayInternal(
                    interop::InteropInputStream* stream, 
                    Guid* res,
                    const int32_t len
                );

                /**
                 * Internal routine to read Date array.
                 *
                 * @param stream Stream.
                 * @param res Resulting array.
                 * @param len Length.
                 */
                static void ReadDateArrayInternal(
                    interop::InteropInputStream* stream, 
                    Date* res,
                    const int32_t len
                );

                /**
                 * Internal routine to read Timestamp array.
                 *
                 * @param stream Stream.
                 * @param res Resulting array.
                 * @param len Length.
                 */
                static void ReadTimestampArrayInternal(
                    interop::InteropInputStream* stream, 
                    Timestamp* res,
                    const int32_t len
                );

                /**
                 * Read single value in raw mode.
                 * 
                 * @param stream Stream.
                 * @param func Function to be invoked on stream.
                 * @return Result.
                 */
                template<typename T>
                T ReadRaw(
                    T(*func) (interop::InteropInputStream*)
                )
                {
                    {
                        CheckRawMode(true);
                        CheckSingleMode(true);

                        return func(stream);
                    }
                }

                /**
                 * Read single value.
                 *
                 * @param fieldName Field name.
                 * @param func Function to be invoked on stream.
                 * @param epxHdr Expected header.
                 * @param dflt Default value returned if field is not found.
                 * @return Result.
                 */
                template<typename T>
                T Read(
                    const char* fieldName, 
                    T(*func) (interop::InteropInputStream*), 
                    const int8_t expHdr, 
                    T dflt
                )
                {
                    {
                        CheckRawMode(false);
                        CheckSingleMode(true);

                        int32_t fieldId = idRslvr->GetFieldId(typeId, fieldName);
                        int32_t fieldPos = FindField(fieldId);

                        if (fieldPos <= 0)
                            return dflt;

                        stream->Position(fieldPos);

                        int8_t typeId = stream->ReadInt8();
                        
                        if (typeId == IGNITE_HDR_NULL)
                            return dflt;

                        if (typeId != expHdr)
                        {
                            int32_t pos = stream->Position();

                            IGNITE_ERROR_FORMATTED_3(IgniteError::IGNITE_ERR_BINARY, "Invalid type ID", 
                                "position", pos, "expected", static_cast<int>(expHdr), "actual", static_cast<int>(typeId))
                        }

                        return func(stream);
                    }
                }

                /**
                 * Read array in raw mode.
                 *
                 * @param res Resulting array.
                 * @param len Length.                 
                 * @param func Function to be invoked on stream.
                 * @param expHdr Expected header.
                 * @return Length.
                 */
                template<typename T>
                int32_t ReadRawArray(
                    T* res,
                    const int32_t len,
                    void(*func)(interop::InteropInputStream*, T* const, const int32_t),
                    const int8_t expHdr
                )
                {
                    {
                        CheckRawMode(true);
                        CheckSingleMode(true);

                        return ReadArrayInternal(res, len, stream, func, expHdr);
                    }
                }

                /**
                 * Read array.
                 *
                 * @param fieldName Field name.
                 * @param res Resulting array.
                 * @param len Length.
                 * @param func Function to be invoked on stream.
                 * @param expHdr Expected header.
                 * @return Length.
                 */
                template<typename T>
                int32_t ReadArray(
                    const char* fieldName,
                    T* res,
                    const int32_t len,                    
                    void(*func)(interop::InteropInputStream*, T* const, const int32_t),
                    const int8_t expHdr
                )
                {
                    {
                        CheckRawMode(false);
                        CheckSingleMode(true);
                        
                        int32_t fieldId = idRslvr->GetFieldId(typeId, fieldName);
                        int32_t fieldPos = FindField(fieldId);

                        if (fieldPos <= 0)
                            return -1;

                        stream->Position(fieldPos);

                        int32_t realLen = ReadArrayInternal(res, len, stream, func, expHdr);

                        return realLen;
                    }
                }

                /**
                 * Internal read array routine.
                 *
                 * @param res Resulting array.
                 * @param len Length.                 
                 * @param stream Stream.
                 * @param func Function to be invoked on stream.
                 * @param expHdr Expected header.
                 * @return Length.
                 */
                template<typename T>
                static int32_t ReadArrayInternal(
                    T* res,
                    const int32_t len,
                    interop::InteropInputStream* stream,
                    void(*func)(interop::InteropInputStream*, T* const, const int32_t),
                    const int8_t expHdr
                )
                {
                    {
                        int8_t hdr = stream->ReadInt8();

                        if (hdr == expHdr)
                        {
                            int32_t realLen = stream->ReadInt32();

                            if (realLen == 0 || (res && len >= realLen))
                                func(stream, res, realLen);
                            else
                                stream->Position(stream->Position() - 5);

                            return realLen;
                        }
                        else if (hdr != IGNITE_HDR_NULL)
                            ThrowOnInvalidHeader(stream->Position() - 1, expHdr, hdr);

                        return -1;
                    }
                }

                /**
                 * Read nullable value.
                 *
                 * @param stream Stream.
                 * @param func Function to be invoked on stream.
                 * @param expHdr Expected header.
                 */
                template<typename T>
                static T ReadNullable(
                    interop::InteropInputStream* stream,
                    T(*func)(interop::InteropInputStream*), 
                    const int8_t expHdr
                )
                {
                    {
                        int8_t hdr = stream->ReadInt8();

                        if (hdr == expHdr)
                            return func(stream);
                        else if (hdr == IGNITE_HDR_NULL)
                            return T();
                        else {
                            ThrowOnInvalidHeader(stream->Position() - 1, expHdr, hdr);

                            return T();
                        }
                    }
                }

                /**
                 * Seek field with the given ID.
                 *
                 * @param fieldId Field ID.
                 * @return Field length or -1 if field is not found.
                 */
                int32_t FindField(const int32_t fieldId);

                /**
                 * Check raw mode.
                 * 
                 * @param expected Expected raw mode of the reader.
                 */
                void CheckRawMode(bool expected) const;

                /**
                 * Check whether reader is currently operating in single mode.
                 *
                 * @param expected Expected value.
                 */
                void CheckSingleMode(bool expected) const;

                /**
                 * Start new container reader session.
                 *
                 * @param expRawMode Expected raw mode.
                 * @param expHdr Expected header.
                 * @param size Container size.
                 * @return Session ID.
                 */
                int32_t StartContainerSession(const bool expRawMode, const int8_t expHdr, int32_t* size);

                /**
                 * Check whether session ID matches.
                 *
                 * @param ses Expected session ID.
                 */
                void CheckSession(int32_t expSes) const;

                /**
                 * Throw an error due to invalid header.
                 *
                 * @param pos Position in the stream.
                 * @param expHdr Expected header.
                 * @param hdr Actual header.
                 */
                static void ThrowOnInvalidHeader(int32_t pos, int8_t expHdr, int8_t hdr);

                /**
                 * Throw an error due to invalid header.
                 *
                 * @param expHdr Expected header.
                 * @param hdr Actual header.
                 */
                void ThrowOnInvalidHeader(int8_t expHdr, int8_t hdr) const;

                /**
                 * Internal string read routine.
                 *
                 * @param res Resulting array.
                 * @param len Length of array.
                 * @return Real array length.
                 */
                int32_t ReadStringInternal(char* res, const int32_t len);

                /**
                 * Read type of the collection. Do not preserve stream position.
                 *
                 * @return Collection type.
                 */
                ignite::binary::CollectionType ReadCollectionTypeUnprotected();

                /**
                 * Read size of the collection. Do not preserve stream position.
                 *
                 * @return Collection size.
                 */
                int32_t ReadCollectionSizeUnprotected();

                /**
                 * Read value.
                 *
                 * @param expHdr Expected header.
                 * @param func Function to be applied to the stream.
                 */
                template<typename T>
                T ReadTopObject0(const int8_t expHdr, T(*func) (ignite::impl::interop::InteropInputStream*))
                {
                    int8_t typeId = stream->ReadInt8();

                    if (typeId == expHdr)
                        return func(stream);
                    else if (typeId == IGNITE_HDR_NULL)
                        return GetNull<T>();
                    else {
                        int32_t pos = stream->Position() - 1;

                        IGNITE_ERROR_FORMATTED_3(IgniteError::IGNITE_ERR_BINARY, "Invalid header", "position", pos, "expected", (int)expHdr, "actual", (int)typeId)
                    }
                }

                /**
                 * Read value.
                 *
                 * @param expHdr Expected header.
                 * @param func Function to be applied to the stream.
                 * @param dflt Default value.
                 */
                template<typename T>
                T ReadTopObject0(const int8_t expHdr, T(*func) (ignite::impl::interop::InteropInputStream*), T dflt)
                {
                    int8_t typeId = stream->ReadInt8();

                    if (typeId == expHdr)
                        return func(stream);
                    else if (typeId == IGNITE_HDR_NULL)
                        return dflt;
                    else {
                        int32_t pos = stream->Position() - 1;

                        IGNITE_ERROR_FORMATTED_3(IgniteError::IGNITE_ERR_BINARY, "Invalid header", "position", pos, "expected", (int)expHdr, "actual", (int)typeId)
                    }
                }
            };

            template<>
            int8_t IGNITE_IMPORT_EXPORT BinaryReaderImpl::ReadTopObject<int8_t>();

            template<>
            bool IGNITE_IMPORT_EXPORT BinaryReaderImpl::ReadTopObject<bool>();

            template<>
            int16_t IGNITE_IMPORT_EXPORT BinaryReaderImpl::ReadTopObject<int16_t>();

            template<>
            uint16_t IGNITE_IMPORT_EXPORT BinaryReaderImpl::ReadTopObject<uint16_t>();

            template<>
            int32_t IGNITE_IMPORT_EXPORT BinaryReaderImpl::ReadTopObject<int32_t>();

            template<>
            int64_t IGNITE_IMPORT_EXPORT BinaryReaderImpl::ReadTopObject<int64_t>();

            template<>
            float IGNITE_IMPORT_EXPORT BinaryReaderImpl::ReadTopObject<float>();

            template<>
            double IGNITE_IMPORT_EXPORT BinaryReaderImpl::ReadTopObject<double>();

            template<>
            Guid IGNITE_IMPORT_EXPORT BinaryReaderImpl::ReadTopObject<Guid>();

            template<>
            Date IGNITE_IMPORT_EXPORT BinaryReaderImpl::ReadTopObject<Date>();

            template<>
            Timestamp IGNITE_IMPORT_EXPORT BinaryReaderImpl::ReadTopObject<Timestamp>();

            template<>
            inline std::string IGNITE_IMPORT_EXPORT BinaryReaderImpl::ReadTopObject<std::string>()
            {
                int8_t typeId = stream->ReadInt8();

                if (typeId == IGNITE_TYPE_STRING)
                {
                    int32_t realLen = stream->ReadInt32();

                    std::string res;

                    if (realLen > 0)
                    {
                        res.resize(realLen, 0);

                        stream->ReadInt8Array(reinterpret_cast<int8_t*>(&res[0]), realLen);
                    }

                    return res;
                }
                else if (typeId == IGNITE_HDR_NULL)
                    return std::string();
                else
                {
                    int32_t pos = stream->Position() - 1;

                    IGNITE_ERROR_FORMATTED_3(IgniteError::IGNITE_ERR_BINARY, "Invalid header", "position", pos,
                        "expected", static_cast<int>(IGNITE_TYPE_STRING), "actual", static_cast<int>(typeId))
                }
            }
        }
    }
}

#endif //_IGNITE_IMPL_BINARY_BINARY_READER
