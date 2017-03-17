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

#ifndef _IGNITE_IMPL_BINARY_BINARY_WRITER
#define _IGNITE_IMPL_BINARY_BINARY_WRITER

#include <cstring>
#include <string>
#include <stdint.h>

#include <ignite/common/common.h>
#include <ignite/common/concurrent.h>

#include "ignite/impl/interop/interop_output_stream.h"
#include "ignite/impl/binary/binary_common.h"
#include "ignite/impl/binary/binary_id_resolver.h"
#include "ignite/impl/binary/binary_type_manager.h"
#include "ignite/impl/binary/binary_utils.h"
#include "ignite/impl/binary/binary_schema.h"
#include "ignite/impl/binary/binary_type_impl.h"
#include "ignite/binary/binary_consts.h"
#include "ignite/binary/binary_type.h"
#include "ignite/guid.h"
#include "ignite/date.h"
#include "ignite/timestamp.h"
#include "binary_type_manager.h"

namespace ignite
{
    namespace impl
    {
        namespace binary
        {
            /**
             * Internal implementation of binary reader.
             */
            class IGNITE_IMPORT_EXPORT BinaryWriterImpl
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param stream Interop stream.
                 * @param idRslvr Binary ID resolver.
                 * @param metaMgr Type manager.
                 * @param metaHnd Type handler.
                 */
                BinaryWriterImpl(ignite::impl::interop::InteropOutputStream* stream, BinaryIdResolver* idRslvr, 
                    BinaryTypeManager* metaMgr, BinaryTypeHandler* metaHnd, int32_t start);
                
                /**
                 * Constructor used to construct light-weight writer allowing only raw operations 
                 * and primitive objects.
                 *
                 * @param stream Interop stream.
                 * @param metaMgr Type manager.
                 */
                BinaryWriterImpl(ignite::impl::interop::InteropOutputStream* stream, BinaryTypeManager* metaMgr);

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
                 * Write Date. Maps to "Date" type in Java.
                 *
                 * @param val Value.
                 */
                void WriteDate(const Date& val);

                /**
                 * Write array of Dates. Maps to "Date[]" type in Java.
                 *
                 * @param val Array.
                 * @param len Array length.
                 */
                void WriteDateArray(const Date* val, const int32_t len);

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
                void WriteDateArray(const char* fieldName, const Date* val, const int32_t len);

                /**
                 * Write Timestamp. Maps to "Timestamp" type in Java.
                 *
                 * @param val Value.
                 */
                void WriteTimestamp(const Timestamp& val);

                /**
                 * Write array of Timestamps. Maps to "Timestamp[]" type in Java.
                 *
                 * @param val Array.
                 * @param len Array length.
                 */
                void WriteTimestampArray(const Timestamp* val, const int32_t len);

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
                void WriteTimestampArray(const char* fieldName, const Timestamp* val, const int32_t len);

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
                 * @param fieldName Field name.
                 * @param val String.
                 * @param len String length (characters).
                 */
                void WriteString(const char* fieldName, const char* val, const int32_t len);

                /**
                 * Start string array write.
                 *
                 * @param typ Collection type.
                 * @return Session ID.
                 */
                int32_t WriteStringArray();

                /**
                 * Start string array write.
                 *
                 * @param fieldName Field name.
                 * @return Session ID.
                 */
                int32_t WriteStringArray(const char* fieldName);

                /**
                 * Write string element.
                 *
                 * @param id Session ID.
                 * @param val Value.
                 * @param len Length.
                 */
                void WriteStringElement(int32_t id, const char* val, int32_t len);

                /**
                 * Write NULL value.
                 */
                void WriteNull();

                /**
                 * Write NULL value.
                 *
                 * @param fieldName Field name.
                 */
                void WriteNull(const char* fieldName);

                /**
                 * Start array write.
                 *
                 * @param typ Collection type.
                 * @return Session ID.
                 */
                int32_t WriteArray();

                /**
                 * Start array write.
                 *
                 * @param fieldName Field name.
                 * @return Session ID.
                 */
                int32_t WriteArray(const char* fieldName);
                                
                /**
                 * Start collection write.
                 *
                 * @param typ Collection type.
                 * @return Session ID.
                 */
                int32_t WriteCollection(ignite::binary::CollectionType typ);

                /**
                 * Start collection write.
                 *
                 * @param fieldName Field name.
                 * @param typ Collection type.
                 * @return Session ID.
                 */
                int32_t WriteCollection(const char* fieldName, ignite::binary::CollectionType typ);

                /**
                 * Write values in interval [first, last).
                 *
                 * @param first Iterator pointing to the beginning of the interval.
                 * @param last Iterator pointing to the end of the interval.
                 * @param typ Collection type.
                 */
                template<typename InputIterator>
                void WriteCollection(InputIterator first, InputIterator last, ignite::binary::CollectionType typ)
                {
                    StartContainerSession(true);

                    WriteCollectionWithinSession(first, last, typ);
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
                void WriteCollection(const char* fieldName, InputIterator first, InputIterator last,
                    ignite::binary::CollectionType typ)
                {
                    StartContainerSession(false);

                    WriteFieldId(fieldName, IGNITE_TYPE_COLLECTION);

                    WriteCollectionWithinSession(first, last, typ);
                }
                
                /**
                 * Start map write.
                 *
                 * @param typ Map type.
                 * @return Session ID.
                 */
                int32_t WriteMap(ignite::binary::MapType typ);

                /**
                 * Start map write.
                 *
                 * @param fieldName Field name.
                 * @param typ Map type.
                 * @return Session ID.
                 */
                int32_t WriteMap(const char* fieldName, ignite::binary::MapType typ);

                /**
                 * Write collection element.
                 *
                 * @param id Session ID.
                 * @param val Value.
                 */
                template<typename T>
                void WriteElement(int32_t id, T val)
                {
                    CheckSession(id);
                                        
                    WriteTopObject<T>(val);

                    elemCnt++;
                }

                /**
                 * Write map element.
                 *
                 * @param id Session ID.
                 * @param key Key.
                 * @param val Value.
                 */
                template<typename K, typename V>
                void WriteElement(int32_t id, K key, V val)
                {
                    CheckSession(id);

                    WriteTopObject<K>(key);
                    WriteTopObject<V>(val);

                    elemCnt++;
                }

                /**
                 * Commit container write session.
                 *
                 * @param id Session ID.
                 */
                void CommitContainer(int32_t id);                

                /**
                 * Write object.
                 *
                 * @param val Object.
                 */
                template<typename T>
                void WriteObject(T val)
                {
                    CheckRawMode(true);

                    WriteTopObject(val);
                }

                /**
                 * Write object.
                 *
                 * @param fieldName Field name.
                 * @param val Object.
                 */
                template<typename T>
                void WriteObject(const char* fieldName, T val)
                {
                    CheckRawMode(false);

                    WriteFieldId(fieldName, IGNITE_TYPE_OBJECT);

                    WriteTopObject(val);
                }

                /**
                 * Set raw mode.
                 */
                void SetRawMode();

                /**
                 * Get raw position.
                 */
                int32_t GetRawPosition() const;

                /**
                 * Write object.
                 *
                 * @param obj Object to write.
                 */
                template<typename T>
                void WriteTopObject(const T& obj)
                {
                    ignite::binary::BinaryType<T> type;

                    if (type.IsNull(obj))
                        stream->WriteInt8(IGNITE_HDR_NULL);
                    else
                    {
                        TemplatedBinaryIdResolver<T> idRslvr(type);
                        ignite::common::concurrent::SharedPointer<BinaryTypeHandler> metaHnd;

                        if (metaMgr)
                            metaHnd = metaMgr->GetHandler(idRslvr.GetTypeId());

                        int32_t pos = stream->Position();

                        BinaryWriterImpl writerImpl(stream, &idRslvr, metaMgr, metaHnd.Get(), pos);
                        ignite::binary::BinaryWriter writer(&writerImpl);

                        stream->WriteInt8(IGNITE_HDR_FULL);
                        stream->WriteInt8(IGNITE_PROTO_VER);
                        stream->WriteInt16(IGNITE_BINARY_FLAG_USER_TYPE);
                        stream->WriteInt32(idRslvr.GetTypeId());

                        int32_t hashPos = stream->Reserve(4);

                        // Reserve space for the Object Lenght, Schema ID and Schema or Raw Offset.
                        stream->Reserve(12);

                        type.Write(writer, obj);

                        writerImpl.PostWrite();

                        stream->Synchronize();

                        ignite::binary::BinaryObject binObj(*stream->GetMemory(), pos);
                        stream->WriteInt32(hashPos, impl::binary::GetHashCode<T>(obj, binObj));

                        if (metaMgr)
                            metaMgr->SubmitHandler(type.GetTypeName(), idRslvr.GetTypeId(), metaHnd.Get());
                    }
                }

                /**
                 * Perform all nessasary post-write operations.
                 * Includes:
                 * - writing object length;
                 * - writing schema offset;
                 * - writing schema id;
                 * - writing schema to the tail.
                 */
                void PostWrite();

                /**
                 * Check if the writer has object schema.
                 *
                 * @return True if has schema.
                 */
                bool HasSchema() const;
                
                /**
                 * Writes contating schema and clears current schema.
                 */
                void WriteAndClearSchema();

                /**
                 * Get underlying stream.
                 *
                 * @return Stream.
                 */
                impl::interop::InteropOutputStream* GetStream();
            private:
                /** Underlying stream. */
                ignite::impl::interop::InteropOutputStream* stream; 
                
                /** ID resolver. */
                BinaryIdResolver* idRslvr;
                
                /** Type manager. */
                BinaryTypeManager* metaMgr;
                
                /** Type handler. */
                BinaryTypeHandler* metaHnd;

                /** Type ID. */
                int32_t typeId;

                /** Elements write session ID generator. */
                int32_t elemIdGen;
                
                /** Elements write session ID. */
                int32_t elemId;
                
                /** Elements count. */
                int32_t elemCnt;
                
                /** Elements start position. */
                int32_t elemPos;

                /** Raw data offset. */
                int32_t rawPos;

                /** Schema of the current object. */
                BinarySchema schema;

                /** Writing start position. */
                int32_t start;

                IGNITE_NO_COPY_ASSIGNMENT(BinaryWriterImpl)

                /**
                 * Write a primitive value to stream in raw mode.
                 *
                 * @param val Value.
                 * @param func Stream function.
                 */
                template<typename T>
                void WritePrimitiveRaw(
                    const T val, 
                    void(*func)(interop::InteropOutputStream*, T)
                )
                {
                    CheckRawMode(true);
                    CheckSingleMode(true);

                    func(stream, val);
                }

                /**
                 * Write a primitive array to stream in raw mode.
                 *
                 * @param val Value.
                 * @param len Array length.
                 * @param func Stream function.
                 * @param hdr Header.
                 */
                template<typename T>
                void WritePrimitiveArrayRaw(
                    const T* val,
                    const int32_t len,
                    void(*func)(interop::InteropOutputStream*, const T*, const int32_t),
                    const int8_t hdr
                )
                {
                    CheckRawMode(true);
                    CheckSingleMode(true);

                    if (val)
                    {
                        stream->WriteInt8(hdr);
                        stream->WriteInt32(len);
                        func(stream, val, len);
                    }
                    else
                        stream->WriteInt8(IGNITE_HDR_NULL);
                }

                /**
                 * Write a primitive value to stream.
                 *
                 * @param fieldName Field name.
                 * @param val Value.
                 * @param func Stream function.
                 * @param typ Field type ID.
                 * @param len Field length.
                 */
                template<typename T>
                void WritePrimitive(
                    const char* fieldName, 
                    const T val, 
                    void(*func)(interop::InteropOutputStream*, T), 
                    const int8_t typ, 
                    const int32_t len
                )
                {
                    CheckRawMode(false);
                    CheckSingleMode(true);

                    WriteFieldId(fieldName, typ);

                    stream->WriteInt8(typ);

                    func(stream, val);
                }

                /**
                 * Write a primitive array to stream.
                 *
                 * @param fieldName Field name.
                 * @param val Value.
                 * @param len Array length.
                 * @param func Stream function.
                 * @param hdr Header.
                 * @param lenShift Length shift.
                 */
                template<typename T>
                void WritePrimitiveArray(
                    const char* fieldName, 
                    const T* val, 
                    const int32_t len, 
                    void(*func)(interop::InteropOutputStream*, const T*, const int32_t), 
                    const int8_t hdr, 
                    const int32_t lenShift
                )
                {
                    CheckRawMode(false);
                    CheckSingleMode(true);

                    WriteFieldId(fieldName, hdr);

                    if (val)
                    {
                        stream->WriteInt8(hdr);
                        stream->WriteInt32(len);
                        func(stream, val, len);
                    }
                    else
                    {
                        stream->WriteInt8(IGNITE_HDR_NULL);
                    }
                }

                /**
                 * Write values in interval [first, last).
                 * New session should be started prior to call to this method.
                 * @param first Iterator pointing to the beginning of the interval.
                 * @param last Iterator pointing to the end of the interval.
                 * @param typ Collection type.
                 */
                template<typename InputIterator>
                void WriteCollectionWithinSession(InputIterator first, InputIterator last,
                    ignite::binary::CollectionType typ)
                {
                    stream->WriteInt8(IGNITE_TYPE_COLLECTION);
                    stream->Position(stream->Position() + 4);
                    stream->WriteInt8(typ);

                    for (InputIterator i = first; i != last; ++i)
                        WriteElement(elemId, *i);

                    CommitContainer(elemId);
                }

                /**
                 * Check raw mode.
                 *
                 * @param expected Expected raw mode of the reader.
                 */
                void CheckRawMode(bool expected) const;

                /**
                 * Check whether writer is currently operating in single mode.
                 *
                 * @param expected Expected value.
                 */
                void CheckSingleMode(bool expected) const;

                /**
                 * Start new container writer session.
                 *
                 * @param expRawMode Expected raw mode.
                 */
                void StartContainerSession(bool expRawMode);

                /**
                 * Check whether session ID matches.
                 *
                 * @param ses Expected session ID.
                 */
                void CheckSession(int32_t expSes) const;

                /**
                 * Write field ID.
                 *
                 * @param fieldName Field name.
                 * @param fieldTypeId Field type ID.
                 */
                void WriteFieldId(const char* fieldName, int32_t fieldTypeId);

                /**
                 * Write primitive value.
                 *
                 * @param obj Value.
                 * @param func Stream function.
                 * @param hdr Header.
                 */
                template<typename T>
                void WriteTopObject0(const T obj, void(*func)(impl::interop::InteropOutputStream*, T), const int8_t hdr)
                {
                    stream->WriteInt8(hdr);
                    func(stream, obj);
                }
            };

            template<>
            void IGNITE_IMPORT_EXPORT BinaryWriterImpl::WriteTopObject(const int8_t& obj);

            template<>
            void IGNITE_IMPORT_EXPORT BinaryWriterImpl::WriteTopObject(const bool& obj);

            template<>
            void IGNITE_IMPORT_EXPORT BinaryWriterImpl::WriteTopObject(const int16_t& obj);

            template<>
            void IGNITE_IMPORT_EXPORT BinaryWriterImpl::WriteTopObject(const uint16_t& obj);

            template<>
            void IGNITE_IMPORT_EXPORT BinaryWriterImpl::WriteTopObject(const int32_t& obj);

            template<>
            void IGNITE_IMPORT_EXPORT BinaryWriterImpl::WriteTopObject(const int64_t& obj);

            template<>
            void IGNITE_IMPORT_EXPORT BinaryWriterImpl::WriteTopObject(const float& obj);

            template<>
            void IGNITE_IMPORT_EXPORT BinaryWriterImpl::WriteTopObject(const double& obj);

            template<>
            void IGNITE_IMPORT_EXPORT BinaryWriterImpl::WriteTopObject(const Guid& obj);

            template<>
            void IGNITE_IMPORT_EXPORT BinaryWriterImpl::WriteTopObject(const Date& obj);

            template<>
            void IGNITE_IMPORT_EXPORT BinaryWriterImpl::WriteTopObject(const Timestamp& obj);

            template<>
            inline void IGNITE_IMPORT_EXPORT BinaryWriterImpl::WriteTopObject(const std::string& obj)
            {
                const char* obj0 = obj.c_str();

                int32_t len = static_cast<int32_t>(obj.size());

                stream->WriteInt8(IGNITE_TYPE_STRING);

                BinaryUtils::WriteString(stream, obj0, len);
            }
        }
    }
}

#endif //_IGNITE_IMPL_BINARY_BINARY_WRITER