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

#ifndef _IGNITE_IMPL_PORTABLE_WRITER
#define _IGNITE_IMPL_PORTABLE_WRITER

#include <cstring>
#include <string>
#include <stdint.h>

#include <ignite/common/common.h>
#include <ignite/common/concurrent.h>

#include "ignite/impl/interop/interop_output_stream.h"
#include "ignite/impl/portable/portable_common.h"
#include "ignite/impl/portable/portable_id_resolver.h"
#include "ignite/impl/portable/portable_metadata_manager.h"
#include "ignite/impl/portable/portable_utils.h"
#include "ignite/portable/portable_consts.h"
#include "ignite/portable/portable_type.h"
#include "ignite/guid.h"

namespace ignite
{
    namespace impl
    {
        namespace portable
        {
            /**
             * Internal implementation of portable reader.
             */
            class IGNITE_IMPORT_EXPORT PortableWriterImpl
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param stream Interop stream.
                 * @param idRslvr Portable ID resolver.
                 * @param metaMgr Metadata manager.
                 * @param metaHnd Metadata handler.
                 */
                PortableWriterImpl(ignite::impl::interop::InteropOutputStream* stream, PortableIdResolver* idRslvr, 
                    PortableMetadataManager* metaMgr, PortableMetadataHandler* metaHnd);
                
                /**
                 * Constructor used to construct light-weight writer allowing only raw operations 
                 * and primitive objects.
                 *
                 * @param stream Interop stream.
                 * @param metaMgr Metadata manager.
                 */
                PortableWriterImpl(ignite::impl::interop::InteropOutputStream* stream, PortableMetadataManager* metaMgr);

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
                int32_t WriteCollection(ignite::portable::CollectionType typ);

                /**
                 * Start collection write.
                 *
                 * @param fieldName Field name.
                 * @param typ Collection type.
                 * @return Session ID.
                 */
                int32_t WriteCollection(const char* fieldName, ignite::portable::CollectionType typ);
                
                /**
                 * Start map write.
                 *
                 * @param typ Map type.
                 * @return Session ID.
                 */
                int32_t WriteMap(ignite::portable::MapType typ);

                /**
                 * Start map write.
                 *
                 * @param fieldName Field name.
                 * @param typ Map type.
                 * @return Session ID.
                 */
                int32_t WriteMap(const char* fieldName, ignite::portable::MapType typ);

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
                        
                    // 1. Write field ID.
                    WriteFieldId(fieldName, IGNITE_TYPE_OBJECT);

                    // 2. Preserve space for length.
                    int32_t lenPos = stream->Position();
                    stream->Position(lenPos + 4);

                    // 3. Actual write.
                    WriteTopObject(val);

                    // 4. Write field length.
                    int32_t len = stream->Position() - lenPos - 4;
                    stream->WriteInt32(lenPos, len);
                }

                /**
                 * Set raw mode.
                 */
                void SetRawMode();

                /**
                 * Get raw position.
                 */
                int32_t GetRawPosition();

                /**
                 * Write object.
                 *
                 * @param obj Object to write.
                 */
                template<typename T>
                void WriteTopObject(const T& obj)
                {
                    ignite::portable::PortableType<T> type;

                    if (type.IsNull(obj))
                        stream->WriteInt8(IGNITE_HDR_NULL);
                    else
                    {
                        TemplatedPortableIdResolver<T> idRslvr(type);
                        ignite::common::concurrent::SharedPointer<PortableMetadataHandler> metaHnd;

                        if (metaMgr)                        
                            metaHnd = metaMgr->GetHandler(idRslvr.GetTypeId());

                        PortableWriterImpl writerImpl(stream, &idRslvr, metaMgr, metaHnd.Get());
                        ignite::portable::PortableWriter writer(&writerImpl);

                        int32_t pos = stream->Position();

                        stream->WriteInt8(IGNITE_HDR_FULL);
                        stream->WriteBool(true);
                        stream->WriteInt32(idRslvr.GetTypeId());
                        stream->WriteInt32(type.GetHashCode(obj));

                        stream->Position(pos + IGNITE_FULL_HDR_LEN);

                        type.Write(writer, obj);

                        int32_t len = stream->Position() - pos;

                        stream->WriteInt32(pos + 10, len);
                        stream->WriteInt32(pos + 14, writerImpl.GetRawPosition() - pos);

                        if (metaMgr)
                            metaMgr->SubmitHandler(type.GetTypeName(), idRslvr.GetTypeId(), metaHnd.Get());
                    }
                }

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
                PortableIdResolver* idRslvr;                     
                
                /** Metadata manager. */
                PortableMetadataManager* metaMgr;                   
                
                /** Metadata handler. */
                PortableMetadataHandler* metaHnd;                  

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

                IGNITE_NO_COPY_ASSIGNMENT(PortableWriterImpl)

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

                    stream->WriteInt32(1 + len);
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
                        stream->WriteInt32(5 + (len << lenShift));
                        stream->WriteInt8(hdr);
                        stream->WriteInt32(len);
                        func(stream, val, len);
                    }
                    else
                    {
                        stream->WriteInt32(1);
                        stream->WriteInt8(IGNITE_HDR_NULL);
                    }
                }

                /**
                 * Check raw mode.
                 *
                 * @param expected Expected raw mode of the reader.
                 */
                void CheckRawMode(bool expected);

                /**
                 * Check whether writer is currently operating in single mode.
                 *
                 * @param expected Expected value.
                 */
                void CheckSingleMode(bool expected);

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
                void CheckSession(int32_t expSes);

                /**
                 * Write field ID.
                 *
                 * @param fieldName Field name.
                 * @param fieldTypeId Field type ID.
                 */
                void WriteFieldId(const char* fieldName, int32_t fieldTypeId);

                /**
                 * Write field ID and skip field length.
                 *
                 * @param fieldName Field name.
                 * @param fieldTypeId Field type ID.
                 */
                void WriteFieldIdSkipLength(const char* fieldName, int32_t fieldTypeId);

                /**
                 * Write field ID and length.
                 *
                 * @param fieldName Field name.
                 * @param fieldTypeId Field type ID.
                 * @param len Length.
                 */
                void WriteFieldIdAndLength(const char* fieldName, int32_t fieldTypeId, int32_t len);

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
            void IGNITE_IMPORT_EXPORT PortableWriterImpl::WriteTopObject(const int8_t& obj);

            template<>
            void IGNITE_IMPORT_EXPORT PortableWriterImpl::WriteTopObject(const bool& obj);

            template<>
            void IGNITE_IMPORT_EXPORT PortableWriterImpl::WriteTopObject(const int16_t& obj);

            template<>
            void IGNITE_IMPORT_EXPORT PortableWriterImpl::WriteTopObject(const uint16_t& obj);

            template<>
            void IGNITE_IMPORT_EXPORT PortableWriterImpl::WriteTopObject(const int32_t& obj);

            template<>
            void IGNITE_IMPORT_EXPORT PortableWriterImpl::WriteTopObject(const int64_t& obj);

            template<>
            void IGNITE_IMPORT_EXPORT PortableWriterImpl::WriteTopObject(const float& obj);

            template<>
            void IGNITE_IMPORT_EXPORT PortableWriterImpl::WriteTopObject(const double& obj);

            template<>
            void IGNITE_IMPORT_EXPORT PortableWriterImpl::WriteTopObject(const Guid& obj);

            template<>
            inline void IGNITE_IMPORT_EXPORT PortableWriterImpl::WriteTopObject(const std::string& obj)
            {
                const char* obj0 = obj.c_str();

                int32_t len = static_cast<int32_t>(strlen(obj0));

                stream->WriteInt8(IGNITE_TYPE_STRING);

                PortableUtils::WriteString(stream, obj0, len);
            }
        }
    }
}

#endif