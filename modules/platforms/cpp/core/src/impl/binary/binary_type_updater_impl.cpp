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

#include <iterator>

#include "ignite/impl/binary/binary_type_updater_impl.h"
#include "ignite/impl/interop/interop_output_stream.h"
#include "ignite/impl/binary/binary_writer_impl.h"
#include "ignite/binary/binary_writer.h"
#include "ignite/binary/binary_reader.h"

using namespace ignite::common::concurrent;
using namespace ignite::jni::java;
using namespace ignite::java;
using namespace ignite::impl;
using namespace ignite::impl::interop;
using namespace ignite::binary;

namespace ignite
{    
    namespace impl
    {
        namespace binary
        {
            struct Operation
            {
                enum Type
                {
                    /** Operation: metadata get. */
                    GET_META = 1,

                    /** Operation: metadata update. */
                    PUT_META = 3
                };
            };

            BinaryTypeUpdaterImpl::BinaryTypeUpdaterImpl(IgniteEnvironment& env, jobject javaRef) :
                env(env),
                javaRef(javaRef)
            {
                // No-op.
            }

            BinaryTypeUpdaterImpl::~BinaryTypeUpdaterImpl()
            {
                JniContext::Release(javaRef);
            }

            bool BinaryTypeUpdaterImpl::Update(const Snap& snap, IgniteError& err)
            {
                JniErrorInfo jniErr;

                SharedPointer<InteropMemory> mem = env.AllocateMemory();

                InteropOutputStream out(mem.Get());
                BinaryWriterImpl writer(&out, 0);
                BinaryRawWriter rawWriter(&writer);

                // We always pass only one meta at a time in current implementation for simplicity.
                rawWriter.WriteInt32(1);

                rawWriter.WriteInt32(snap.GetTypeId());
                rawWriter.WriteString(snap.GetTypeName());

                const std::string& affFieldName = snap.GetAffinityFieldName();

                if (affFieldName.empty())
                    rawWriter.WriteNull();
                else
                    rawWriter.WriteString(affFieldName);

                if (snap.HasFields())
                {
                    const Snap::FieldMap& fields = snap.GetFieldMap();

                    rawWriter.WriteInt32(static_cast<int32_t>(fields.size()));

                    for (Snap::FieldMap::const_iterator it = fields.begin(); it != fields.end(); ++it)
                    {
                        const BinaryFieldMeta& fieldMeta = it->second;

                        rawWriter.WriteString(it->first);
                        fieldMeta.Write(rawWriter);
                    }
                }
                else
                    rawWriter.WriteInt32(0);

                rawWriter.WriteBool(false); // Enums are not supported for now.

                rawWriter.WriteInt32(0); // Schema size. Compact schema footer is not yet supported.

                out.Synchronize();

                int64_t res = env.Context()->TargetInStreamOutLong(javaRef, Operation::PUT_META, mem.Get()->PointerLong(), &jniErr);

                IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                return jniErr.code == IGNITE_JNI_ERR_SUCCESS && res == 1;
            }

            SPSnap BinaryTypeUpdaterImpl::GetMeta(int32_t typeId, IgniteError& err)
            {
                JniErrorInfo jniErr;

                SharedPointer<InteropMemory> outMem = env.AllocateMemory();
                SharedPointer<InteropMemory> inMem = env.AllocateMemory();

                InteropOutputStream out(outMem.Get());
                BinaryWriterImpl writer(&out, 0);

                writer.WriteInt32(typeId);

                out.Synchronize();

                env.Context()->TargetInStreamOutStream(javaRef, Operation::GET_META,
                    outMem.Get()->PointerLong(), inMem.Get()->PointerLong(), &jniErr);

                IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                if (err.GetCode() != IgniteError::IGNITE_SUCCESS)
                    return SPSnap();

                InteropInputStream in(inMem.Get());
                BinaryReaderImpl reader(&in);
                BinaryRawReader rawReader(&reader);

                bool found = rawReader.ReadBool();

                if (!found)
                    return SPSnap();

                int32_t readTypeId = rawReader.ReadInt32();

                assert(typeId == readTypeId);

                std::string typeName = rawReader.ReadString();
                std::string affFieldName = rawReader.ReadString();

                SPSnap res(new Snap(typeName, affFieldName, readTypeId));
                
                int32_t fieldsNum = rawReader.ReadInt32();

                for (int32_t i = 0; i < fieldsNum; ++i)
                {
                    std::string fieldName = rawReader.ReadString();
                    BinaryFieldMeta fieldMeta;
                    fieldMeta.Read(rawReader);

                    res.Get()->AddField(fieldMeta.GetFieldId(), fieldName, fieldMeta.GetTypeId());
                }

                // Skipping isEnum info.
                rawReader.ReadBool();

                return res;
            }
        }
    }
}