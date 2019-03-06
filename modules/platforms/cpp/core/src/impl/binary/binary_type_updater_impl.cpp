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
                rawWriter.WriteString(0); // Affinity key is not supported for now.
                
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

                long long res = env.Context()->TargetInStreamOutLong(javaRef, Operation::PUT_META, mem.Get()->PointerLong(), &jniErr);

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

                SPSnap res(new Snap(typeName, readTypeId));

                // Skipping affinity key field name.
                rawReader.ReadString();
                
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