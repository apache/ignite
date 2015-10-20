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

#include "ignite/impl/portable/portable_metadata_updater_impl.h"
#include "ignite/impl/interop/interop_output_stream.h"
#include "ignite/impl/portable/portable_writer_impl.h"
#include "ignite/portable/portable_raw_writer.h"

using namespace ignite::common::concurrent;
using namespace ignite::common::java;
using namespace ignite::impl;
using namespace ignite::impl::interop;
using namespace ignite::portable;

namespace ignite
{    
    namespace impl
    {
        namespace portable
        {
            /** Operation: Clear. */
            const int32_t OP_METADATA = -1;

            PortableMetadataUpdaterImpl::PortableMetadataUpdaterImpl(SharedPointer<IgniteEnvironment> env,
                jobject javaRef) :  env(env), javaRef(javaRef)
            {
                // No-op.
            }

            PortableMetadataUpdaterImpl::~PortableMetadataUpdaterImpl()
            {
                // No-op.
            }

            bool PortableMetadataUpdaterImpl::Update(Snap* snap, IgniteError* err)
            {
                JniErrorInfo jniErr;

                SharedPointer<InteropMemory> mem = env.Get()->AllocateMemory();

                InteropOutputStream out(mem.Get());
                PortableWriterImpl writer(&out, NULL);
                PortableRawWriter rawWriter(&writer);

                // We always pass only one meta at a time in current implementation for simplicity.
                rawWriter.WriteInt32(1);

                rawWriter.WriteInt32(snap->GetTypeId());
                rawWriter.WriteString(snap->GetTypeName());
                rawWriter.WriteString(NULL); // Affinity key is not supported for now.
                
                if (snap->HasFields())
                {
                    std::map<std::string, int32_t>* fields = snap->GetFields();

                    rawWriter.WriteInt32(static_cast<int32_t>(fields->size()));

                    for (std::map<std::string, int32_t>::iterator it = fields->begin(); it != fields->end(); ++it)
                    {
                        rawWriter.WriteString(it->first);
                        rawWriter.WriteInt32(it->second);
                    }
                }
                else
                    rawWriter.WriteInt32(0);

                out.Synchronize();

                long long res = env.Get()->Context()->TargetInStreamOutLong(javaRef, OP_METADATA, mem.Get()->PointerLong(), &jniErr);

                IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                if (jniErr.code == IGNITE_JNI_ERR_SUCCESS)
                    return res == 1;
                else
                    return false;
            }
        }
    }
}