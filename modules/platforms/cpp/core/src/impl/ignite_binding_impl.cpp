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

#include <ignite/impl/ignite_environment.h>
#include <ignite/impl/ignite_binding_impl.h>

using namespace ignite::common::concurrent;

namespace ignite
{
    namespace impl
    {
        IgniteBindingImpl::IgniteBindingImpl(IgniteEnvironment &env) :
            env(env),
            callbacks()
        {
            // No-op.
        }

        int64_t IgniteBindingImpl::InvokeCallback(bool& found, int32_t type, int32_t id,
            binary::BinaryReaderImpl& reader, binary::BinaryWriterImpl& writer)
        {
            int64_t key = makeKey(type, id);

            CsLockGuard guard(lock);

            std::map<int64_t, Callback*>::iterator it = callbacks.find(key);

            found = it != callbacks.end();

            if (found)
            {
                Callback* callback = it->second;

                // We have found callback and does not need lock here anymore.
                guard.Reset();

                return callback(reader, writer, env);
            }

            return 0;
        }

        void IgniteBindingImpl::RegisterCallback(int32_t type, int32_t id, Callback* proc, IgniteError& err)
        {
            int64_t key = makeKey(type, id);

            CsLockGuard guard(lock);

            bool inserted = callbacks.insert(std::make_pair(key, proc)).second;

            guard.Reset();

            if (!inserted)
            {
                std::stringstream builder;

                builder << "Trying to register multiple PRC callbacks with the same ID. [type="
                        << type << ", id=" << id << ']';

                err = IgniteError(IgniteError::IGNITE_ERR_ENTRY_PROCESSOR, builder.str().c_str());
            }
        }

        void IgniteBindingImpl::RegisterCallback(int32_t type, int32_t id, Callback* callback)
        {
            IgniteError err;

            RegisterCallback(type, id, callback, err);

            IgniteError::ThrowIfNeeded(err);
        }
    }
}