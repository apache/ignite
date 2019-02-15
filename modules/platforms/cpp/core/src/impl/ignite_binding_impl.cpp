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