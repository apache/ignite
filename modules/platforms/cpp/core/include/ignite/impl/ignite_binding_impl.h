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

#ifndef _IGNITE_IMPL_IGNITE_BINDING_IMPL
#define _IGNITE_IMPL_IGNITE_BINDING_IMPL

#include <map>

#include <ignite/common/common.h>

#include <ignite/impl/binary/binary_reader_impl.h>
#include <ignite/impl/binary/binary_writer_impl.h>

namespace ignite
{
    namespace impl
    {
        /**
         * Ignite binding implementation.
         *
         * Used to register and invoke callbacks.
         */
        class IgniteBindingImpl
        {
            typedef void (Callback)(binary::BinaryReaderImpl&, binary::BinaryWriterImpl&);

        public:
            /**
             * Default constructor.
             */
            IgniteBindingImpl() : callbacks()
            {
                // No-op.
            }

            /**
             * Invoke callback using provided ID.
             *
             * Deserializes data and callback itself, invokes callback and
             * serializes processing result using providede reader and writer.
             *
             * @param id Processor ID.
             * @param reader Reader.
             * @param writer Writer.
             * @return True if callback is registered and false otherwise.
             */
            bool InvokeCallbackById(int64_t id, binary::BinaryReaderImpl& reader, binary::BinaryWriterImpl& writer)
            {
                common::concurrent::CsLockGuard guard(lock);

                std::map<int64_t, Callback*>::iterator it = callbacks.find(id);

                if (it != callbacks.end())
                {
                    Callback* callback = it->second;

                    // We have found callback and does not need lock here anymore.
                    guard.Reset();

                    callback(reader, writer);

                    return true;
                }

                return false;
            }

            /**
             * Register cache entry processor and associate it with provided ID.
             *
             * @throw IgniteError another processor is already associated with
             *     the given ID.
             *
             * @param id Identifier for processor to be associated with.
             * @param proc Callback.
             */
            void RegisterCallback(int64_t id, Callback* proc, IgniteError& err)
            {
                common::concurrent::CsLockGuard guard(lock);

                bool inserted = callbacks.insert(std::make_pair(id, proc)).second;

                guard.Reset();

                if (!inserted)
                {
                    std::stringstream builder;

                    builder << "Trying to register multiple PRC callbacks with the same ID. [id=" << id << ']';

                    err = IgniteError(IgniteError::IGNITE_ERR_ENTRY_PROCESSOR, builder.str().c_str());
                }
            }

        private:
            IGNITE_NO_COPY_ASSIGNMENT(IgniteBindingImpl);

            /** Registered callbacks. */
            std::map<int64_t, Callback*> callbacks;

            /** Callback lock. */
            common::concurrent::CriticalSection lock;
        };
    }
}

#endif //_IGNITE_IMPL_IGNITE_BINDING_IMPL
