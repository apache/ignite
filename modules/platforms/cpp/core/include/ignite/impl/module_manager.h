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

#ifndef _IGNITE_CALLABLE
#define _IGNITE_CALLABLE

#include <vector>

#include <ignite/common/common.h>
#include <ignite/common/dynamic_load_os.h>
#include <ignite/impl/binary/binary_reader_impl.h>
#include <ignite/impl/binary/binary_writer_impl.h>
#include "ignite/impl/cache/cache_entry_processor_holder.h"


#define IGNITE_CACHE_ENTRY_PROCESSOR_INVOKER_NAME "ignite_impl_InvokeCacheProcessor"

#define IGNITE_CACHE_ENTRY_PROCESSOR_LIST_BEGIN \
    extern "C" IGNITE_IMPORT_EXPORT \
    bool ignite_impl_InvokeCacheProcessor(int64_t jobTypeId, \
                                         ignite::impl::binary::BinaryReaderImpl& reader, \
                                         ignite::impl::binary::BinaryWriterImpl& writer) \
    {

#define IGNITE_CACHE_ENTRY_PROCESSOR_LIST_END \
        return false; \
    }

#define IGNITE_CACHE_ENTRY_PROCESSOR_DECLARE(ProcessorType, KeyType, ValueType, ResultType, ArgumentType) \
    if (jobTypeId == ProcessorType::GetJobId()) \
    { \
        using ignite::impl::CallCacheEntryProcessor; \
        CallCacheEntryProcessor<ProcessorType, KeyType, ValueType, ResultType, ArgumentType>(reader, writer); \
        return true; \
    }

namespace ignite
{
    namespace impl
    {
        template<typename P, typename K, typename V, typename R, typename A>
        void CallCacheEntryProcessor(impl::binary::BinaryReaderImpl& reader, impl::binary::BinaryWriterImpl& writer)
        {
            typedef cache::CacheEntryProcessorHolder<P, A> ProcessorHolder;

            K key = reader.ReadObject<K>();
            V value;

            bool exists = reader.TryReadObject<V>(value);

            bool isLocal = reader.ReadBool();

            // For C++ client we currently always writing processor as an
            // object, no matter if it's local or not.
            assert(!isLocal);

            cache::MutableCacheEntryState entryState;

            ProcessorHolder procHolder = reader.ReadObject<ProcessorHolder>();

            R res = procHolder.Process<R, K, V>(key, value, exists, entryState);

            writer.WriteInt8(static_cast<int8_t>(entryState));
            writer.WriteTopObject(value);
            writer.WriteTopObject(res);
        }

        /**
         * Module manager.
         * Provides methods to manipulate loadable modules.
         */
        class ModuleManager
        {
            typedef ignite::common::dynamic::Module Module;
            typedef ignite::impl::binary::BinaryReaderImpl BinaryReaderImpl;
            typedef ignite::impl::binary::BinaryWriterImpl BinaryWriterImpl;
            typedef bool (JobInvoker)(int64_t, BinaryReaderImpl&, BinaryWriterImpl&);

        public:
            static ModuleManager& GetInstance()
            {
                static ModuleManager self;

                return self;
            }

            bool InvokeJobById(int64_t id, BinaryReaderImpl& reader, BinaryWriterImpl& writer)
            {
                typedef std::vector<JobInvoker*> Invokers;

                for (Invokers::iterator i = jobInvokers.begin(); i != jobInvokers.end(); ++i)
                {
                    JobInvoker* invoker = *i;

                    if (invoker(id, reader, writer))
                        return true;
                }

                return false;
            }

            void RegisterModule(Module& module)
            {
                loadedModules.push_back(module);

                JobInvoker *invoker = GetRemoteJobInvoker(module);

                if (invoker)
                    jobInvokers.push_back(invoker);
            }

        private:
            JobInvoker* GetRemoteJobInvoker(Module& module)
            {
                return reinterpret_cast<JobInvoker*>(module.FindSymbol(IGNITE_CACHE_ENTRY_PROCESSOR_INVOKER_NAME));
            }

            ModuleManager() : loadedModules(), jobInvokers()
            {
                Module current = common::dynamic::GetCurrent();

                RegisterModule(current);
            }

            IGNITE_NO_COPY_ASSIGNMENT(ModuleManager);

            std::vector<Module> loadedModules;

            std::vector<JobInvoker*> jobInvokers;
        };
    }
}

#endif
