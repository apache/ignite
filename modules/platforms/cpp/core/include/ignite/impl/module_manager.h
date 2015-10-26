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
#include <ignite/impl/portable/portable_reader_impl.h>
#include <ignite/impl/portable/portable_writer_impl.h>
#include "ignite/impl/cache/cache_entry_processor_holder.h"


#define IGNITE_REMOTE_JOB_INVOKER_NAME "ignite_impl_InvokeRemoteJob"

#define IGNITE_REMOTE_JOB_LIST_BEGIN \
    extern "C" IGNITE_IMPORT_EXPORT \
    bool ignite_impl_InvokeRemoteJob(const std::string& jobTypeId, \
                                     ignite::impl::portable::PortableReaderImpl& reader, \
                                     ignite::impl::portable::PortableWriterImpl& writer) \
    {

#define IGNITE_REMOTE_JOB_LIST_END \
        return false; \
    }

#define IGNITE_REMOTE_CACHE_ENTRY_PROCESSOR_DECLARE(ProcessorType, KeyType, ValueType, ResultType, ArgumentType) \
    if (jobTypeId == #ProcessorType) \
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
        void CallCacheEntryProcessor(impl::portable::PortableReaderImpl& reader, impl::portable::PortableWriterImpl& writer)
        {
            typedef cache::CacheEntryProcessorHolder<P, A> ProcessorHolder;

            K key = reader.ReadObject<K>();
            V value = reader.ReadObject<V>();
            bool isLocal = reader.ReadBool();
            
            cache::MutableCacheEntryState entryState;

            ignite::portable::PortableType<V> pvalue;

            ProcessorHolder procHolder = reader.ReadObject<ProcessorHolder>();

            R res = procHolder.Process<R, K, V>(key, value, !pvalue.IsNull(value), entryState);

            writer.WriteInt8(static_cast<int8_t>(entryState));
            writer.WriteTopObject(value);
            writer.WriteTopObject(res);
        }

        typedef bool (JobInvoker)(const std::string&, 
                                  ignite::impl::portable::PortableReaderImpl&,
                                  ignite::impl::portable::PortableWriterImpl&);

        class ModuleManager
        {
        public:
            static ModuleManager& GetInstance()
            {
                static ModuleManager self;

                return self;
            }

            bool InvokeJobById(const std::string& id,
                               impl::portable::PortableReaderImpl& reader,
                               impl::portable::PortableWriterImpl& writer)
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

        private:

            JobInvoker* GetRemoteJobInvoker(common::dynamic::Module& module)
            {
                return reinterpret_cast<JobInvoker*>(module.FindSymbol(IGNITE_REMOTE_JOB_INVOKER_NAME));
            }

            void RegisterModule(common::dynamic::Module& module)
            {
                loadedModules.push_back(module);

                JobInvoker *invoker = GetRemoteJobInvoker(module);

                if (invoker)
                {
                    jobInvokers.push_back(invoker);
                }
            }

            ModuleManager() : loadedModules(), jobInvokers()
            {
                common::dynamic::Module current = common::dynamic::GetCurrent();

                RegisterModule(current);
            }

            IGNITE_NO_COPY_ASSIGNMENT(ModuleManager);

            std::vector<common::dynamic::Module> loadedModules;

            std::vector<JobInvoker*> jobInvokers;
        };
    }
}

#endif
