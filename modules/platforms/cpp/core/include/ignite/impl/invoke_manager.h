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

#ifndef _IGNITE_IMPL_INVOKE_MANAGER
#define _IGNITE_IMPL_INVOKE_MANAGER

#include <map>

#include <ignite/common/common.h>
#include <ignite/cache/cache_entry_processor.h>

#include <ignite/impl/binary/binary_reader_impl.h>
#include <ignite/impl/binary/binary_writer_impl.h>
#include <ignite/impl/cache/cache_entry_processor_holder.h>

namespace ignite
{
    namespace impl
    {
        /**
         * Invoke manager.
         * Used to register and invoke cache entry processors.
         */
        class InvokeManager
        {
            typedef common::dynamic::Module Module;
            typedef impl::binary::BinaryReaderImpl BinaryReaderImpl;
            typedef impl::binary::BinaryWriterImpl BinaryWriterImpl;
            typedef bool (JobInvoker)(int64_t, BinaryReaderImpl&, BinaryWriterImpl&);
            typedef void (EntryProcessor)(BinaryReaderImpl&, BinaryWriterImpl&);

        public:
            /**
             * Default constructor.
             */
            InvokeManager() : processors()
            {
                // No-op.
            }

            /**
             * Invoke cache entry processor using provided ID.
             *
             * Deserializes entry and processor itself, invokes processor and
             * serializes processing result using providede reader and writer.
             *
             * @param id Processor ID.
             * @param reader Reader.
             * @param writer Writer.
             * @return True if processor is registered and false otherwise.
             */
            bool InvokeCacheEntryProcessorById(int64_t id, BinaryReaderImpl& reader, BinaryWriterImpl& writer)
            {
                typedef std::map<int64_t, EntryProcessor*> Processors;

                Processors::iterator it = processors.find(id);
                if (it != processors.end())
                {
                    it->second(reader, writer);

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
             * @param proc Processor.
             */
            void RegisterProcessor(int64_t id, EntryProcessor* proc)
            {
                if (processors.find(id) != processors.end())
                    throw IgniteError(IgniteError::IGNITE_ERR_ENTRY_PROCESSOR,
                        "Entry processor with the specified id is already registred.");

                processors[id] = proc;
            }

            template<typename P>
            void RegisterProcessor()
            {
                RegisterProcessor(P::GetJobId(), &P::CacheEntryProcessor::InternalProcess);
            }

        private:
            IGNITE_NO_COPY_ASSIGNMENT(InvokeManager);

            /** Registered cache entry processors. */
            std::map<int64_t, EntryProcessor*> processors;
        };
    }
}

#endif //_IGNITE_IMPL_INVOKE_MANAGER
