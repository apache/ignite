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

#ifndef _IGNITE_IMPL_BINDINGS
#define _IGNITE_IMPL_BINDINGS

#include <stdint.h>

#include <ignite/impl/binary/binary_reader_impl.h>
#include <ignite/impl/ignite_environment.h>
#include <ignite/impl/cache/query/continuous/continuous_query_impl.h>
#include <ignite/impl/cache/cache_entry_processor_holder.h>
#include <ignite/impl/compute/compute_task_holder.h>

namespace ignite
{
    namespace impl
    {
        namespace binding
        {
            /**
             * Binding for filter creation.
             * 
             * @tparam F The filter which inherits from CacheEntryEventFilter.
             *
             * @param reader Reader.
             * @param env Environment.
             * @return Handle for the filter.
             */
            template<typename F>
            int64_t FilterCreate(binary::BinaryReaderImpl& reader, binary::BinaryWriterImpl&, IgniteEnvironment& env)
            {
                using namespace common::concurrent;
                using namespace cache::query::continuous;

                F filter = reader.ReadObject<F>();

                SharedPointer<ContinuousQueryImplBase> qry(new RemoteFilterHolder(MakeReferenceFromCopy(filter)));

                return env.GetHandleRegistry().Allocate(qry);
            }

            /**
             * Process input streaming data to produce output streaming data.
             *
             * Deserializes cache entry and processor using provided reader, invokes
             * cache entry processor, gets result and serializes it using provided
             * writer.
             *
             * @param reader Reader.
             * @param writer Writer.
             */
            template<typename P, typename K, typename V, typename R, typename A>
            int64_t ListenerApply(binary::BinaryReaderImpl& reader, binary::BinaryWriterImpl& writer, IgniteEnvironment&)
            {
                typedef cache::CacheEntryProcessorHolder<P, A> ProcessorHolder;

                ProcessorHolder procHolder = reader.ReadObject<ProcessorHolder>();

                K key = reader.ReadObject<K>();

                V value;
                bool exists = reader.TryReadObject<V>(value);

                cache::MutableCacheEntryState::Type entryState;

                R res = procHolder.template Process<R, K, V>(key, value, exists, entryState);

                writer.WriteInt8(static_cast<int8_t>(entryState));

                if (entryState == cache::MutableCacheEntryState::VALUE_SET)
                    writer.WriteTopObject(value);

                writer.WriteTopObject(res);

                return 0;
            }

            /**
             * Binding for compute job creation.
             *
             * @tparam F The job type.
             * @tparam R The job return type.
             *
             * @param reader Reader.
             * @param env Environment.
             * @return Handle for the job.
             */
            template<typename F, typename R>
            int64_t ComputeJobCreate(binary::BinaryReaderImpl& reader, binary::BinaryWriterImpl&, IgniteEnvironment& env)
            {
                using namespace common::concurrent;
                using namespace compute;

                F job = reader.ReadObject<F>();

                SharedPointer<ComputeJobHolder> jobPtr(new ComputeJobHolderImpl<F, R>(job));

                return env.GetHandleRegistry().Allocate(jobPtr);
            }
        }
    }
}

#endif //_IGNITE_IMPL_BINDINGS
