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

#ifndef _IGNITE_IMPL_THIN_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_HANDLE_CLIENT
#define _IGNITE_IMPL_THIN_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_HANDLE_CLIENT

#include <ignite/common/common.h>
#include <ignite/common/concurrent.h>

#include "impl/data_channel.h"
#include "impl/message.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace cache
            {
                namespace query
                {
                    namespace continuous
                    {
                        /**
                         * Continuous query handle client implementation.
                         *
                         * Once destructed, continuous query is stopped and event listener stops getting event.
                         */
                        class ContinuousQueryHandleClientImpl
                        {
                        public:
                            /**
                             * Default constructor.
                             *
                             * @param channel Channel.
                             * @param queryId Query ID.
                             */
                            ContinuousQueryHandleClientImpl(SP_DataChannel channel, int64_t queryId) :
                                channel(channel),
                                queryId(queryId)
                            {
                                // No-op.
                            }

                            /**
                             * Destructor.
                             */
                            virtual ~ContinuousQueryHandleClientImpl()
                            {
                                assert(channel.IsValid());

                                DataChannel& channel0 = *channel.Get();

                                channel0.DeregisterNotificationHandler(queryId);
                                channel0.CloseResource(queryId);
                            }

                        private:
                            /** Data channel. */
                            SP_DataChannel channel;

                            /** Query ID. */
                            int64_t queryId;
                        };

                        /** Shared pointer to ContinuousQueryHandleClientImpl. */
                        typedef common::concurrent::SharedPointer<ContinuousQueryHandleClientImpl> SP_ContinuousQueryHandleClientImpl;
                    }
                }
            }
        }
    }
}

#endif //_IGNITE_IMPL_THIN_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_HANDLE_CLIENT