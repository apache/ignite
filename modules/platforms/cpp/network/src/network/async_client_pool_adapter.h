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

#ifndef _IGNITE_NETWORK_ASYNC_CLIENT_POOL_ADAPTER
#define _IGNITE_NETWORK_ASYNC_CLIENT_POOL_ADAPTER

#include <ignite/network/async_client_pool.h>

namespace ignite
{
    namespace network
    {
        /**
         * Asynchronous client pool adapter.
         */
        class AsyncClientPoolAdapter : public AsyncClientPool
        {
        public:
            /**
             * Constructor.
             *
             * @param filters Filters.
             * @param pool Client pool.
             */
            AsyncClientPoolAdapter(const std::vector<SP_DataFilter>& filters, const SP_AsyncClientPool& pool);

            /**
             * Destructor.
             */
            virtual ~AsyncClientPoolAdapter()
            {
                // No-op.
            }

            /**
             * Start internal thread that establishes connections to provided addresses and asynchronously sends and
             * receives messages from them. Function returns either when thread is started and first connection is
             * established or failure happened.
             *
             * @param addrs Addresses to connect to.
             * @param connLimit Connection upper limit. Zero means limit is disabled.
             *
             * @throw IgniteError on error.
             */
            virtual void Start(const std::vector<TcpRange>& addrs, uint32_t connLimit);

            /**
             * Close all established connections and stops handling threads.
             */
            virtual void Stop();

            /**
             * Set handler.
             *
             * @param handler Handler to set.
             */
            virtual void SetHandler(AsyncHandler *handler);

            /**
             * Send data to specific established connection.
             *
             * @param id Client ID.
             * @param data Data to be sent.
             * @return @c true if connection is present and @c false otherwise.
             *
             * @throw IgniteError on error.
             */
            virtual bool Send(uint64_t id, const DataBuffer& data);

            /**
             * Closes specified connection if it's established. Connection to the specified address is planned for
             * re-connect. Error is reported to handler.
             *
             * @param id Client ID.
             */
            virtual void Close(uint64_t id, const IgniteError* err);

        private:
            /** Filters. */
            std::vector<SP_DataFilter> filters;

            /** Underlying pool. */
            SP_AsyncClientPool pool;

            /** Lower level data sink. */
            DataSink* sink;

            /** Upper level event handler. */
            AsyncHandler* handler;
        };

        // Type alias
        typedef common::concurrent::SharedPointer<AsyncClientPoolAdapter> SP_AsyncClientPoolAdapter;
    }
}

#endif //_IGNITE_NETWORK_ASYNC_CLIENT_POOL_ADAPTER