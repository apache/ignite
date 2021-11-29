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

#ifndef _IGNITE_NETWORK_ASYNC_HANDLER
#define _IGNITE_NETWORK_ASYNC_HANDLER

#include <stdint.h>

#include <vector>

#include <ignite/future.h>
#include <ignite/ignite_error.h>
#include <ignite/network/end_point.h>

namespace ignite
{
    namespace network
    {
        class AsyncHandler
        {
        public:
            /**
             * Destructor.
             */
            virtual ~AsyncHandler()
            {
                // No-op.
            }

            /**
             * Callback that called on successful connection establishment.
             *
             * @param addr Address of the new connection.
             * @param id Connection ID.
             */
            virtual void OnConnectionSuccess(const EndPoint& addr, uint64_t id) = 0;

            /**
             * Callback that called on error during connection establishment.
             *
             * @param addr Connection address.
             * @param err Error.
             */
            virtual void OnConnectionError(const EndPoint& addr, const IgniteError& err) = 0;

            /**
             * Callback that called when new message is received.
             *
             * @param id Async client ID.
             * @param msg Received message.
             */
            virtual void OnMessageReceived(uint64_t id, impl::interop::SP_InteropMemory msg) = 0;

            /**
             * Callback that called on error during connection establishment.
             *
             * @param id Async client ID.
             * @param err Error.
             */
            virtual void OnConnectionBroken(uint64_t id, const IgniteError& err) = 0;
        };
    }
}

#endif //_IGNITE_NETWORK_ASYNC_HANDLER