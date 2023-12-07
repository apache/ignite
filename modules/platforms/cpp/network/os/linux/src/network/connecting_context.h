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

#ifndef _IGNITE_NETWORK_CONNECTING_CONTEXT
#define _IGNITE_NETWORK_CONNECTING_CONTEXT

#include <netdb.h>

#include <stdint.h>
#include <memory>

#include <ignite/network/end_point.h>
#include <ignite/network/tcp_range.h>

#include "network/linux_async_client.h"

namespace ignite
{
    namespace network
    {
        /**
         * Connecting context.
         */
        class ConnectingContext
        {
        public:
            /**
             * Constructor.
             */
            ConnectingContext(const TcpRange& range);

            /**
             * Destructor.
             */
            ~ConnectingContext();

            /**
             * Reset connection context to it's initial state.
             */
            void Reset();

            /**
             * Next address in range.
             *
             * @return Next addrinfo for connection.
             */
            addrinfo* Next();

            /**
             * Get lastaddress.
             *
             * @return Address.
             */
            EndPoint GetAddress() const;

            /**
             * Make client.
             *
             * @param fd Socket file descriptor.
             * @return Client instance from current internal state.
             */
            SP_LinuxAsyncClient ToClient(int fd);

        private:
            IGNITE_NO_COPY_ASSIGNMENT(ConnectingContext);

            /** Range. */
            const TcpRange range;

            /** Next port. */
            uint16_t nextPort;

            /** Current addrinfo. */
            addrinfo* info;

            /** Addrinfo which is currently used for connection */
            addrinfo* currentInfo;
        };
    }
}

#endif //_IGNITE_NETWORK_CONNECTING_CONTEXT
