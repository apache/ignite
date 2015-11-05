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

#ifndef _IGNITE_ODBC_DRIVER_CONNECTION
#define _IGNITE_ODBC_DRIVER_CONNECTION

#include <stdint.h>

#include "socket_client.h"

namespace ignite
{
    namespace odbc
    {
        class Connection
        {
            friend class Environment;
        public:
            /**
             * Destructor.
             */
            ~Connection();

            /**
             * Establish connection to ODBC server.
             *
             * @param host Host.
             * @param port Port.
             * @return True on success.
             */
            bool Establish(const std::string& host, uint16_t port);
            
            /**
             * Release established connection.
             *
             * @return True on success.
             */
            bool Release();

        private:
            /**
             * Constructor.
             */
            Connection();

            /** Socket. */
            tcp::SocketClient socket;

            /** State flag. */
            bool connected;
        };
    }
}

#endif