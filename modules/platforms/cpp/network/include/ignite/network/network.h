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

#ifndef _IGNITE_NETWORK_SSL_SSL_API
#define _IGNITE_NETWORK_SSL_SSL_API

#include <string>

#include <ignite/common/common.h>
#include <ignite/network/socket_client.h>

namespace ignite
{
    namespace network
    {
        namespace ssl
        {
            /**
             * Ensure that SSL library is loaded.
             *
             * Called implicitly when SecureSocket is created, so there is no
             * need to call this function explicitly.
             *
             * @throw IgniteError if it is not possible to load SSL library.
             */
            IGNITE_IMPORT_EXPORT void EnsureSslLoaded();

            /**
             * Make basic TCP socket.
             */
            IGNITE_IMPORT_EXPORT SocketClient* MakeTcpSocketClient();

            /**
             * Make secure socket for SSL/TLS connection.
             *
             * @param certPath Certificate file path.
             * @param keyPath Private key file path.
             * @param caPath Certificate authority file path.
             *
             * @throw IgniteError if it is not possible to load SSL library.
             */
            IGNITE_IMPORT_EXPORT SocketClient* MakeSecureSocketClient(const std::string& certPath,
                const std::string& keyPath, const std::string& caPath);
        }
    }
}

#endif //_IGNITE_NETWORK_SSL_SSL_API