/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "network/sockets.h"

#include <ignite/network/network.h>

#include "network/ssl/ssl_gateway.h"
#include "network/ssl/secure_socket_client.h"
#include "network/tcp_socket_client.h"

namespace ignite
{
    namespace network
    {
        namespace ssl
        {
            void EnsureSslLoaded()
            {
                SslGateway::GetInstance().LoadAll();
            }

            SocketClient* MakeTcpSocketClient()
            {
                return new TcpSocketClient;
            }

            SocketClient* MakeSecureSocketClient(const std::string& certPath,
                const std::string& keyPath, const std::string& caPath)
            {
                EnsureSslLoaded();

                return new SecureSocketClient(certPath, keyPath, caPath);
            }
        }
    }
}
