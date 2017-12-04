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

#ifndef _IGNITE_ODBC_SSL_SECURE_SOCKET_CLIENT
#define _IGNITE_ODBC_SSL_SECURE_SOCKET_CLIENT

#include <stdint.h>
#include <string>

#include "ignite/odbc/diagnostic/diagnosable.h"

namespace ignite
{
    namespace odbc
    {
        namespace ssl
        {
            /**
             * Secure socket client.
             */
            class SecureSocketClient
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param certPath Certificate file path.
                 * @param keyPath Private key file path.
                 * @param caPath Certificate authority file path.
                 * @param caDirPath Path to directory containing certificate authority files.
                 */
                SecureSocketClient(const std::string& certPath, const std::string& keyPath, const std::string& caPath,
                    const std::string& caDirPath);

                /**
                 * Destructor.
                 */
                ~SecureSocketClient();

                /**
                 * Establish connection with the host.
                 *
                 * @param hostname Host name or address.
                 * @param port TCP port.
                 * @param diag Diagnostics collector to use for error-reporting.
                 * @return @c true on success and @c false on fail.
                 */
                bool Connect(const char* hostname, uint16_t port, diagnostic::Diagnosable& diag);

                /**
                 * Close the connection.
                 */
                void Close();

                /**
                 * Send data using connection.
                 * @param data Data to send.
                 * @param size Number of bytes to send.
                 * @param timeout Timeout.
                 * @return Number of bytes that have been sent on success, 
                 *     WaitResult::TIMEOUT on timeout and -errno on failure.
                 */
                int Send(const int8_t* data, size_t size, int32_t timeout);

                /**
                 * Receive data from established connection.
                 *
                 * @param buffer Pointer to data buffer.
                 * @param size Size of the buffer in bytes.
                 * @param timeout Timeout.
                 * @return Number of bytes that have been sent on success,
                 *     WaitResult::TIMEOUT on timeout and -errno on failure.
                 */
                int Receive(int8_t* buffer, size_t size, int32_t timeout);

                /**
                 * Check if the socket is blocking or not.
                 * @return @c true if the socket is blocking and false otherwise.
                 */
                bool IsBlocking()
                {
                    return true;
                }

            private:
                /**
                 * Make new context instance.
                 *
                 * @param certPath Certificate file path.
                 * @param keyPath Private key file path.
                 * @param caPath Certificate authority file path.
                 * @param caDirPath Path to directory containing certificate authority files.
                 * @param diag Diagnostics collector to use for error-reporting.
                 * @return New context instance on success and null-opinter on fail.
                 */
                static void* MakeContext(const std::string& certPath, const std::string& keyPath,
                    const std::string& caPath, const std::string& caDirPath, diagnostic::Diagnosable& diag);

                /** Certificate file path. */
                std::string certPath;

                /** Private key file path. */
                std::string keyPath;

                /** Certificate authority file path. */
                std::string caPath;

                /** Path to directory containing certificate authority files. */
                std::string caDirPath;

                /** SSL context. */
                void* context;

                /** OpenSSL I/O stream abstraction */
                void* sslBio;
            };
        }
    }
}

#endif //_IGNITE_ODBC_SSL_SECURE_SOCKET_CLIENT