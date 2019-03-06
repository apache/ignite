/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

#ifndef _IGNITE_NETWORK_SSL_SECURE_SOCKET_CLIENT
#define _IGNITE_NETWORK_SSL_SECURE_SOCKET_CLIENT

#include <stdint.h>
#include <string>

#include <ignite/network/socket_client.h>

namespace ignite
{
    namespace network
    {
        namespace ssl
        {
            /**
             * Secure socket client.
             */
            class SecureSocketClient : public SocketClient
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param certPath Certificate file path.
                 * @param keyPath Private key file path.
                 * @param caPath Certificate authority file path.
                 */
                SecureSocketClient(const std::string& certPath, const std::string& keyPath, const std::string& caPath);

                /**
                 * Destructor.
                 */
                virtual ~SecureSocketClient();

                /**
                 * Establish connection with the host.
                 *
                 * @param hostname Host name or address.
                 * @param port TCP port.
                 * @param timeout Timeout in seconds.
                 * @return @c true on success and @c false on timeout.
                 */
                virtual bool Connect(const char* hostname, uint16_t port, int32_t timeout);

                /**
                 * Close the connection.
                 */
                virtual void Close();

                /**
                 * Send data using connection.
                 * @param data Data to send.
                 * @param size Number of bytes to send.
                 * @param timeout Timeout in seconds.
                 * @return Number of bytes that have been sent on success,
                 *     WaitResult::TIMEOUT on timeout and -errno on failure.
                 */
                virtual int Send(const int8_t* data, size_t size, int32_t timeout);

                /**
                 * Receive data from established connection.
                 *
                 * @param buffer Pointer to data buffer.
                 * @param size Size of the buffer in bytes.
                 * @param timeout Timeout in seconds.
                 * @return Number of bytes that have been received on success,
                 *     WaitResult::TIMEOUT on timeout and -errno on failure.
                 */
                virtual int Receive(int8_t* buffer, size_t size, int32_t timeout);

                /**
                 * Check if the socket is blocking or not.
                 * @return @c true if the socket is blocking and false otherwise.
                 */
                virtual bool IsBlocking() const;

            private:
                /**
                 * Close the connection.
                 * Internal call.
                 */
                void CloseInteral();

                /**
                 * Throw SSL-related error.
                 *
                 * @param err Error message.
                 */
                static void ThrowSecureError(const std::string& err);

                /**
                 * Wait on the socket for any event for specified time.
                 * This function uses poll to achive timeout functionality
                 * for every separate socket operation.
                 *
                 * @param ssl SSL instance.
                 * @param timeout Timeout in seconds.
                 * @param rd Wait for read if @c true, or for write if @c false.
                 * @return -errno on error, WaitResult::TIMEOUT on timeout and
                 *     WaitResult::SUCCESS on success.
                 */
                static int WaitOnSocket(void* ssl, int32_t timeout, bool rd);

                /**
                 * Make new context instance.
                 *
                 * @param certPath Certificate file path.
                 * @param keyPath Private key file path.
                 * @param caPath Certificate authority file path.
                 * @return New context instance on success and null-pointer on fail.
                 */
                static void* MakeContext(const std::string& certPath, const std::string& keyPath,
                    const std::string& caPath);

                /**
                 * Make new SSL instance.
                 *
                 * @param context SSL context.
                 * @param hostname Host name or address.
                 * @param port TCP port.
                 * @param blocking Indicates if the resulted SSL is blocking or not.
                 * @return New SSL instance on success and null-pointer on fail.
                 */
                static void* MakeSsl(void* context, const char* hostname, uint16_t port, bool& blocking);

                /**
                 * Complete async connect.
                 *
                 * @param ssl SSL instance.
                 * @param timeout Timeout in seconds.
                 * @return @c true on success and @c false on timeout.
                 */
                static bool CompleteConnectInternal(void* ssl, int timeout);

                /**
                 * Get SSL error.
                 *
                 * @param ssl SSL instance.
                 * @param ret Return value of the pervious operation.
                 * @return Error string.
                 */
                static std::string GetSslError(void* ssl, int ret);

                /**
                 * Check if a actual error occured.
                 *
                 * @param err SSL error code.
                 * @return @true if a actual error occured
                 */
                static bool IsActualError(int err);

                /** Certificate file path. */
                std::string certPath;

                /** Private key file path. */
                std::string keyPath;

                /** Certificate authority file path. */
                std::string caPath;

                /** SSL context. */
                void* context;

                /** OpenSSL instance */
                void* ssl;

                /** Blocking flag. */
                bool blocking;
            };
        }
    }
}

#endif //_IGNITE_NETWORK_SSL_SECURE_SOCKET_CLIENT
