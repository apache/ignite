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

#include "network/sockets.h"

#include <sstream>
#include <cassert>

#include <ignite/common/utils.h>
#include <ignite/network/utils.h>

#include "network/ssl/secure_socket_client.h"
#include "network/ssl/secure_utils.h"
#include "network/ssl/ssl_gateway.h"

using namespace ignite::network::ssl;

void FreeBio(BIO* bio)
{
    using namespace ignite::network::ssl;

    SslGateway &sslGateway = SslGateway::GetInstance();

    assert(sslGateway.Loaded());

    sslGateway.BIO_free_all_(bio);
}

namespace ignite
{
    namespace network
    {
        namespace ssl
        {
            SecureSocketClient::SecureSocketClient(const SecureConfiguration& cfg):
                cfg(cfg),
                context(0),
                ssl(0),
                blocking(true)
            {
                // No-op.
            }

            SecureSocketClient::~SecureSocketClient()
            {
                CloseInternal();

                if (context)
                    FreeContext(reinterpret_cast<SSL_CTX*>(context));
            }

            bool SecureSocketClient::Connect(const char* hostname, uint16_t port, int32_t timeout)
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                assert(sslGateway.Loaded());

                CloseInternal();

                if (!context)
                {
                    context = MakeContext(cfg);

                    if (!context)
                        ThrowLastSecureError("Can not create SSL context", "Aborting connect");
                }

                ssl = MakeSsl(context, hostname, port, blocking);

                assert(ssl != 0);

                common::MethodGuard<SecureSocketClient> guard(this, &SecureSocketClient::CloseInternal);

                SSL* ssl0 = reinterpret_cast<SSL*>(ssl);

                int res = sslGateway.SSL_set_tlsext_host_name_(ssl0, hostname);

                if (res != SSL_OPERATION_SUCCESS)
                    ThrowLastSecureError("Can not set host name for secure connection");

                sslGateway.SSL_set_connect_state_(ssl0);

                bool connected = CompleteConnectInternal(ssl0, timeout);

                if (!connected)
                    return false;

                // Verify a server certificate was presented during the negotiation
                X509* cert = sslGateway.SSL_get_peer_certificate_(ssl0);
                if (cert)
                    sslGateway.X509_free_(cert);
                else
                    ThrowLastSecureError("Remote host did not provide certificate");

                // Verify the result of chain verification.
                // Verification performed according to RFC 4158.
                res = sslGateway.SSL_get_verify_result_(ssl0);
                if (X509_V_OK != res)
                    ThrowLastSecureError("Certificate chain verification failed");

                res = WaitOnSocket(ssl, timeout, false);

                if (res == WaitResult::TIMEOUT)
                    return false;

                if (res != WaitResult::SUCCESS)
                    ThrowLastSecureError("Error while establishing secure connection");

                guard.Release();

                return true;
            }

            void SecureSocketClient::Close()
            {
                CloseInternal();
            }

            int SecureSocketClient::Send(const int8_t* data, size_t size, int32_t)
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                assert(sslGateway.Loaded());

                if (!ssl)
                    utils::ThrowNetworkError("Trying to send data using closed connection");

                SSL* ssl0 = reinterpret_cast<SSL*>(ssl);

                return sslGateway.SSL_write_(ssl0, data, static_cast<int>(size));
            }

            int SecureSocketClient::Receive(int8_t* buffer, size_t size, int32_t timeout)
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                assert(sslGateway.Loaded());

                if (!ssl)
                    utils::ThrowNetworkError("Trying to receive data using closed connection");

                SSL* ssl0 = reinterpret_cast<SSL*>(ssl);

                int res = 0;

                if (!blocking && sslGateway.SSL_pending_(ssl0) == 0)
                {
                    res = WaitOnSocket(ssl, timeout, true);

                    if (res < 0 || res == WaitResult::TIMEOUT)
                        return res;
                }

                do {
                    res = sslGateway.SSL_read_(ssl0, buffer, static_cast<int>(size));

                    if (res <= 0)
                    {
                        int err = sslGateway.SSL_get_error_(ssl0, res);
                        if (IsActualError(err))
                            return -err;

                        int want = sslGateway.SSL_want_(ssl0);

                        int waitRes = WaitOnSocket(ssl, timeout, want == SSL_READING);

                        if (waitRes < 0 || waitRes == WaitResult::TIMEOUT)
                            return waitRes;
                    }
                } while (res <= 0);

                return res;
            }

            bool SecureSocketClient::IsBlocking() const
            {
                return blocking;
            }

            void* SecureSocketClient::MakeSsl(void* context, const char* hostname, uint16_t port, bool& blocking)
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                assert(sslGateway.Loaded());

                BIO* bio = sslGateway.BIO_new_ssl_connect_(reinterpret_cast<SSL_CTX*>(context));
                if (!bio)
                    ThrowLastSecureError("Can not create SSL connection");

                common::DeinitGuard<BIO> guard(bio, &FreeBio);

                blocking = sslGateway.BIO_set_nbio_(bio, 1) != SSL_OPERATION_SUCCESS;

                std::stringstream stream;
                stream << hostname << ":" << port;

                std::string address = stream.str();

                long res = sslGateway.BIO_set_conn_hostname_(bio, address.c_str());
                if (res != SSL_OPERATION_SUCCESS)
                    ThrowLastSecureError("Can not set SSL connection hostname");

                SSL* ssl = 0;
                sslGateway.BIO_get_ssl_(bio, &ssl);
                if (!ssl)
                    ThrowLastSecureError("Can not get SSL instance from BIO");

                guard.Release();

                return ssl;
            }

            bool SecureSocketClient::CompleteConnectInternal(void* ssl, int timeout)
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                assert(sslGateway.Loaded());

                SSL* ssl0 = reinterpret_cast<SSL*>(ssl);

                while (true)
                {
                    int res = sslGateway.SSL_connect_(ssl0);

                    if (res == SSL_OPERATION_SUCCESS)
                        return true;

                    int sslError = sslGateway.SSL_get_error_(ssl0, res);

                    if (IsActualError(sslError))
                        ThrowLastSecureError("Can not establish secure connection");

                    int want = sslGateway.SSL_want_(ssl0);

                    res = WaitOnSocket(ssl, timeout, want == SSL_READING);

                    if (res == WaitResult::TIMEOUT)
                        return false;

                    if (res != WaitResult::SUCCESS)
                        ThrowLastSecureError("Error while establishing secure connection");
                }
            }

            void SecureSocketClient::CloseInternal()
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                if (sslGateway.Loaded() && ssl)
                {
                    sslGateway.SSL_free_(reinterpret_cast<SSL*>(ssl));

                    ssl = 0;
                }
            }

            int SecureSocketClient::WaitOnSocket(void* ssl, int32_t timeout, bool rd)
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                assert(sslGateway.Loaded());

                SSL* ssl0 = reinterpret_cast<SSL*>(ssl);
                int fd = sslGateway.SSL_get_fd_(ssl0);

                if (fd < 0)
                {
                    std::stringstream ss;
                    ss << "Can not get file descriptor from the SSL socket, fd=" << fd;

                    ThrowLastSecureError(ss.str());
                }

                return sockets::WaitOnSocket(fd, timeout, rd);
            }
        }
    }
}
