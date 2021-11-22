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
#include <ignite/common/concurrent.h>
#include <ignite/ignite_error.h>

#include "network/ssl/secure_socket_client.h"
#include "network/ssl/ssl_gateway.h"

enum { OPERATION_SUCCESS = 1 };

void FreeContext(SSL_CTX* ctx)
{
    using namespace ignite::network::ssl;

    SslGateway &sslGateway = SslGateway::GetInstance();

    assert(sslGateway.Loaded());

    sslGateway.SSL_CTX_free_(ctx);
}

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
            SecureSocketClient::SecureSocketClient(const std::string& certPath, const std::string& keyPath,
                const std::string& caPath):
                certPath(certPath),
                keyPath(keyPath),
                caPath(caPath),
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
                    SslGateway::GetInstance().SSL_CTX_free_(reinterpret_cast<SSL_CTX*>(context));
            }

            bool SecureSocketClient::Connect(const char* hostname, uint16_t port, int32_t timeout)
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                assert(sslGateway.Loaded());

                CloseInternal();

                if (!context)
                {
                    context = MakeContext(certPath, keyPath, caPath);

                    if (!context)
                        ThrowSecureError("Can not create SSL context. Aborting connect");
                }

                ssl = MakeSsl(context, hostname, port, blocking);

                assert(ssl != 0);

                common::MethodGuard<SecureSocketClient> guard(this, &SecureSocketClient::CloseInternal);

                SSL* ssl0 = reinterpret_cast<SSL*>(ssl);

                int res = sslGateway.SSL_set_tlsext_host_name_(ssl0, hostname);

                if (res != OPERATION_SUCCESS)
                    ThrowSecureError("Can not set host name for secure connection: " + GetSslError(ssl0, res));

                sslGateway.SSL_set_connect_state_(ssl0);

                bool connected = CompleteConnectInternal(ssl0, timeout);

                if (!connected)
                    return false;

                // Verify a server certificate was presented during the negotiation
                X509* cert = sslGateway.SSL_get_peer_certificate_(ssl0);
                if (cert)
                    sslGateway.X509_free_(cert);
                else
                    ThrowSecureError("Remote host did not provide certificate: " + GetSslError(ssl0, res));

                // Verify the result of chain verification
                // Verification performed according to RFC 4158
                res = sslGateway.SSL_get_verify_result_(ssl0);
                if (X509_V_OK != res)
                    ThrowSecureError("Certificate chain verification failed: " + GetSslError(ssl0, res));

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
                    ThrowNetworkError("Trying to send data using closed connection");

                SSL* ssl0 = reinterpret_cast<SSL*>(ssl);

                return sslGateway.SSL_write_(ssl0, data, static_cast<int>(size));
            }

            int SecureSocketClient::Receive(int8_t* buffer, size_t size, int32_t timeout)
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                assert(sslGateway.Loaded());

                if (!ssl)
                    ThrowNetworkError("Trying to receive data using closed connection");

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

            void* SecureSocketClient::MakeContext(const std::string& certPath, const std::string& keyPath,
                const std::string& caPath)
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                assert(sslGateway.Loaded());

                const SSL_METHOD* method = sslGateway.SSLv23_client_method_();
                if (!method)
                    ThrowSecureError("Can not get SSL method");

                SSL_CTX* ctx = sslGateway.SSL_CTX_new_(method);
                if (!ctx)
                    ThrowSecureError("Can not create new SSL context");

                common::DeinitGuard<SSL_CTX> guard(ctx, &FreeContext);

                sslGateway.SSL_CTX_set_verify_(ctx, SSL_VERIFY_PEER, 0);

                sslGateway.SSL_CTX_set_verify_depth_(ctx, 8);

                sslGateway.SSL_CTX_set_options_(ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_COMPRESSION);

                const char* cCaPath = caPath.empty() ? 0 : caPath.c_str();

                long res = sslGateway.SSL_CTX_load_verify_locations_(ctx, cCaPath, 0);
                if (res != OPERATION_SUCCESS)
                    ThrowSecureError("Can not set Certificate Authority path for secure connection");

                res = sslGateway.SSL_CTX_use_certificate_chain_file_(ctx, certPath.c_str());
                if (res != OPERATION_SUCCESS)
                    ThrowSecureError("Can not set client certificate file for secure connection");

                res = sslGateway.SSL_CTX_use_RSAPrivateKey_file_(ctx, keyPath.c_str(), SSL_FILETYPE_PEM);
                if (res != OPERATION_SUCCESS)
                    ThrowSecureError("Can not set private key file for secure connection");

                const char* const PREFERRED_CIPHERS = "HIGH:!aNULL:!kRSA:!PSK:!SRP:!MD5:!RC4";
                res = sslGateway.SSL_CTX_set_cipher_list_(ctx, PREFERRED_CIPHERS);
                if (res != OPERATION_SUCCESS)
                    ThrowSecureError("Can not set ciphers list for secure connection");

                guard.Release();

                return ctx;
            }

            void* SecureSocketClient::MakeSsl(void* context, const char* hostname, uint16_t port, bool& blocking)
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                assert(sslGateway.Loaded());

                BIO* bio = sslGateway.BIO_new_ssl_connect_(reinterpret_cast<SSL_CTX*>(context));
                if (!bio)
                    ThrowSecureError("Can not create SSL connection");

                common::DeinitGuard<BIO> guard(bio, &FreeBio);

                blocking = sslGateway.BIO_set_nbio_(bio, 1) != OPERATION_SUCCESS;

                std::stringstream stream;
                stream << hostname << ":" << port;

                std::string address = stream.str();

                long res = sslGateway.BIO_set_conn_hostname_(bio, address.c_str());
                if (res != OPERATION_SUCCESS)
                    ThrowSecureError("Can not set SSL connection hostname");

                SSL* ssl = 0;
                sslGateway.BIO_get_ssl_(bio, &ssl);
                if (!ssl)
                    ThrowSecureError("Can not get SSL instance from BIO");

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

                    if (res == OPERATION_SUCCESS)
                        return true;

                    int sslError = sslGateway.SSL_get_error_(ssl0, res);

                    if (IsActualError(sslError))
                        ThrowSecureError("Can not establish secure connection: " + GetSslError(ssl0, res));

                    int want = sslGateway.SSL_want_(ssl0);

                    res = WaitOnSocket(ssl, timeout, want == SSL_READING);

                    if (res == WaitResult::TIMEOUT)
                        return false;

                    if (res != WaitResult::SUCCESS)
                        ThrowSecureError("Error while establishing secure connection: " + GetSslError(ssl0, res));
                }
            }

            std::string SecureSocketClient::GetSslError(void* ssl, int ret)
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                assert(sslGateway.Loaded());

                SSL* ssl0 = reinterpret_cast<SSL*>(ssl);

                int sslError = sslGateway.SSL_get_error_(ssl0, ret);

                switch (sslError)
                {
                    case SSL_ERROR_NONE:
                        break;

                    case SSL_ERROR_WANT_WRITE:
                        return std::string("SSL_connect wants write");

                    case SSL_ERROR_WANT_READ:
                        return std::string("SSL_connect wants read");

                    default:
                        return std::string("SSL error: ") + common::LexicalCast<std::string>(sslError);
                }

                long error = sslGateway.ERR_get_error_();

                char errBuf[1024] = { 0 };

                sslGateway.ERR_error_string_n_(error, errBuf, sizeof(errBuf));

                return std::string(errBuf);
            }

            bool SecureSocketClient::IsActualError(int err)
            {
                switch (err)
                {
                    case SSL_ERROR_NONE:
                    case SSL_ERROR_WANT_WRITE:
                    case SSL_ERROR_WANT_READ:
                    case SSL_ERROR_WANT_CONNECT:
                    case SSL_ERROR_WANT_ACCEPT:
                    case SSL_ERROR_WANT_X509_LOOKUP:
                        return false;

                    default:
                        return true;
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

            void SecureSocketClient::ThrowSecureError(const std::string& err)
            {
                throw IgniteError(IgniteError::IGNITE_ERR_SECURE_CONNECTION_FAILURE, err.c_str());
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

                    ss << "Can not get file descriptor from the SSL socket: " << fd << ", " << GetSslError(ssl, fd);

                    ThrowNetworkError(ss.str());
                }

                return sockets::WaitOnSocket(fd, timeout, rd);
            }
        }
    }
}
