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

#include <sstream>
#include <cstdio>

#include "ignite/odbc/log.h"
#include "ignite/common/concurrent.h"
#include "ignite/odbc/system/tcp_socket_client.h"
#include "ignite/odbc/ssl/secure_socket_client.h"
#include "ignite/odbc/ssl/ssl_bindings.h"
#include "ignite/common/utils.h"

#ifndef SOCKET_ERROR
#   define SOCKET_ERROR (-1)
#endif // SOCKET_ERROR

namespace ignite
{
    namespace odbc
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
                CloseInteral();

                if (context)
                    ssl::SSL_CTX_free(reinterpret_cast<SSL_CTX*>(context));
            }

            bool SecureSocketClient::Connect(const char* hostname, uint16_t port, int32_t,
                diagnostic::Diagnosable& diag)
            {
                assert(SslGateway::GetInstance().Loaded());

                if (!context)
                {
                    context = MakeContext(certPath, keyPath, caPath, diag);

                    if (!context)
                    {
                        diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                            "Can not create SSL context. Aborting connect.");

                        return false;
                    }
                }

                SSL* ssl0 = reinterpret_cast<SSL*>(MakeSsl(context, hostname, port, blocking, diag));
                if (!ssl0)
                    return false;

                int res = ssl::SSL_set_tlsext_host_name_(ssl0, hostname);
                if (res != OPERATION_SUCCESS)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                        "Can not set host name for secure connection: " + GetSslError(ssl0, res));

                    ssl::SSL_free_(ssl0);

                    return false;
                }

                ssl::SSL_set_connect_state_(ssl0);

                bool connected = CompleteConnectInternal(ssl0, DEFALT_CONNECT_TIMEOUT, diag);

                if (!connected)
                {
                    ssl::SSL_free_(ssl0);

                    return false;
                }

                // Verify a server certificate was presented during the negotiation
                X509* cert = ssl::SSL_get_peer_certificate(ssl0);
                if (cert)
                    ssl::X509_free(cert);
                else
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                        "Remote host did not provide certificate: " + GetSslError(ssl0, res));

                    ssl::SSL_free_(ssl0);

                    return false;
                }

                // Verify the result of chain verification
                // Verification performed according to RFC 4158
                res = ssl::SSL_get_verify_result(ssl0);
                if (X509_V_OK != res)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                        "Certificate chain verification failed: " + GetSslError(ssl0, res));

                    ssl::SSL_free_(ssl0);

                    return false;
                }

                ssl = reinterpret_cast<void*>(ssl0);

                return true;
            }

            void SecureSocketClient::Close()
            {
                CloseInteral();
            }

            int SecureSocketClient::Send(const int8_t* data, size_t size, int32_t timeout)
            {
                assert(SslGateway::GetInstance().Loaded());

                if (!ssl)
                {
                    LOG_MSG("Trying to send data using closed connection");

                    return -1;
                }

                SSL* ssl0 = reinterpret_cast<SSL*>(ssl);

                int res = ssl::SSL_write_(ssl0, data, static_cast<int>(size));

                return res;
            }

            int SecureSocketClient::Receive(int8_t* buffer, size_t size, int32_t timeout)
            {
                assert(SslGateway::GetInstance().Loaded());

                if (!ssl)
                {
                    LOG_MSG("Trying to receive data using closed connection");

                    return -1;
                }

                SSL* ssl0 = reinterpret_cast<SSL*>(ssl);

                int res = 0;

                if (!blocking && ssl::SSL_pending_(ssl0) == 0)
                {
                    res = WaitOnSocket(ssl, timeout, true);

                    if (res < 0 || res == WaitResult::TIMEOUT)
                        return res;
                }

                res = ssl::SSL_read_(ssl0, buffer, static_cast<int>(size));

                return res;
            }

            bool SecureSocketClient::IsBlocking() const
            {
                return blocking;
            }

            void* SecureSocketClient::MakeContext(const std::string& certPath, const std::string& keyPath,
                const std::string& caPath, diagnostic::Diagnosable& diag)
            {
                assert(SslGateway::GetInstance().Loaded());

                static bool sslLibInited = false;
                static common::concurrent::CriticalSection sslCs;

                if (!sslLibInited)
                {
                    common::concurrent::CsLockGuard lock(sslCs);

                    if (!sslLibInited)
                    {
                        LOG_MSG("Initializing SSL library");

                        (void)SSL_library_init();

                        SSL_load_error_strings();

                        OPENSSL_config(0);

                        sslLibInited = true;
                    }
                }

                const SSL_METHOD* method = ssl::SSLv23_client_method_();
                if (!method)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Can not get SSL method.");

                    return 0;
                }

                SSL_CTX* ctx = ssl::SSL_CTX_new(method);
                if (!ctx)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Can not create new SSL context.");

                    return 0;
                }

                ssl::SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, 0);

                ssl::SSL_CTX_set_verify_depth(ctx, 8);

                const long flags = SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_COMPRESSION;
                ssl::SSL_CTX_ctrl(ctx, SSL_CTRL_OPTIONS, flags, NULL);

                const char* cCaPath = caPath.empty() ? 0 : caPath.c_str();

                long res = ssl::SSL_CTX_load_verify_locations(ctx, cCaPath, 0);
                if (res != OPERATION_SUCCESS)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                        "Can not set Certificate Authority path for secure connection.");

                    ssl::SSL_CTX_free(ctx);

                    return 0;
                }

                res = ssl::SSL_CTX_use_certificate_chain_file(ctx, certPath.c_str());
                if (res != OPERATION_SUCCESS)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                        "Can not set client certificate file for secure connection.");

                    ssl::SSL_CTX_free(ctx);

                    return 0;
                }

                res = ssl::SSL_CTX_use_RSAPrivateKey_file(ctx, keyPath.c_str(), SSL_FILETYPE_PEM);
                if (res != OPERATION_SUCCESS)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                        "Can not set private key file for secure connection.");

                    ssl::SSL_CTX_free(ctx);

                    return 0;
                }

                const char* const PREFERRED_CIPHERS = "HIGH:!aNULL:!kRSA:!PSK:!SRP:!MD5:!RC4";
                res = ssl::SSL_CTX_set_cipher_list(ctx, PREFERRED_CIPHERS);
                if (res != OPERATION_SUCCESS)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                        "Can not set ciphers list for secure connection.");

                    ssl::SSL_CTX_free(ctx);

                    return 0;
                }

                return ctx;
            }

            void* SecureSocketClient::MakeSsl(void* context, const char* hostname, uint16_t port,
                bool& blocking, diagnostic::Diagnosable& diag)
            {
                BIO* bio = ssl::BIO_new_ssl_connect(reinterpret_cast<SSL_CTX*>(context));
                if (!bio)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Can not create SSL connection.");

                    return 0;
                }

                blocking = false;
                long res = ssl::BIO_set_nbio_(bio, 1);
                if (res != OPERATION_SUCCESS)
                {
                    blocking = true;

                    diag.AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                        "Can not set up non-blocking mode. Timeouts are not available.");
                }

                std::stringstream stream;
                stream << hostname << ":" << port;

                std::string address = stream.str();

                res = ssl::BIO_set_conn_hostname_(bio, address.c_str());
                if (res != OPERATION_SUCCESS)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Can not set SSL connection hostname.");

                    ssl::BIO_free_all(bio);

                    return 0;
                }

                SSL* ssl = 0;
                ssl::BIO_get_ssl_(bio, &ssl);
                if (!ssl)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Can not get SSL instance from BIO.");

                    ssl::BIO_free_all(bio);

                    return 0;
                }

                return ssl;
            }

            bool SecureSocketClient::CompleteConnectInternal(void* ssl, int timeout, diagnostic::Diagnosable& diag)
            {
                SSL* ssl0 = reinterpret_cast<SSL*>(ssl);

                while (true)
                {
                    int res = ssl::SSL_connect_(ssl0);

                    if (res == OPERATION_SUCCESS)
                        return true;

                    int sslError = ssl::SSL_get_error_(ssl0, res);

                    LOG_MSG("wait res=" << res << ", sslError=" << sslError);

                    if (IsActualError(sslError))
                    {
                        diag.AddStatusRecord(SqlState::S08001_CANNOT_CONNECT,
                            "Can not establish secure connection: " + GetSslError(ssl0, res));

                        return false;
                    }

                    int want = ssl::SSL_want_(ssl0);

                    res = WaitOnSocket(ssl, timeout, want == SSL_READING);

                    LOG_MSG("wait res=" << res << ", want=" << want);

                    if (res == WaitResult::TIMEOUT)
                    {
                        diag.AddStatusRecord(SqlState::SHYT01_CONNECTION_TIMEOUT,
                            "Can not establish secure connection: Timeout expired (" +
                                common::LexicalCast<std::string>(timeout) + " seconds)");

                        return false;
                    }

                    if (res != WaitResult::SUCCESS)
                    {
                        diag.AddStatusRecord(SqlState::S08001_CANNOT_CONNECT,
                            "Can not establish secure connection due to internal error");

                        return false;
                    }
                }
            }

            std::string SecureSocketClient::GetSslError(void* ssl, int ret)
            {
                SSL* ssl0 = reinterpret_cast<SSL*>(ssl);

                int sslError = ssl::SSL_get_error_(ssl0, ret);

                LOG_MSG("ssl_error: " << sslError);

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

                long error = ssl::ERR_get_error_();

                char errBuf[1024] = { 0 };

                ssl::ERR_error_string_n_(error, errBuf, sizeof(errBuf));

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

            void SecureSocketClient::CloseInteral()
            {
                assert(SslGateway::GetInstance().Loaded());

                if (ssl)
                {
                    ssl::SSL_free_(reinterpret_cast<SSL*>(ssl));

                    ssl = 0;
                }
            }

            int SecureSocketClient::WaitOnSocket(void* ssl, int32_t timeout, bool rd)
            {
                int ready = 0;
                int lastError = 0;
                SSL* ssl0 = reinterpret_cast<SSL*>(ssl);

                fd_set fds;

                int fd = ssl::SSL_get_fd_(ssl0);

                if (fd < 0)
                {
                    LOG_MSG("Can not get file descriptor from the SSL socket: " << fd << ", " << GetSslError(ssl, fd));

                    return fd;
                }

                do {
                    struct timeval tv = { 0 };
                    tv.tv_sec = timeout;

                    FD_ZERO(&fds);
                    FD_SET(static_cast<long>(fd), &fds);

                    fd_set* readFds = 0;
                    fd_set* writeFds = 0;

                    if (rd)
                        readFds = &fds;
                    else
                        writeFds = &fds;

                    ready = select(fd + 1, readFds, writeFds, NULL, (timeout == 0 ? NULL : &tv));

                    if (ready == SOCKET_ERROR)
                        lastError = system::TcpSocketClient::GetLastSocketError();

                } while (ready == SOCKET_ERROR && system::TcpSocketClient::IsSocketOperationInterrupted(lastError));

                if (ready == SOCKET_ERROR)
                    return -lastError;

                lastError = system::TcpSocketClient::GetLastSocketError(fd);

                if (lastError != 0)
                    return -lastError;

                if (ready == 0)
                    return WaitResult::TIMEOUT;

                return WaitResult::SUCCESS;
            }
        }
    }
}
