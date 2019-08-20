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
#include "ignite/odbc/ssl/ssl_gateway.h"
#include "ignite/common/utils.h"

#ifndef SOCKET_ERROR
#   define SOCKET_ERROR (-1)
#endif // SOCKET_ERROR

enum { OPERATION_SUCCESS = 1 };

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
                    SslGateway::GetInstance().SSL_CTX_free_(reinterpret_cast<SSL_CTX*>(context));
            }

            bool SecureSocketClient::Connect(const char* hostname, uint16_t port, int32_t,
                diagnostic::Diagnosable& diag)
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                assert(sslGateway.Loaded());

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

                int res = sslGateway.SSL_set_tlsext_host_name_(ssl0, hostname);
                if (res != OPERATION_SUCCESS)
                {
                    sslGateway.SSL_free_(ssl0);

                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                        "Can not set host name for secure connection: " + GetSslError(ssl0, res));

                    return false;
                }

                sslGateway.SSL_set_connect_state_(ssl0);

                bool connected = CompleteConnectInternal(ssl0, DEFALT_CONNECT_TIMEOUT, diag);

                if (!connected)
                {
                    sslGateway.SSL_free_(ssl0);

                    return false;
                }

                // Verify a server certificate was presented during the negotiation
                X509* cert = sslGateway.SSL_get_peer_certificate_(ssl0);
                if (cert)
                    sslGateway.X509_free_(cert);
                else
                {
                    sslGateway.SSL_free_(ssl0);

                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                        "Remote host did not provide certificate: " + GetSslError(ssl0, res));

                    return false;
                }

                // Verify the result of chain verification
                // Verification performed according to RFC 4158
                res = sslGateway.SSL_get_verify_result_(ssl0);
                if (X509_V_OK != res)
                {
                    sslGateway.SSL_free_(ssl0);

                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                        "Certificate chain verification failed: " + GetSslError(ssl0, res));

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
                SslGateway &sslGateway = SslGateway::GetInstance();

                assert(sslGateway.Loaded());

                if (!ssl)
                {
                    LOG_MSG("Trying to send data using closed connection");

                    return -1;
                }

                SSL* ssl0 = reinterpret_cast<SSL*>(ssl);

                int res = sslGateway.SSL_write_(ssl0, data, static_cast<int>(size));

                return res;
            }

            int SecureSocketClient::Receive(int8_t* buffer, size_t size, int32_t timeout)
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                assert(sslGateway.Loaded());

                if (!ssl)
                {
                    LOG_MSG("Trying to receive data using closed connection");

                    return -1;
                }

                SSL* ssl0 = reinterpret_cast<SSL*>(ssl);

                int res = 0;

                if (!blocking && sslGateway.SSL_pending_(ssl0) == 0)
                {
                    res = WaitOnSocket(ssl, timeout, true);

                    if (res < 0 || res == WaitResult::TIMEOUT)
                        return res;
                }

                res = sslGateway.SSL_read_(ssl0, buffer, static_cast<int>(size));

                return res;
            }

            bool SecureSocketClient::IsBlocking() const
            {
                return blocking;
            }

            void* SecureSocketClient::MakeContext(const std::string& certPath, const std::string& keyPath,
                const std::string& caPath, diagnostic::Diagnosable& diag)
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                assert(sslGateway.Loaded());

                const SSL_METHOD* method = sslGateway.SSLv23_client_method_();
                if (!method)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Can not get SSL method.");

                    return 0;
                }

                SSL_CTX* ctx = sslGateway.SSL_CTX_new_(method);
                if (!ctx)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Can not create new SSL context.");

                    return 0;
                }

                sslGateway.SSL_CTX_set_verify_(ctx, SSL_VERIFY_PEER, 0);

                sslGateway.SSL_CTX_set_verify_depth_(ctx, 8);

                sslGateway.SSL_CTX_set_options_(ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_COMPRESSION);

                const char* cCaPath = caPath.empty() ? 0 : caPath.c_str();

                long res = sslGateway.SSL_CTX_load_verify_locations_(ctx, cCaPath, 0);
                if (res != OPERATION_SUCCESS)
                {
                    sslGateway.SSL_CTX_free_(ctx);

                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                        "Can not set Certificate Authority path for secure connection.");

                    return 0;
                }

                res = sslGateway.SSL_CTX_use_certificate_chain_file_(ctx, certPath.c_str());
                if (res != OPERATION_SUCCESS)
                {
                    sslGateway.SSL_CTX_free_(ctx);

                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                        "Can not set client certificate file for secure connection.");

                    return 0;
                }

                res = sslGateway.SSL_CTX_use_RSAPrivateKey_file_(ctx, keyPath.c_str(), SSL_FILETYPE_PEM);
                if (res != OPERATION_SUCCESS)
                {
                    sslGateway.SSL_CTX_free_(ctx);

                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                        "Can not set private key file for secure connection.");

                    return 0;
                }

                const char* const PREFERRED_CIPHERS = "HIGH:!aNULL:!kRSA:!PSK:!SRP:!MD5:!RC4";
                res = sslGateway.SSL_CTX_set_cipher_list_(ctx, PREFERRED_CIPHERS);
                if (res != OPERATION_SUCCESS)
                {
                    sslGateway.SSL_CTX_free_(ctx);

                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                        "Can not set ciphers list for secure connection.");

                    return 0;
                }

                return ctx;
            }

            void* SecureSocketClient::MakeSsl(void* context, const char* hostname, uint16_t port,
                bool& blocking, diagnostic::Diagnosable& diag)
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                assert(sslGateway.Loaded());

                BIO* bio = sslGateway.BIO_new_ssl_connect_(reinterpret_cast<SSL_CTX*>(context));
                if (!bio)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Can not create SSL connection.");

                    return 0;
                }

                blocking = false;
                long res = sslGateway.BIO_set_nbio_(bio, 1);
                if (res != OPERATION_SUCCESS)
                {
                    blocking = true;

                    diag.AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                        "Can not set up non-blocking mode. Timeouts are not available.");
                }

                std::stringstream stream;
                stream << hostname << ":" << port;

                std::string address = stream.str();

                res = sslGateway.BIO_set_conn_hostname_(bio, address.c_str());
                if (res != OPERATION_SUCCESS)
                {
                    sslGateway.BIO_free_all_(bio);

                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Can not set SSL connection hostname.");

                    return 0;
                }

                SSL* ssl = 0;
                sslGateway.BIO_get_ssl_(bio, &ssl);
                if (!ssl)
                {
                    sslGateway.BIO_free_all_(bio);

                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Can not get SSL instance from BIO.");

                    return 0;
                }

                return ssl;
            }

            bool SecureSocketClient::CompleteConnectInternal(void* ssl, int timeout, diagnostic::Diagnosable& diag)
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

                    LOG_MSG("wait res=" << res << ", sslError=" << sslError);

                    if (IsActualError(sslError))
                    {
                        diag.AddStatusRecord(SqlState::S08001_CANNOT_CONNECT,
                            "Can not establish secure connection: " + GetSslError(ssl0, res));

                        return false;
                    }

                    int want = sslGateway.SSL_want_(ssl0);

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
                SslGateway &sslGateway = SslGateway::GetInstance();

                assert(sslGateway.Loaded());

                SSL* ssl0 = reinterpret_cast<SSL*>(ssl);

                int sslError = sslGateway.SSL_get_error_(ssl0, ret);

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

            void SecureSocketClient::CloseInteral()
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                assert(sslGateway.Loaded());

                if (ssl)
                {
                    sslGateway.SSL_free_(reinterpret_cast<SSL*>(ssl));

                    ssl = 0;
                }
            }

            int SecureSocketClient::WaitOnSocket(void* ssl, int32_t timeout, bool rd)
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                assert(sslGateway.Loaded());

                int ready = 0;
                int lastError = 0;
                SSL* ssl0 = reinterpret_cast<SSL*>(ssl);

                fd_set fds;

                int fd = sslGateway.SSL_get_fd_(ssl0);

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
