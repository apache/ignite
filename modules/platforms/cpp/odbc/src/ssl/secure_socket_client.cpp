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

#include <openssl/ssl.h>
#include <openssl/conf.h>

#include "ignite/common/concurrent.h"
#include "ignite/odbc/log.h"
#include "ignite/odbc/ssl/secure_socket_client.h"

// Declaring constant used by OpenSSL for readability.
enum { SSL_OPERATION_SUCCESS = 1 };

namespace ignite
{
    namespace odbc
    {
        namespace ssl
        {
            SecureSocketClient::SecureSocketClient(const std::string& certPath, const std::string& keyPath,
                const std::string& caPath, const std::string& caDirPath):
                certPath(certPath),
                keyPath(keyPath),
                caPath(caPath),
                caDirPath(caDirPath),
                context(0),
                sslBio(0)
            {
                // No-op.
            }

            SecureSocketClient::~SecureSocketClient()
            {
                Close();

                if (context)
                    SSL_CTX_free(reinterpret_cast<SSL_CTX*>(context));
            }

            bool SecureSocketClient::Connect(const char* hostname, uint16_t port, diagnostic::Diagnosable& diag)
            {
                if (!context)
                {
                    context = MakeContext(certPath, keyPath, caPath, caDirPath, diag);

                    if (!context)
                    {
                        diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                            "Can not create SSL context. Aborting connect.");

                        return false;
                    }
                }

                BIO* bio = BIO_new_ssl_connect(reinterpret_cast<SSL_CTX*>(context));
                if (!bio)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Can not create SSL connection.");

                    return false;
                }

                std::stringstream stream;
                stream << hostname << ":" << port;

                std::string address = stream.str();

                long res = BIO_set_conn_hostname(bio, address.c_str());
                if (res != SSL_OPERATION_SUCCESS)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Can not set SSL connection hostname.");

                    BIO_free_all(bio);

                    return false;
                }

                SSL* ssl = 0;
                BIO_get_ssl(bio, &ssl);
                if (!ssl)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Can not get SSL instance from BIO.");

                    BIO_free_all(bio);

                    return false;
                }

                res = SSL_set_tlsext_host_name(ssl, hostname);
                if (res != SSL_OPERATION_SUCCESS)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Can not set host name for secure connection");

                    BIO_free_all(bio);

                    return false;
                }

                res = BIO_do_connect(bio);
                if (res != SSL_OPERATION_SUCCESS)
                {
                    diag.AddStatusRecord(SqlState::S08001_CANNOT_CONNECT,
                        "Failed to establish secure connection with the host.");

                    BIO_free_all(bio);

                    return false;
                }

                res = BIO_do_handshake(bio);
                if (res != SSL_OPERATION_SUCCESS)
                {
                    diag.AddStatusRecord(SqlState::S08001_CANNOT_CONNECT, "SSL handshake failed.");

                    BIO_free_all(bio);

                    return false;
                }

                // Verify a server certificate was presented during the negotiation
                X509* cert = SSL_get_peer_certificate(ssl);
                if (cert)
                    X509_free(cert);
                else
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Remote host did not provide certificate.");

                    BIO_free_all(bio);

                    return false;
                }

                // Verify the result of chain verification
                // Verification performed according to RFC 4158
                res = SSL_get_verify_result(ssl);
                if (X509_V_OK != res)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Certificate chain verification failed.");

                    BIO_free_all(bio);

                    return false;
                }

                sslBio = reinterpret_cast<void*>(bio);

                return true;
            }

            void SecureSocketClient::Close()
            {
                if (sslBio)
                {
                    BIO_free_all(reinterpret_cast<BIO*>(sslBio));
                    
                    sslBio = 0;
                }
            }

            int SecureSocketClient::Send(const int8_t* data, size_t size, int32_t timeout)
            {
                if (!sslBio)
                {
                    LOG_MSG("Trying to send data using closed connection");

                    return -1;
                }

                (void)timeout;
                BIO* sslBio0 = reinterpret_cast<BIO*>(sslBio);

                int res = 0;

                do
                {
                    res = BIO_write(sslBio0, data, static_cast<int>(size));
                }
                while (BIO_should_retry(sslBio0));

                return res;
            }

            int SecureSocketClient::Receive(int8_t* buffer, size_t size, int32_t timeout)
            {
                if (!sslBio)
                {
                    LOG_MSG("Trying to receive data using closed connection");

                    return -1;
                }

                (void)timeout;
                BIO* sslBio0 = reinterpret_cast<BIO*>(sslBio);

                int res = 0;

                do
                {
                    res = BIO_read(sslBio0, buffer, static_cast<int>(size));
                }
                while (BIO_should_retry(sslBio0));

                return res;
            }

            void* SecureSocketClient::MakeContext(const std::string& certPath, const std::string& keyPath,
                const std::string& caPath, const std::string& caDirPath, diagnostic::Diagnosable& diag)
            {
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

                const SSL_METHOD* method = SSLv23_method();
                if (!method)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Can not create new SSL method.");

                    return 0;
                }

                SSL_CTX* ctx = SSL_CTX_new(method);
                if (!ctx)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Can not create new SSL context.");

                    return 0;
                }

                SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, 0);

                SSL_CTX_set_verify_depth(ctx, 8);

                const long flags = SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_COMPRESSION;
                SSL_CTX_set_options(ctx, flags);

                const char* cCaPath = caPath.empty() ? 0 : caPath.c_str();
                const char* cCaDirPath = caDirPath.empty() ? 0 : caDirPath.c_str();

                long res = SSL_CTX_load_verify_locations(ctx, cCaPath, cCaDirPath);
                if (res != SSL_OPERATION_SUCCESS)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                        "Can not set Certificate Authority path for secure connection.");

                    SSL_CTX_free(ctx);

                    return 0;
                }

                res = SSL_CTX_use_certificate_chain_file(ctx, certPath.c_str());
                if (res != SSL_OPERATION_SUCCESS)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                        "Can not set client certificate file for secure connection.");

                    SSL_CTX_free(ctx);

                    return 0;
                }

                res = SSL_CTX_use_RSAPrivateKey_file(ctx, keyPath.c_str(), SSL_FILETYPE_PEM);
                if (res != SSL_OPERATION_SUCCESS)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                        "Can not set private key file for secure connection.");

                    SSL_CTX_free(ctx);

                    return 0;
                }

                const char* const PREFERRED_CIPHERS = "HIGH:!aNULL:!kRSA:!PSK:!SRP:!MD5:!RC4";
                res = SSL_CTX_set_cipher_list(ctx, PREFERRED_CIPHERS);
                if (res != SSL_OPERATION_SUCCESS)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                        "Can not set ciphers list for secure connection.");

                    SSL_CTX_free(ctx);

                    return false;
                }

                return ctx;
            }
        }
    }
}
