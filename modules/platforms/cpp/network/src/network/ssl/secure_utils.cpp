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

#ifdef _WIN32
#include "network/sockets.h"
#include <wincrypt.h>
#endif

#include <ignite/ignite_error.h>

#include <ignite/common/utils.h>
#include <ignite/network/network.h>

#include "network/ssl/secure_utils.h"

namespace
{
    void LoadDefaultCa(SSL_CTX* sslContext)
    {
        using namespace ignite::common;
        using namespace ignite::network::ssl;

        assert(sslContext != 0);
        SslGateway &sslGateway = SslGateway::GetInstance();

#ifndef _WIN32
        long res = sslGateway.SSL_CTX_set_default_verify_paths_(sslContext);
        if (res != SSL_OPERATION_SUCCESS)
            ThrowLastSecureError("Can not set default Certificate Authority for secure connection",
                 "Try setting custom CA");
#else
        X509_STORE *sslStore = sslGateway.X509_STORE_new_();
        if (!sslStore)
            ThrowLastSecureError("Can not create X509_STORE certificate store", "Try setting custom CA");

        HCERTSTORE sysStore = CertOpenSystemStoreA(NULL, "ROOT");
        if (!sysStore)
            ThrowSecureError(GetLastSystemError("Can not open System Certificate store for secure connection",
                "Try setting custom CA"));

        PCCERT_CONTEXT certIter = CertEnumCertificatesInStore(sysStore, NULL);
        while (certIter)
        {
            const unsigned char *currentCert = certIter->pbCertEncoded;
            X509* x509 = sslGateway.d2i_X509_(NULL, &currentCert, static_cast<long>(certIter->cbCertEncoded));
            if (x509)
            {
                sslGateway.X509_STORE_add_cert_(sslStore, x509);

                sslGateway.X509_free_(x509);
            }
            certIter = CertEnumCertificatesInStore(sysStore, certIter);
        }

        CertCloseStore(sysStore, 0);

        sslGateway.SSL_CTX_set_cert_store_(sslContext, sslStore);
#endif
    }
}

namespace ignite
{
    namespace network
    {
        namespace ssl
        {
            SSL_CTX* MakeContext(const SecureConfiguration& cfg)
            {
                EnsureSslLoaded();

                SslGateway &sslGateway = SslGateway::GetInstance();

                const SSL_METHOD* method = sslGateway.SSLv23_client_method_();
                if (!method)
                    ThrowLastSecureError("Can not get SSL method");

                SSL_CTX* sslContext = sslGateway.SSL_CTX_new_(method);
                if (!sslContext)
                    ThrowLastSecureError("Can not create new SSL context");

                common::DeinitGuard<SSL_CTX> guard(sslContext, &FreeContext);

                sslGateway.SSL_CTX_set_verify_(sslContext, SSL_VERIFY_PEER, 0);

                sslGateway.SSL_CTX_set_verify_depth_(sslContext, 8);

                sslGateway.SSL_CTX_set_options_(sslContext, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_COMPRESSION);

                if (!cfg.caPath.empty())
                {
                    long res = sslGateway.SSL_CTX_load_verify_locations_(sslContext, cfg.caPath.c_str(), 0);
                    if (res != SSL_OPERATION_SUCCESS)
                        ThrowLastSecureError("Can not set Certificate Authority path for secure connection, path=" +
                            cfg.caPath);
                }
                else
                    LoadDefaultCa(sslContext);

                if (!cfg.certPath.empty())
                {
                    long res = sslGateway.SSL_CTX_use_certificate_chain_file_(sslContext, cfg.certPath.c_str());
                    if (res != SSL_OPERATION_SUCCESS)
                        ThrowLastSecureError("Can not set client certificate file for secure connection, path=" +
                            cfg.certPath);
                }

                if (!cfg.keyPath.empty())
                {
                    long res = sslGateway.SSL_CTX_use_RSAPrivateKey_file_(sslContext, cfg.keyPath.c_str(), SSL_FILETYPE_PEM);
                    if (res != SSL_OPERATION_SUCCESS)
                        ThrowLastSecureError("Can not set private key file for secure connection, path=" + cfg.keyPath);
                }

                const char* const PREFERRED_CIPHERS = "HIGH:!aNULL:!kRSA:!PSK:!SRP:!MD5:!RC4";
                long res = sslGateway.SSL_CTX_set_cipher_list_(sslContext, PREFERRED_CIPHERS);
                if (res != SSL_OPERATION_SUCCESS)
                    ThrowLastSecureError("Can not set ciphers list for secure connection");

                guard.Release();
                return sslContext;
            }

            void FreeContext(SSL_CTX* ctx)
            {
                using namespace ignite::network::ssl;

                SslGateway &sslGateway = SslGateway::GetInstance();

                assert(sslGateway.Loaded());

                sslGateway.SSL_CTX_free_(ctx);
            }

            bool IsActualError(int err)
            {
                switch (err)
                {
                    case SSL_ERROR_NONE:
                    case SSL_ERROR_WANT_READ:
                    case SSL_ERROR_WANT_WRITE:
                    case SSL_ERROR_WANT_X509_LOOKUP:
                    case SSL_ERROR_WANT_CONNECT:
                    case SSL_ERROR_WANT_ACCEPT:
                        return false;

                    default:
                        return true;
                }
            }

            void ThrowSecureError(const std::string& err)
            {
                throw IgniteError(IgniteError::IGNITE_ERR_SECURE_CONNECTION_FAILURE, err.c_str());
            }

            std::string GetLastSecureError()
            {
                using namespace ignite::network::ssl;

                SslGateway &sslGateway = SslGateway::GetInstance();

                assert(sslGateway.Loaded());

                unsigned long errorCode = sslGateway.ERR_get_error_();

                std::string errorDetails;
                if (errorCode != 0)
                {
                    char errBuf[1024] = { 0 };

                    sslGateway.ERR_error_string_n_(errorCode, errBuf, sizeof(errBuf));

                    errorDetails.assign(errBuf);
                }

                return errorDetails;
            }

            void ThrowLastSecureError(const std::string& description, const std::string& advice)
            {
                ThrowSecureError(common::FormatErrorMessage(description, GetLastSecureError(), advice));
            }

            void ThrowLastSecureError(const std::string& description)
            {
                std::string empty;
                ThrowLastSecureError(description, empty);
            }
        }
    }
}
