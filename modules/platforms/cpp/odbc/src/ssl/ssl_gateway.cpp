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

#include <openssl/ssl.h>
#include <openssl/conf.h>

#include "ignite/common/utils.h"

#include "ignite/odbc/ssl/ssl_gateway.h"
#include "ignite/odbc/log.h"

namespace ignite
{
    namespace odbc
    {
        namespace ssl
        {
            SslGateway* SslGateway::self = 0;
            common::concurrent::CriticalSection SslGateway::constructionCs;

            SslGateway::SslGateway() :
                inited(false),
                functions()
            {
                // No-op.
            }

            SslGateway::~SslGateway()
            {
                // No-op.
            }

            common::dynamic::Module SslGateway::LoadSslLibrary(const char* name)
            {
                using namespace common;
                using namespace dynamic;

                std::string fullName = GetDynamicLibraryName(name);

                Module libModule = LoadModule(fullName);

                if (libModule.IsLoaded())
                    return libModule;

                std::string home = GetEnv("OPEN_SSL_HOME");

                if (home.empty())
                    home = GetEnv("OPENSSL_HOME");

                if (home.empty())
                    return libModule;

                std::stringstream constructor;

                constructor << home << Fs << "bin" << Fs << fullName;

                std::string fullPath = constructor.str();

                return LoadModule(fullPath);
            }

            SslGateway& SslGateway::GetInstance()
            {
                if (!self)
                {
                    common::concurrent::CsLockGuard lock(constructionCs);

                    if (!self)
                        self = new SslGateway();
                }

                return *self;
            }

            bool SslGateway::LoadAll()
            {
                using namespace common::dynamic;

                if (inited)
                    return true;

                common::concurrent::CsLockGuard lock(initCs);

                if (inited)
                    return true;

                libeay32 = LoadSslLibrary("libeay32");

                if (!libeay32.IsLoaded())
                {
                    LOG_MSG("Can not load libeay32.");

                    return false;
                }

                ssleay32 = LoadSslLibrary("ssleay32");

                if (!ssleay32.IsLoaded())
                {
                    LOG_MSG("Can not load ssleay32.");

                    libeay32.Unload();

                    return false;
                }

                functions.fpSSL_CTX_new = LoadSslMethod(ssleay32, "SSL_CTX_new");
                functions.fpSSL_CTX_free = LoadSslMethod(ssleay32, "SSL_CTX_free");
                functions.fpSSL_CTX_set_verify = LoadSslMethod(ssleay32, "SSL_CTX_set_verify");
                functions.fpSSL_CTX_set_verify_depth = LoadSslMethod(ssleay32, "SSL_CTX_set_verify_depth");
                functions.fpSSL_CTX_load_verify_locations = LoadSslMethod(ssleay32, "SSL_CTX_load_verify_locations");
                functions.fpSSL_CTX_use_certificate_chain_file =
                    LoadSslMethod(ssleay32, "SSL_CTX_use_certificate_chain_file");
                functions.fpSSL_CTX_use_RSAPrivateKey_file = LoadSslMethod(ssleay32, "SSL_CTX_use_RSAPrivateKey_file");
                functions.fpSSL_CTX_set_cipher_list = LoadSslMethod(ssleay32, "SSL_CTX_set_cipher_list");

                functions.fpSSL_get_verify_result = LoadSslMethod(ssleay32, "SSL_get_verify_result");
                functions.fpSSL_library_init = LoadSslMethod(ssleay32, "SSL_library_init");
                functions.fpSSL_load_error_strings = LoadSslMethod(ssleay32, "SSL_load_error_strings");
                functions.fpSSL_get_peer_certificate = LoadSslMethod(ssleay32, "SSL_get_peer_certificate");
                functions.fpSSL_ctrl = LoadSslMethod(ssleay32, "SSL_ctrl");
                functions.fpSSL_CTX_ctrl = LoadSslMethod(ssleay32, "SSL_CTX_ctrl");

                functions.fpSSLv23_method = LoadSslMethod(ssleay32, "SSLv23_method");
                functions.fpBIO_new_ssl_connect = LoadSslMethod(ssleay32, "BIO_new_ssl_connect");

                functions.fpOPENSSL_config = LoadSslMethod(libeay32, "OPENSSL_config");
                functions.fpX509_free = LoadSslMethod(libeay32, "X509_free");

                functions.fpBIO_write = LoadSslMethod(libeay32, "BIO_write");
                functions.fpBIO_read = LoadSslMethod(libeay32, "BIO_read");
                functions.fpBIO_free_all = LoadSslMethod(libeay32, "BIO_free_all");
                functions.fpBIO_test_flags = LoadSslMethod(libeay32, "BIO_test_flags");
                functions.fpBIO_ctrl = LoadSslMethod(libeay32, "BIO_ctrl");

                bool allLoaded =
                    functions.fpSSL_CTX_new != 0 &&
                    functions.fpSSL_CTX_free != 0 &&
                    functions.fpSSL_CTX_set_verify != 0 &&
                    functions.fpSSL_CTX_set_verify_depth != 0 &&
                    functions.fpSSL_CTX_load_verify_locations != 0 &&
                    functions.fpSSL_CTX_use_certificate_chain_file != 0 &&
                    functions.fpSSL_CTX_use_RSAPrivateKey_file != 0 &&
                    functions.fpSSL_CTX_set_cipher_list != 0 &&
                    functions.fpSSL_get_verify_result != 0 &&
                    functions.fpSSL_library_init != 0 &&
                    functions.fpSSL_load_error_strings != 0 &&
                    functions.fpSSL_get_peer_certificate != 0 &&
                    functions.fpSSL_ctrl != 0 &&
                    functions.fpSSL_CTX_ctrl != 0 &&
                    functions.fpSSLv23_method != 0 &&
                    functions.fpBIO_new_ssl_connect != 0 &&
                    functions.fpOPENSSL_config != 0 &&
                    functions.fpX509_free != 0 &&
                    functions.fpBIO_write != 0 &&
                    functions.fpBIO_read != 0 &&
                    functions.fpBIO_free_all != 0 &&
                    functions.fpBIO_test_flags != 0 &&
                    functions.fpBIO_ctrl != 0;

                if (!allLoaded)
                {
                    libeay32.Unload();
                    ssleay32.Unload();
                }

                inited = allLoaded;

                return inited;
            }

            void* SslGateway::LoadSslMethod(common::dynamic::Module mod, const char* name)
            {
                void* fp = mod.FindSymbol(name);

                if (!fp)
                    LOG_MSG("Can not load function " << name);

                return fp;
            }
        }
    }
}
