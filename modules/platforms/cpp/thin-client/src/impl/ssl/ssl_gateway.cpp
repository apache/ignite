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

#include <ignite/ignite_error.h>
#include <ignite/common/utils.h>

#include "impl/ssl/ssl_gateway.h"

#ifndef ADDITIONAL_OPENSSL_HOME_ENV
#   define ADDITIONAL_OPENSSL_HOME_ENV "OPEN_SSL_HOME"
#endif // ADDITIONAL_OPENSSL_HOME_ENV

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace ssl
            {
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

                    std::string home = GetEnv(ADDITIONAL_OPENSSL_HOME_ENV);

                    if (home.empty())
                        home = GetEnv("OPENSSL_HOME");

                    if (home.empty())
                        return libModule;

                    std::stringstream constructor;

                    constructor << home << Fs << "bin" << Fs << fullName;

                    std::string fullPath = constructor.str();

                    return LoadModule(fullPath);
                }

                void SslGateway::LoadSslLibraries()
                {
                    libeay32 = LoadSslLibrary("libeay32");
                    ssleay32 = LoadSslLibrary("ssleay32");
                    libssl = LoadSslLibrary("libssl");

                    if (!libssl.IsLoaded() && (!libeay32.IsLoaded() || !ssleay32.IsLoaded()))
                    {
                        if (!libssl.IsLoaded())
                            throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                                "Can not load neccessary OpenSSL library: libssl");

                        std::stringstream ss;

                        ss << "Can not load neccessary OpenSSL libraries: ";

                        if (!libeay32.IsLoaded())
                            ss << "libeay32";

                        if (!ssleay32.IsLoaded())
                            ss << " ssleay32";

                        libeay32.Unload();
                        ssleay32.Unload();
                        libssl.Unload();

                        std::string res = ss.str();

                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, res.c_str());
                    }
                }

                SslGateway& SslGateway::GetInstance()
                {
                    static SslGateway self;

                    return self;
                }

                void SslGateway::LoadAll()
                {
                    using namespace common::dynamic;

                    if (inited)
                        return;

                    common::concurrent::CsLockGuard lock(initCs);

                    if (inited)
                        return;

                    LoadSslLibraries();

                    functions.fpSSL_CTX_new = LoadSslMethod("SSL_CTX_new");
                    functions.fpSSL_CTX_free = LoadSslMethod("SSL_CTX_free");
                    functions.fpSSL_CTX_set_verify = LoadSslMethod("SSL_CTX_set_verify");
                    functions.fpSSL_CTX_set_verify_depth = LoadSslMethod("SSL_CTX_set_verify_depth");
                    functions.fpSSL_CTX_load_verify_locations = LoadSslMethod("SSL_CTX_load_verify_locations");
                    functions.fpSSL_CTX_use_certificate_chain_file = LoadSslMethod("SSL_CTX_use_certificate_chain_file");
                    functions.fpSSL_CTX_use_RSAPrivateKey_file = LoadSslMethod("SSL_CTX_use_RSAPrivateKey_file");
                    functions.fpSSL_CTX_set_cipher_list = LoadSslMethod("SSL_CTX_set_cipher_list");

                    functions.fpSSL_get_verify_result = LoadSslMethod("SSL_get_verify_result");
                    functions.fpSSL_library_init = LoadSslMethod("SSL_library_init");
                    functions.fpSSL_load_error_strings = LoadSslMethod("SSL_load_error_strings");
                    functions.fpSSL_get_peer_certificate = LoadSslMethod("SSL_get_peer_certificate");
                    functions.fpSSL_ctrl = LoadSslMethod("SSL_ctrl");
                    functions.fpSSL_CTX_ctrl = LoadSslMethod("SSL_CTX_ctrl");

                    functions.fpSSLv23_client_method = LoadSslMethod("SSLv23_client_method");
                    functions.fpSSL_set_connect_state = LoadSslMethod("SSL_set_connect_state");
                    functions.fpSSL_connect = LoadSslMethod("SSL_connect");
                    functions.fpSSL_get_error = LoadSslMethod("SSL_get_error");
                    functions.fpSSL_want = LoadSslMethod("SSL_want");
                    functions.fpSSL_write = LoadSslMethod("SSL_write");
                    functions.fpSSL_read = LoadSslMethod("SSL_read");
                    functions.fpSSL_pending = LoadSslMethod("SSL_pending");
                    functions.fpSSL_get_fd = LoadSslMethod("SSL_get_fd");
                    functions.fpSSL_free = LoadSslMethod("SSL_free");
                    functions.fpBIO_new_ssl_connect = LoadSslMethod("BIO_new_ssl_connect");

                    functions.fpOPENSSL_config = LoadSslMethod("OPENSSL_config");
                    functions.fpX509_free = LoadSslMethod("X509_free");

                    functions.fpBIO_free_all = LoadSslMethod("BIO_free_all");
                    functions.fpBIO_ctrl = LoadSslMethod("BIO_ctrl");

                    functions.fpERR_get_error = LoadSslMethod("ERR_get_error");
                    functions.fpERR_error_string_n = LoadSslMethod("ERR_error_string_n");

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
                        functions.fpSSLv23_client_method != 0 &&
                        functions.fpSSL_set_connect_state != 0 &&
                        functions.fpSSL_connect != 0 &&
                        functions.fpSSL_get_error != 0 &&
                        functions.fpSSL_want != 0 &&
                        functions.fpSSL_write != 0 &&
                        functions.fpSSL_read != 0 &&
                        functions.fpSSL_pending != 0 &&
                        functions.fpSSL_get_fd != 0 &&
                        functions.fpSSL_free != 0 &&
                        functions.fpBIO_new_ssl_connect != 0 &&
                        functions.fpOPENSSL_config != 0 &&
                        functions.fpX509_free != 0 &&
                        functions.fpBIO_free_all != 0 &&
                        functions.fpBIO_ctrl != 0 &&
                        functions.fpERR_get_error != 0 &&
                        functions.fpERR_error_string_n != 0;

                    if (!allLoaded)
                    {
                        libeay32.Unload();
                        ssleay32.Unload();
                        libssl.Unload();
                    }

                    inited = allLoaded;
                }

                void* SslGateway::LoadSslMethod(const char* name)
                {
                    void* fp = libeay32.FindSymbol(name);

                    if (!fp)
                        fp = ssleay32.FindSymbol(name);

                    if (!fp)
                        fp = libssl.FindSymbol(name);

                    if (!fp)
                    {
                        std::stringstream ss;

                        ss << "Can not load function " << name;

                        std::string res = ss.str();

                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, res.c_str());
                    }

                    return fp;
                }
            }
        }
    }
}
