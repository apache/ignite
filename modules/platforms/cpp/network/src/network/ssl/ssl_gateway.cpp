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

#include "network/ssl/ssl_gateway.h"

#ifndef ADDITIONAL_OPENSSL_HOME_ENV
#   define ADDITIONAL_OPENSSL_HOME_ENV "OPEN_SSL_HOME"
#endif // ADDITIONAL_OPENSSL_HOME_ENV

#ifndef SSL_CTRL_OPTIONS
#   define SSL_CTRL_OPTIONS 32
#endif // SSL_CTRL_OPTIONS

#ifndef OPENSSL_INIT_LOAD_SSL_STRINGS
#   define OPENSSL_INIT_LOAD_SSL_STRINGS 0x00200000L
#endif // OPENSSL_INIT_LOAD_SSL_STRINGS

#ifndef OPENSSL_INIT_LOAD_CRYPTO_STRINGS
#   define OPENSSL_INIT_LOAD_CRYPTO_STRINGS 0x00000002L
#endif // OPENSSL_INIT_LOAD_CRYPTO_STRINGS

namespace ignite
{
    namespace network
    {
        namespace ssl
        {
            SslGateway::SslGateway() :
                inited(false),
                functions()
            {
                memset(&functions, 0, sizeof(functions));
            }

            SslGateway::~SslGateway()
            {
                // No-op.
            }

            void SslGateway::UnloadAll()
            {
                libeay32.Unload();
                ssleay32.Unload();
                libssl.Unload();
                libcrypto.Unload();

                memset(&functions, 0, sizeof(functions));
            }

            common::dynamic::Module SslGateway::LoadSslLibrary(const char* name)
            {
                using namespace common;
                using namespace dynamic;

                std::string home = GetEnv(ADDITIONAL_OPENSSL_HOME_ENV);

                if (home.empty())
                    home = GetEnv("OPENSSL_HOME");

                std::string fullName = GetDynamicLibraryName(name);

                if (!home.empty())
                {
                    const char* paths[] = {"bin", "lib"};

                    for (size_t i = 0; i < 2; i++) {
                        std::stringstream constructor;

                        constructor << home << Fs << paths[i] << Fs << fullName;

                        std::string fullPath = constructor.str();

                        Module mod = LoadModule(fullPath);

                        if (mod.IsLoaded())
                            return mod;
                    }
                }

                return LoadModule(fullName);
            }

            void SslGateway::LoadSslLibraries()
            {
                libssl = LoadSslLibrary("libssl");

                if (!libssl.IsLoaded())
                {
                    libcrypto = LoadSslLibrary("libcrypto-1_1-x64");
                    libssl = LoadSslLibrary("libssl-1_1-x64");
                }

                if (!libssl.IsLoaded())
                {
                    libeay32 = LoadSslLibrary("libeay32");
                    ssleay32 = LoadSslLibrary("ssleay32");
                }

                if (!libssl.IsLoaded() && (!libeay32.IsLoaded() || !ssleay32.IsLoaded()))
                {
                    if (!libssl.IsLoaded())
                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                            "Can not load neccessary OpenSSL library: libssl");

                    std::stringstream ss;

                    ss << "Can not load neccessary OpenSSL libraries:";

                    if (!libeay32.IsLoaded())
                        ss << " libeay32";

                    if (!ssleay32.IsLoaded())
                        ss << " ssleay32";

                    std::string res = ss.str();

                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, res.c_str());
                }
            }

            void SslGateway::LoadMandatoryMethods()
            {
                functions.fpSSLeay_version = TryLoadSslMethod("SSLeay_version");

                if (!functions.fpSSLeay_version)
                    functions.fpOpenSSL_version = LoadSslMethod("OpenSSL_version");

                functions.fpSSL_library_init = TryLoadSslMethod("SSL_library_init");
                functions.fpSSL_load_error_strings = TryLoadSslMethod("SSL_load_error_strings");

                if (!functions.fpSSL_library_init || !functions.fpSSL_load_error_strings)
                    functions.fpOPENSSL_init_ssl = LoadSslMethod("OPENSSL_init_ssl");

                functions.fpSSLv23_client_method = TryLoadSslMethod("SSLv23_client_method");

                if (!functions.fpSSLv23_client_method)
                    functions.fpTLS_client_method = LoadSslMethod("TLS_client_method");

                functions.fpSSL_CTX_new = LoadSslMethod("SSL_CTX_new");
                functions.fpSSL_CTX_free = LoadSslMethod("SSL_CTX_free");
                functions.fpSSL_CTX_set_verify = LoadSslMethod("SSL_CTX_set_verify");
                functions.fpSSL_CTX_set_verify_depth = LoadSslMethod("SSL_CTX_set_verify_depth");
                functions.fpSSL_CTX_load_verify_locations = LoadSslMethod("SSL_CTX_load_verify_locations");
                functions.fpSSL_CTX_use_certificate_chain_file = LoadSslMethod("SSL_CTX_use_certificate_chain_file");
                functions.fpSSL_CTX_use_RSAPrivateKey_file = LoadSslMethod("SSL_CTX_use_RSAPrivateKey_file");
                functions.fpSSL_CTX_set_cipher_list = LoadSslMethod("SSL_CTX_set_cipher_list");

                functions.fpSSL_get_verify_result = LoadSslMethod("SSL_get_verify_result");

                functions.fpSSL_get_peer_certificate = LoadSslMethod("SSL_get_peer_certificate");
                functions.fpSSL_ctrl = LoadSslMethod("SSL_ctrl");
                functions.fpSSL_CTX_ctrl = LoadSslMethod("SSL_CTX_ctrl");

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

                common::MethodGuard<SslGateway> guard(this, &SslGateway::UnloadAll);

                LoadSslLibraries();

                LoadMandatoryMethods();

                functions.fpSSL_CTX_set_options = TryLoadSslMethod("SSL_CTX_set_options");
                functions.fpERR_print_errors_fp = TryLoadSslMethod("ERR_print_errors_fp");

                IGNITE_UNUSED(SSL_library_init_());

                SSL_load_error_strings_();

                OPENSSL_config_(0);

                guard.Release();

                inited = true;
            }

            void* SslGateway::TryLoadSslMethod(const char* name)
            {
                void* fp = libeay32.FindSymbol(name);

                if (!fp)
                    fp = ssleay32.FindSymbol(name);

                if (!fp)
                    fp = libcrypto.FindSymbol(name);

                if (!fp)
                    fp = libssl.FindSymbol(name);

                return fp;
            }

            void* SslGateway::LoadSslMethod(const char* name)
            {
                void* fp = TryLoadSslMethod(name);

                if (!fp)
                {
                    std::stringstream ss;

                    ss << "Can not load function " << name;

                    std::string res = ss.str();

                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, res.c_str());
                }

                return fp;
            }

            char* SslGateway::SSLeay_version_(int type)
            {
                typedef char* (FuncType)(int);

                FuncType* fp = 0;

                if (functions.fpSSLeay_version)
                    fp = reinterpret_cast<FuncType*>(functions.fpSSLeay_version);
                else
                    fp = reinterpret_cast<FuncType*>(functions.fpOpenSSL_version);

                assert(fp != 0);

                return fp(type);
            }

            int SslGateway::OPENSSL_init_ssl_(uint64_t opts, const void* settings)
            {
                assert(functions.fpOPENSSL_init_ssl != 0);

                typedef int (FuncType)(uint64_t, const void*);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpOPENSSL_init_ssl);

                return fp(opts, settings);
            }

            long SslGateway::SSL_CTX_set_options_(SSL_CTX* ctx, long options)
            {
                if (functions.fpSSL_CTX_set_options)
                {
                    typedef long (FuncType)(SSL_CTX*, long);

                    FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_CTX_set_options);

                    return fp(ctx, options);
                }

                return SSL_CTX_ctrl_(ctx, SSL_CTRL_OPTIONS, options, NULL);
            }

            long SslGateway::SSL_CTX_ctrl_(SSL_CTX* ctx, int cmd, long larg, void* parg)
            {
                assert(functions.fpSSL_CTX_ctrl != 0);

                typedef long (FuncType)(SSL_CTX*, int, long, void*);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_CTX_ctrl);

                return fp(ctx, cmd, larg, parg);
            }

            SSL_CTX* SslGateway::SSL_CTX_new_(const SSL_METHOD* meth)
            {
                assert(functions.fpSSL_CTX_new != 0);

                typedef SSL_CTX*(FuncType)(const SSL_METHOD*);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_CTX_new);

                return fp(meth);
            }

            void SslGateway::SSL_CTX_free_(SSL_CTX* ctx)
            {
                assert(functions.fpSSL_CTX_free != 0);

                typedef void (FuncType)(SSL_CTX*);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_CTX_free);

                fp(ctx);
            }

            void SslGateway::SSL_CTX_set_verify_(SSL_CTX* ctx, int mode, int (* callback)(int, X509_STORE_CTX*))
            {
                assert(functions.fpSSL_CTX_set_verify != 0);

                typedef void (FuncType)(SSL_CTX*, int, int (*)(int, X509_STORE_CTX*));

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_CTX_set_verify);

                fp(ctx, mode, callback);
            }

            void SslGateway::SSL_CTX_set_verify_depth_(SSL_CTX* ctx, int depth)
            {
                assert(functions.fpSSL_CTX_set_verify_depth != 0);

                typedef void (FuncType)(SSL_CTX*, int);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_CTX_set_verify_depth);

                fp(ctx, depth);
            }

            int SslGateway::SSL_CTX_load_verify_locations_(SSL_CTX* ctx, const char* cAfile, const char* cApath)
            {
                assert(functions.fpSSL_CTX_load_verify_locations != 0);

                typedef int (FuncType)(SSL_CTX*, const char*, const char*);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_CTX_load_verify_locations);

                return fp(ctx, cAfile, cApath);
            }

            int SslGateway::SSL_CTX_use_certificate_chain_file_(SSL_CTX* ctx, const char* file)
            {
                assert(functions.fpSSL_CTX_use_certificate_chain_file != 0);

                typedef int (FuncType)(SSL_CTX*, const char*);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_CTX_use_certificate_chain_file);

                return fp(ctx, file);
            }

            int SslGateway::SSL_CTX_use_RSAPrivateKey_file_(SSL_CTX* ctx, const char* file, int type)
            {
                assert(functions.fpSSL_CTX_use_RSAPrivateKey_file != 0);

                typedef int (FuncType)(SSL_CTX*, const char*, int);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_CTX_use_RSAPrivateKey_file);

                return fp(ctx, file, type);
            }

            int SslGateway::SSL_CTX_set_cipher_list_(SSL_CTX* ctx, const char* str)
            {
                assert(functions.fpSSL_CTX_set_cipher_list != 0);

                typedef int (FuncType)(SSL_CTX*, const char*);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_CTX_set_cipher_list);

                return fp(ctx, str);
            }

            long SslGateway::SSL_get_verify_result_(const SSL* s)
            {
                assert(functions.fpSSL_get_verify_result != 0);

                typedef long (FuncType)(const SSL*);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_get_verify_result);

                return fp(s);
            }

            int SslGateway::SSL_library_init_()
            {
                typedef int (FuncType)();

                if (functions.fpSSL_library_init)
                {
                    FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_library_init);

                    return fp();
                }

                return OPENSSL_init_ssl_(0, NULL);
            }

            void SslGateway::SSL_load_error_strings_()
            {
                typedef void (FuncType)();

                if (functions.fpSSL_load_error_strings)
                {
                    FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_load_error_strings);

                    fp();

                    return;
                }

                OPENSSL_init_ssl_(OPENSSL_INIT_LOAD_SSL_STRINGS | OPENSSL_INIT_LOAD_CRYPTO_STRINGS, NULL);
            }

            X509* SslGateway::SSL_get_peer_certificate_(const SSL* s)
            {
                assert(functions.fpSSL_get_peer_certificate != 0);

                typedef X509*(FuncType)(const SSL*);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_get_peer_certificate);

                return fp(s);
            }

            long SslGateway::SSL_ctrl_(SSL* s, int cmd, long larg, void* parg)
            {
                assert(functions.fpSSL_ctrl != 0);

                typedef long (FuncType)(SSL*, int, long, void*);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_ctrl);

                return fp(s, cmd, larg, parg);
            }

            long SslGateway::SSL_set_tlsext_host_name_(SSL* s, const char* name)
            {
                return SSL_ctrl_(s, SSL_CTRL_SET_TLSEXT_HOSTNAME,
                                        TLSEXT_NAMETYPE_host_name, const_cast<char*>(name));
            }

            void SslGateway::SSL_set_connect_state_(SSL* s)
            {
                assert(functions.fpSSL_set_connect_state != 0);

                typedef void (FuncType)(SSL*);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_set_connect_state);

                return fp(s);
            }

            int SslGateway::SSL_connect_(SSL* s)
            {
                assert(functions.fpSSL_connect != 0);

                typedef int (FuncType)(SSL*);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_connect);

                return fp(s);
            }

            int SslGateway::SSL_get_error_(const SSL* s, int ret)
            {
                assert(functions.fpSSL_get_error != 0);

                typedef int (FuncType)(const SSL*, int);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_get_error);

                return fp(s, ret);
            }

            int SslGateway::SSL_want_(const SSL* s)
            {
                assert(functions.fpSSL_want != 0);

                typedef int (FuncType)(const SSL*);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_want);

                return fp(s);
            }

            int SslGateway::SSL_write_(SSL* s, const void* buf, int num)
            {
                assert(functions.fpSSL_write != 0);

                typedef int (FuncType)(SSL*, const void*, int);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_write);

                return fp(s, buf, num);
            }

            int SslGateway::SSL_read_(SSL* s, void* buf, int num)
            {
                assert(functions.fpSSL_read != 0);

                typedef int (FuncType)(SSL*, void*, int);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_read);

                return fp(s, buf, num);
            }

            int SslGateway::SSL_pending_(const SSL* ssl)
            {
                assert(functions.fpSSL_pending != 0);

                typedef int (FuncType)(const SSL*);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_pending);

                return fp(ssl);
            }

            int SslGateway::SSL_get_fd_(const SSL* ssl)
            {
                assert(functions.fpSSL_get_fd != 0);

                typedef int (FuncType)(const SSL*);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_get_fd);

                return fp(ssl);
            }

            void SslGateway::SSL_free_(SSL* ssl)
            {
                assert(functions.fpSSL_free != 0);

                typedef void (FuncType)(SSL*);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSL_free);

                fp(ssl);
            }

            const SSL_METHOD* SslGateway::SSLv23_client_method_()
            {
                if (functions.fpSSLv23_client_method)
                {
                    typedef const SSL_METHOD*(FuncType)();

                    FuncType* fp = reinterpret_cast<FuncType*>(functions.fpSSLv23_client_method);

                    return fp();
                }

                return TLS_client_method_();
            }

            const SSL_METHOD* SslGateway::TLS_client_method_()
            {
                assert(functions.fpTLS_client_method != 0);

                typedef const SSL_METHOD*(FuncType)();

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpTLS_client_method);

                return fp();
            }

            void SslGateway::OPENSSL_config_(const char* configName)
            {
                assert(functions.fpOPENSSL_config != 0);

                typedef void (FuncType)(const char*);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpOPENSSL_config);

                fp(configName);
            }

            void SslGateway::X509_free_(X509* a)
            {
                assert(functions.fpX509_free != 0);

                typedef void (FuncType)(X509*);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpX509_free);

                fp(a);
            }

            BIO* SslGateway::BIO_new_ssl_connect_(SSL_CTX* ctx)
            {
                assert(functions.fpBIO_new_ssl_connect != 0);

                typedef BIO*(FuncType)(SSL_CTX*);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpBIO_new_ssl_connect);

                return fp(ctx);
            }

            void SslGateway::BIO_free_all_(BIO* a)
            {
                assert(functions.fpBIO_free_all != 0);

                typedef void (FuncType)(BIO*);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpBIO_free_all);

                fp(a);
            }

            long SslGateway::BIO_ctrl_(BIO* bp, int cmd, long larg, void* parg)
            {
                assert(functions.fpBIO_ctrl != 0);

                typedef long (FuncType)(BIO*, int, long, void*);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpBIO_ctrl);

                return fp(bp, cmd, larg, parg);
            }

            long SslGateway::BIO_get_fd_(BIO* bp, int* fd)
            {
                return BIO_ctrl_(bp, BIO_C_GET_FD, 0, reinterpret_cast<void*>(fd));
            }

            long SslGateway::BIO_get_ssl_(BIO* bp, SSL** ssl)
            {
                return BIO_ctrl_(bp, BIO_C_GET_SSL, 0, reinterpret_cast<void*>(ssl));
            }

            long SslGateway::BIO_set_nbio_(BIO* bp, long n)
            {
                return BIO_ctrl_(bp, BIO_C_SET_NBIO, n, NULL);
            }

            long SslGateway::BIO_set_conn_hostname_(BIO* bp, const char* name)
            {
                return BIO_ctrl_(bp, BIO_C_SET_CONNECT, 0, const_cast<char*>(name));
            }

            unsigned long SslGateway::ERR_get_error_()
            {
                assert(functions.fpERR_get_error != 0);

                typedef unsigned long (FuncType)();

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpERR_get_error);

                return fp();
            }

            void SslGateway::ERR_error_string_n_(unsigned long e, char* buf, size_t len)
            {
                assert(functions.fpERR_error_string_n != 0);

                typedef void (FuncType)(unsigned long, char*, size_t);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpERR_error_string_n);

                fp(e, buf, len);
            }

            void SslGateway::ERR_print_errors_fp_(FILE* fd)
            {
                if (!functions.fpERR_print_errors_fp)
                    return;

                typedef void (FuncType)(FILE*);

                FuncType* fp = reinterpret_cast<FuncType*>(functions.fpERR_print_errors_fp);

                fp(fd);
            }
        }
    }
}
