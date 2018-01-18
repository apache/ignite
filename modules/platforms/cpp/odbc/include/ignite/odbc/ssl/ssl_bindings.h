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

#ifndef _IGNITE_ODBC_SSL_SSL_BINDINGS
#define _IGNITE_ODBC_SSL_SSL_BINDINGS

#include <openssl/ssl.h>
#include <openssl/conf.h>

#include "ignite/odbc/ssl/ssl_gateway.h"

namespace ignite
{
    namespace odbc
    {
        namespace ssl
        {
            // Declaring constant used by OpenSSL for readability.
            enum { OPERATION_SUCCESS = 1 };

            inline SSL_CTX *SSL_CTX_new(const SSL_METHOD *meth)
            {
                typedef SSL_CTX*(FuncType)(const SSL_METHOD*);

                FuncType* fp = reinterpret_cast<FuncType*>(SslGateway::GetInstance().GetFunctions().fpSSL_CTX_new);

                return fp(meth);
            }

            inline void SSL_CTX_free(SSL_CTX *ctx)
            {
                typedef void(FuncType)(SSL_CTX*);

                FuncType* fp = reinterpret_cast<FuncType*>(SslGateway::GetInstance().GetFunctions().fpSSL_CTX_free);

                fp(ctx);
            }

            inline void SSL_CTX_set_verify(SSL_CTX *ctx, int mode, int(*callback) (int, X509_STORE_CTX *))
            {
                typedef void(FuncType)(SSL_CTX*, int, int(*)(int, X509_STORE_CTX*));

                FuncType* fp = reinterpret_cast<FuncType*>(
                    SslGateway::GetInstance().GetFunctions().fpSSL_CTX_set_verify);

                fp(ctx, mode, callback);
            }

            inline void SSL_CTX_set_verify_depth(SSL_CTX *ctx, int depth)
            {
                typedef void(FuncType)(SSL_CTX*, int);

                FuncType* fp = reinterpret_cast<FuncType*>(
                    SslGateway::GetInstance().GetFunctions().fpSSL_CTX_set_verify_depth);

                fp(ctx, depth);
            }

            inline int SSL_CTX_load_verify_locations(SSL_CTX *ctx, const char *cAfile, const char *cApath)
            {
                typedef int(FuncType)(SSL_CTX*, const char*, const char*);

                FuncType* fp = reinterpret_cast<FuncType*>(
                    SslGateway::GetInstance().GetFunctions().fpSSL_CTX_load_verify_locations);

                return fp(ctx, cAfile, cApath);
            }

            inline int SSL_CTX_use_certificate_chain_file(SSL_CTX *ctx, const char *file)
            {
                typedef int(FuncType)(SSL_CTX*, const char*);

                FuncType* fp = reinterpret_cast<FuncType*>(
                    SslGateway::GetInstance().GetFunctions().fpSSL_CTX_use_certificate_chain_file);

                return fp(ctx, file);
            }

            inline int SSL_CTX_use_RSAPrivateKey_file(SSL_CTX *ctx, const char *file, int type)
            {
                typedef int(FuncType)(SSL_CTX*, const char*, int);

                FuncType* fp = reinterpret_cast<FuncType*>(
                    SslGateway::GetInstance().GetFunctions().fpSSL_CTX_use_RSAPrivateKey_file);

                return fp(ctx, file, type);
            }

            inline int SSL_CTX_set_cipher_list(SSL_CTX *ctx, const char *str)
            {
                typedef int(FuncType)(SSL_CTX*, const char*);

                FuncType* fp = reinterpret_cast<FuncType*>(
                    SslGateway::GetInstance().GetFunctions().fpSSL_CTX_set_cipher_list);

                return fp(ctx, str);
            }

            inline long SSL_CTX_ctrl(SSL_CTX *ctx, int cmd, long larg, void *parg)
            {
                typedef long(FuncType)(SSL_CTX*, int, long, void*);

                FuncType* fp = reinterpret_cast<FuncType*>(SslGateway::GetInstance().GetFunctions().fpSSL_CTX_ctrl);

                return fp(ctx, cmd, larg, parg);
            }

            inline long SSL_get_verify_result(const SSL *s)
            {
                typedef long(FuncType)(const SSL*);

                FuncType* fp = reinterpret_cast<FuncType*>(
                    SslGateway::GetInstance().GetFunctions().fpSSL_get_verify_result);

                return fp(s);
            }

            inline int SSL_library_init()
            {
                typedef int(FuncType)();

                FuncType* fp = reinterpret_cast<FuncType*>(SslGateway::GetInstance().GetFunctions().fpSSL_library_init);

                return fp();
            }

            inline void SSL_load_error_strings()
            {
                typedef void(FuncType)();

                FuncType* fp = reinterpret_cast<FuncType*>(
                    SslGateway::GetInstance().GetFunctions().fpSSL_load_error_strings);

                fp();
            }

            inline X509 *SSL_get_peer_certificate(const SSL *s)
            {
                typedef X509*(FuncType)(const SSL*);

                FuncType* fp = reinterpret_cast<FuncType*>(
                    SslGateway::GetInstance().GetFunctions().fpSSL_get_peer_certificate);

                return fp(s);
            }

            inline long SSL_ctrl(SSL *s, int cmd, long larg, void *parg)
            {
                typedef long(FuncType)(SSL*, int, long, void*);

                FuncType* fp = reinterpret_cast<FuncType*>(SslGateway::GetInstance().GetFunctions().fpSSL_ctrl);

                return fp(s, cmd, larg ,parg);
            }

            inline long SSL_set_tlsext_host_name_(SSL *s, const char *name)
            {
                return ssl::SSL_ctrl(s, SSL_CTRL_SET_TLSEXT_HOSTNAME,
                    TLSEXT_NAMETYPE_host_name, const_cast<char*>(name));
            }



            inline const SSL_METHOD *SSLv23_method()
            {
                typedef const SSL_METHOD*(FuncType)();

                FuncType* fp = reinterpret_cast<FuncType*>(SslGateway::GetInstance().GetFunctions().fpSSLv23_method);

                return fp();
            }

            inline void OPENSSL_config(const char *configName)
            {
                typedef void(FuncType)(const char*);

                FuncType* fp = reinterpret_cast<FuncType*>(SslGateway::GetInstance().GetFunctions().fpOPENSSL_config);

                fp(configName);
            }

            inline void X509_free(X509 *a)
            {
                typedef void(FuncType)(X509*);

                FuncType* fp = reinterpret_cast<FuncType*>(SslGateway::GetInstance().GetFunctions().fpX509_free);

                fp(a);
            }

            inline BIO *BIO_new_ssl_connect(SSL_CTX *ctx)
            {
                typedef BIO*(FuncType)(SSL_CTX*);

                FuncType* fp = reinterpret_cast<FuncType*>(
                    SslGateway::GetInstance().GetFunctions().fpBIO_new_ssl_connect);

                return fp(ctx);
            }

            inline int BIO_write(BIO *b, const void *data, int len)
            {
                typedef int(FuncType)(BIO*, const void*, int);

                FuncType* fp = reinterpret_cast<FuncType*>(SslGateway::GetInstance().GetFunctions().fpBIO_write);

                return fp(b, data, len);
            }

            inline int BIO_read(BIO *b, void *data, int len)
            {
                typedef int(FuncType)(BIO*, const void*, int);

                FuncType* fp = reinterpret_cast<FuncType*>(SslGateway::GetInstance().GetFunctions().fpBIO_read);

                return fp(b, data, len);
            }

            inline void BIO_free_all(BIO *a)
            {
                typedef void(FuncType)(BIO*);

                FuncType* fp = reinterpret_cast<FuncType*>(SslGateway::GetInstance().GetFunctions().fpBIO_free_all);

                fp(a);
            }

            inline int BIO_test_flags(const BIO *b, int flags)
            {
                typedef int(FuncType)(const BIO*, int);

                FuncType* fp = reinterpret_cast<FuncType*>(SslGateway::GetInstance().GetFunctions().fpBIO_test_flags);

                return fp(b, flags);
            }

            inline int BIO_should_retry_(const BIO *b)
            {
                return ssl::BIO_test_flags(b, BIO_FLAGS_SHOULD_RETRY);
            }

            inline long BIO_ctrl(BIO *bp, int cmd, long larg, void *parg)
            {
                typedef long(FuncType)(BIO*, int, long, void*);

                FuncType* fp = reinterpret_cast<FuncType*>(SslGateway::GetInstance().GetFunctions().fpBIO_ctrl);

                return fp(bp, cmd, larg, parg);
            }

            inline long BIO_get_fd_(BIO *bp, int *fd)
            {
                return ssl::BIO_ctrl(bp, BIO_C_GET_FD, 0, reinterpret_cast<void*>(fd));
            }

            inline long BIO_do_handshake_(BIO *bp)
            {
                return ssl::BIO_ctrl(bp, BIO_C_DO_STATE_MACHINE, 0, NULL);
            }

            inline long BIO_do_connect_(BIO *bp)
            {
                return ssl::BIO_do_handshake_(bp);
            }

            inline long BIO_get_ssl_(BIO *bp, SSL** ssl)
            {
                return ssl::BIO_ctrl(bp, BIO_C_GET_SSL, 0, reinterpret_cast<void*>(ssl));
            }

            inline long BIO_set_nbio_(BIO *bp, long n)
            {
                return ssl::BIO_ctrl(bp, BIO_C_SET_NBIO, n, NULL);
            }

            inline long BIO_set_conn_hostname_(BIO *bp, const char *name)
            {
                return ssl::BIO_ctrl(bp, BIO_C_SET_CONNECT, 0, const_cast<char*>(name));
            }

            inline long BIO_pending_(BIO *bp)
            {
                return ssl::BIO_ctrl(bp, BIO_CTRL_PENDING, 0, NULL);
            }
        }
    }
}

#endif //_IGNITE_ODBC_SSL_SSL_BINDINGS