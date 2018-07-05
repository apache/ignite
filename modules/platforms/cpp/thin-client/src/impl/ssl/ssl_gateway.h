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

#ifndef _IGNITE_IMPL_THIN_SSL_SSL_LIBRARY
#define _IGNITE_IMPL_THIN_SSL_SSL_LIBRARY

#include <ignite/common/concurrent.h>
#include <ignite/common/dynamic_load_os.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace ssl
            {
                /**
                 * Functions collection.
                 */
                struct SslFunctions
                {
                    void *fpSSL_CTX_new;
                    void *fpSSL_CTX_free;
                    void *fpSSL_CTX_set_verify;
                    void *fpSSL_CTX_set_verify_depth;
                    void *fpSSL_CTX_load_verify_locations;
                    void *fpSSL_CTX_use_certificate_chain_file;
                    void *fpSSL_CTX_use_RSAPrivateKey_file;
                    void *fpSSL_CTX_set_cipher_list;
                    void *fpSSL_CTX_ctrl;
                    void *fpSSL_get_verify_result;
                    void *fpSSL_library_init;
                    void *fpSSL_load_error_strings;
                    void *fpSSL_get_peer_certificate;
                    void *fpSSL_ctrl;
                    void *fpSSLv23_client_method;
                    void *fpSSL_set_connect_state;
                    void *fpSSL_connect;
                    void *fpSSL_get_error;
                    void *fpSSL_want;
                    void *fpSSL_write;
                    void *fpSSL_read;
                    void *fpSSL_pending;
                    void *fpSSL_get_fd;
                    void *fpSSL_free;
                    void *fpOPENSSL_config;
                    void *fpX509_free;
                    void *fpBIO_new_ssl_connect;
                    void *fpBIO_free_all;
                    void *fpBIO_ctrl;
                    void *fpERR_get_error;
                    void *fpERR_error_string_n;
                };

                /**
                 * SSL Gateway abstraction.
                 * Used as a factory for secure sockets. Needed for dynamic loading
                 * of the SSL libraries.
                 */
                class SslGateway
                {
                public:
                    /**
                     * Get class instance.
                     * @return SslLibrary instance.
                     */
                    static SslGateway& GetInstance();

                    /**
                     * Try loading SSL library.
                     */
                    void LoadAll();

                    /**
                     * Get functions.
                     * @return Functions structure.
                     */
                    SslFunctions& GetFunctions()
                    {
                        return functions;
                    }

                    /**
                     * Check whether the libraries are loaded.
                     * @return @c true if loaded.
                     */
                    bool Loaded() const
                    {
                        return inited;
                    }

                private:
                    /**
                     * Constructor.
                     */
                    SslGateway();

                    /**
                     * Destructor.
                     */
                    ~SslGateway();

                    /**
                     * Load SSL library.
                     * @param name Name.
                     * @return Module.
                     */
                    common::dynamic::Module LoadSslLibrary(const char* name);

                    /**
                     * Load all SSL libraries.
                     */
                    void LoadSslLibraries();

                    /**
                     * Load SSL method.
                     * @param name Name.
                     * @return Method pointer.
                     */
                    void* LoadSslMethod(const char* name);

                    /** Indicates whether the library was inited. */
                    bool inited;

                    /** Critical section to prevent multiple instance creation. */
                    common::concurrent::CriticalSection initCs;

                    /** libeay32 module. */
                    common::dynamic::Module libeay32;

                    /** ssleay32 module. */
                    common::dynamic::Module ssleay32;

                    /** libssl module. */
                    common::dynamic::Module libssl;

                    /** Functions. */
                    SslFunctions functions;
                };
            }
        }
        
    }
}

#endif //_IGNITE_IMPL_THIN_SSL_SSL_LIBRARY