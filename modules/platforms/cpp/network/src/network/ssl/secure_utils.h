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

#ifndef _IGNITE_NETWORK_SSL_SECURE_UTILS
#define _IGNITE_NETWORK_SSL_SECURE_UTILS

#include <ignite/network/ssl/secure_configuration.h>
#include "network/ssl/ssl_gateway.h"

namespace ignite
{
    namespace network
    {
        namespace ssl
        {
            enum
            {
                /** OpenSSL functions return this code on success. */
                SSL_OPERATION_SUCCESS = 1,
            };

            /**
             * Make SSL context using configuration.
             *
             * @param cfg Configuration to use.
             * @return New context instance on success.
             * @throw IgniteError on error.
             */
            SSL_CTX* MakeContext(const SecureConfiguration& cfg);

            /**
             * Free context.
             *
             * @param ctx Context to free.
             */
            void FreeContext(SSL_CTX* ctx);

            /**
             * Check whether error is actual error or code returned when used in async environment.
             *
             * @param err Error obtained with SSL_get_error.
             * @return @c true if the code returned on actual error.
             */
            bool IsActualError(int err);

            /**
             * Throw SSL-related error.
             *
             * @param err Error message.
             */
            void ThrowSecureError(const std::string& err);

            /**
             * Get SSL-related error in text format.
             *
             * @param err Error message in human-readable format.
             */
            std::string GetLastSecureError();

            /**
             * Try extract from OpenSSL error stack and throw SSL-related error.
             *
             * @param description Error description.
             * @param advice User advice.
             */
            void ThrowLastSecureError(const std::string& description, const std::string& advice);

            /**
             * Try extract from OpenSSL error stack and throw SSL-related error.
             *
             * @param description Error description.
             */
            void ThrowLastSecureError(const std::string& description);
        }
    }
}

#endif //_IGNITE_NETWORK_SSL_SECURE_UTILS
