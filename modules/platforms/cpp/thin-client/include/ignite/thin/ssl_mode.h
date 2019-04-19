/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @file
 * Declares ignite::thin::SslMode.
 */

#ifndef _IGNITE_THIN_SSL_MODE
#define _IGNITE_THIN_SSL_MODE

namespace ignite
{
    namespace thin
    {
        /** SSL Mode. */
        struct SslMode
        {
            enum Type
            {
                /** Do not try establish SSL/TLS connection. */
                DISABLE = 0,

                /** Try to establish SSL/TLS connection. Fail if the server does not support SSL/TLS. */
                REQUIRE = 1,
            };
        };
    }
}

#endif //_IGNITE_THIN_SSL_MODE
