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
                REQUIRE = 1
            };
        };
    }
}

#endif //_IGNITE_THIN_SSL_MODE
