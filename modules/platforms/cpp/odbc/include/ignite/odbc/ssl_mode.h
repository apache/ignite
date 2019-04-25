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

#ifndef _IGNITE_ODBC_SSL_SSL_MODE
#define _IGNITE_ODBC_SSL_SSL_MODE

#include <string>

#include "ignite/odbc/diagnostic/diagnosable.h"

namespace ignite
{
    namespace odbc
    {
        namespace ssl
        {
            /** SSL Mode enum. */
            struct SslMode
            {
                enum Type
                {
                    DISABLE = 0,

                    REQUIRE = 1,

                    UNKNOWN = 100
                };

                /**
                 * Convert mode from string. 
                 *
                 * @param val String value.
                 * @param dflt Default value to return on error.
                 * @return Corresponding enum value.
                 */
                static Type FromString(const std::string& val, Type dflt = UNKNOWN);

                /**
                 * Convert mode to string. 
                 *
                 * @param val Value to convert.
                 * @return String value.
                 */
                static std::string ToString(Type val);
            };
        }
    }
}

#endif //_IGNITE_ODBC_SSL_SSL_MODE