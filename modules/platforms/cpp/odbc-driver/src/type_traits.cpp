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
#   include <windows.h>
#endif //_WIN32

#include <sqlext.h>
#include <odbcinst.h>

#include "ignite/odbc/type_traits.h"

namespace ignite
{
    namespace odbc
    {
        namespace type_traits
        {
            bool IsApplicationTypeSupported(int16_t type)
            {
                return ToDriverType(type) != IGNITE_SQL_TYPE_UNSUPPORTED;
            }

            IgniteSqlType ToDriverType(int16_t type)
            {
                switch (type)
                {
                case SQL_C_CHAR:
                    return IGNITE_SQL_TYPE_CHAR;

                case SQL_C_WCHAR:
                    return IGNITE_SQL_TYPE_WCHAR;

                case SQL_C_SSHORT:
                    return IGNITE_SQL_TYPE_SIGNED_SHORT;

                case SQL_C_USHORT:
                    return IGNITE_SQL_TYPE_UNSIGNED_SHORT;

                case SQL_C_SLONG:
                    return IGNITE_SQL_TYPE_SIGNED_LONG;

                case SQL_C_ULONG:
                    return IGNITE_SQL_TYPE_UNSIGNED_LONG;

                case SQL_C_FLOAT:
                    return IGNITE_SQL_TYPE_FLOAT;

                case SQL_C_DOUBLE:
                    return IGNITE_SQL_TYPE_DOUBLE;

                case SQL_C_BIT:
                    return IGNITE_SQL_TYPE_BIT;

                case SQL_C_STINYINT:
                    return IGNITE_SQL_TYPE_SIGNED_TINYINT;

                case SQL_C_UTINYINT:
                    return IGNITE_SQL_TYPE_UNSIGNED_TINYINT;

                case SQL_C_SBIGINT:
                    return IGNITE_SQL_TYPE_SIGNED_BIGINT;

                case SQL_C_UBIGINT:
                    return IGNITE_SQL_TYPE_UNSIGNED_BIGINT;

                case SQL_C_BINARY:
                    return IGNITE_SQL_TYPE_BINARY;

                case SQL_C_TYPE_DATE:
                    return IGNITE_SQL_TYPE_TDATE;

                case SQL_C_TYPE_TIME:
                    return IGNITE_SQL_TYPE_TTIME;

                case SQL_C_TYPE_TIMESTAMP:
                    return IGNITE_SQL_TYPE_TTIMESTAMP;

                case SQL_C_NUMERIC:
                    return IGNITE_SQL_TYPE_NUMERIC;

                case SQL_C_GUID:
                    return IGNITE_SQL_TYPE_GUID;

                case SQL_DEFAULT:
                    return IGNITE_SQL_TYPE_DEFAULT;

                default:
                    return IGNITE_SQL_TYPE_UNSUPPORTED;
                }
            }

        }
    }
}

