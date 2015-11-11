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

#include "type_traits.h"

namespace ignite
{
    namespace odbc
    {
        namespace type_traits
        {
            bool IsApplicationTypeSupported(uint16_t type)
            {
                switch (type)
                {
                    case SQL_C_CHAR:
                    case SQL_C_WCHAR:
                    case SQL_C_SSHORT:
                    case SQL_C_USHORT:
                    case SQL_C_SLONG:
                    case SQL_C_ULONG:
                    case SQL_C_FLOAT:
                    case SQL_C_DOUBLE:
                    case SQL_C_BIT:
                    case SQL_C_STINYINT:
                    case SQL_C_UTINYINT:
                    case SQL_C_SBIGINT:
                    case SQL_C_UBIGINT:
                    case SQL_C_BINARY:
                    case SQL_C_TYPE_DATE:
                    case SQL_C_TYPE_TIME:
                    case SQL_C_TYPE_TIMESTAMP:
                    case SQL_C_NUMERIC:
                    case SQL_C_GUID:
                        return true;
                    default:
                        return false;
                }
            }

        }
    }
}

