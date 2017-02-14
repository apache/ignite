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

#ifndef _IGNITE_ODBC_CONFIG_CONNECTION_INFO
#define _IGNITE_ODBC_CONFIG_CONNECTION_INFO

#include <stdint.h>

#include <map>

#include <ignite/common/common.h>
#include <ignite/odbc/common_types.h>

namespace ignite
{
    namespace odbc
    {
        namespace config
        {
            /**
             * Connection info.
             */
            class ConnectionInfo
            {
            public:
                /** Info type. */
                typedef unsigned short InfoType;

                /**
                 * Convert type to string containing its name.
                 * Debug function.
                 * @param type Info type.
                 * @return Null-terminated string containing types name.
                 */
                static const char* InfoTypeToString(InfoType type);

                /**
                 * Constructor.
                 */
                ConnectionInfo();

                /**
                 * Destructor.
                 */
                ~ConnectionInfo();

                /**
                 * Get info of any type.
                 * @param type Info type.
                 * @param buf Result buffer pointer.
                 * @param buflen Result buffer length.
                 * @param reslen Result value length pointer.
                 * @return True on success.
                 */
                SqlResult GetInfo(InfoType type, void* buf, short buflen, short* reslen) const;

            private:
                IGNITE_NO_COPY_ASSIGNMENT(ConnectionInfo);

                /** Associative array of string parameters. */
                typedef std::map<InfoType, std::string> StringInfoMap;

                /** Associative array of unsigned integer parameters. */
                typedef std::map<InfoType, unsigned int> UintInfoMap;

                /** Associative array of unsigned short parameters. */
                typedef std::map<InfoType, unsigned short> UshortInfoMap;

                /** String parameters. */
                StringInfoMap strParams;

                /** Integer parameters. */
                UintInfoMap intParams;

                /** Short parameters. */
                UshortInfoMap shortParams;
            };
        }
    }
}

#endif //_IGNITE_ODBC_CONFIG_CONNECTION_INFO