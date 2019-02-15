/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

#ifndef _IGNITE_ODBC_CONFIG_CONNECTION_INFO
#define _IGNITE_ODBC_CONFIG_CONNECTION_INFO

#include <stdint.h>

#include <map>

#include <ignite/common/common.h>
#include <ignite/odbc/common_types.h>
#include <ignite/odbc/config/configuration.h>

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
                 *
                 * @param config Configuration.
                 */
                ConnectionInfo(const Configuration& config);

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
                SqlResult::Type GetInfo(InfoType type, void* buf, short buflen, short* reslen) const;

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

                /** Configuration. */
                const Configuration& config;
            };
        }
    }
}

#endif //_IGNITE_ODBC_CONFIG_CONNECTION_INFO