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

#ifndef _IGNITE_ODBC_TEST_TEST_UTILS
#define _IGNITE_ODBC_TEST_TEST_UTILS

#ifdef _WIN32
#   include <windows.h>
#endif

#include <sql.h>
#include <sqlext.h>

#include <string>

#include "ignite/ignition.h"

#define ODBC_FAIL_ON_ERROR(ret, type, handle)                       \
    if (!SQL_SUCCEEDED(ret))                                        \
    {                                                               \
        Ignition::StopAll(true);                                    \
        BOOST_FAIL(ignite_test::GetOdbcErrorMessage(type, handle)); \
    }


namespace ignite_test
{
    /** Read buffer size. */
    enum { ODBC_BUFFER_SIZE = 1024 };

    /**
     * Extract error message.
     *
     * @param handleType Type of the handle.
     * @param handle Handle.
     * @return Error message.
     */
    std::string GetOdbcErrorMessage(SQLSMALLINT handleType, SQLHANDLE handle);

    /**
     * Initialize configuration for a node.
     *
     * Inits Ignite node configuration from specified config file.
     * Config file is searched in path specified by IGNITE_NATIVE_TEST_CPP_CONFIG_PATH
     * environmental variable.
     *
     * @param cfg Ignite config.
     * @param cfgFile Ignite node config file name without path.
     */
    void InitConfig(ignite::IgniteConfiguration& cfg, const char* cfgFile);

    /**
     * Start Ignite node.
     *
     * Starts new Ignite node from specified config file.
     * Config file is searched in path specified by IGNITE_NATIVE_TEST_CPP_CONFIG_PATH
     * environmental variable.
     *
     * @param cfgFile Ignite node config file name without path.
     * @return New node.
     */
    ignite::Ignite StartNode(const char* cfgFile);

    /**
     * Start Ignite node.
     *
     * Starts new Ignite node with the specified name and from specified config file.
     * Config file is searched in path specified by IGNITE_NATIVE_TEST_CPP_CONFIG_PATH
     * environmental variable.
     *
     * @param cfgFile Ignite node config file name without path.
     * @param name Node name.
     * @return New node.
     */
    ignite::Ignite StartNode(const char* cfgFile, const char* name);
}

#endif // _IGNITE_ODBC_TEST_TEST_UTILS