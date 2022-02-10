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

#ifndef _IGNITE_CORE_TEST_TEST_UTILS
#define _IGNITE_CORE_TEST_TEST_UTILS

#include <boost/chrono.hpp>
#include <boost/thread.hpp>

#include "ignite/ignition.h"

#define MUTE_TEST_FOR_TEAMCITY                                              \
    if (jetbrains::teamcity::underTeamcity()) {                             \
        BOOST_TEST_MESSAGE("Muted on TeamCity because of "                  \
            "periodical non-critical failures");                            \
        BOOST_CHECK(jetbrains::teamcity::underTeamcity());                  \
        return;                                                             \
    }

#define WITH_STABLE_TOPOLOGY_BEGIN(node)                                    \
    int64_t __topVer = node.GetCluster().GetTopologyVersion();              \
    while (true) {

#define WITH_STABLE_TOPOLOGY_END break; }

#define CHECK_TOPOLOGY_STABLE(node)                                         \
    {                                                                       \
        int64_t __topVer0 = node.GetCluster().GetTopologyVersion();         \
        if (__topVer != __topVer0) {                                        \
            __topVer = __topVer0;                                           \
            continue;                                                       \
        }                                                                   \
    }

#define WITH_RETRY_BEGIN                                                    \
    int retries = 0;                                                        \
    while (true) {

#define WITH_RETRY_END break; }

#define CHECK_EQUAL_OR_RETRY(expected, actual)                              \
    if ((retries < RETRIES_FOR_STABLE_TOPOLOGY) && (expected != actual)) {  \
        ++retries;                                                          \
        continue;                                                           \
    }                                                                       \
    BOOST_CHECK_EQUAL(expected, actual)



namespace ignite_test
{
    enum
    {
        TEST_ERROR = 424242
    };

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

    /**
     * Start node with the config for the current platform.
     *
     * @param cfg Basic config path. Changed to platform config if needed.
     * @param name Instance name.
     */
    ignite::Ignite StartPlatformNode(const char* cfg, const char* name);

    /**
     * Remove all the LFS artifacts.
     */
    void ClearLfs();

    /**
     * Check if the error is generic.
     *
     * @param err Error.
     * @return True if the error is generic.
     */
    bool IsGenericError(const ignite::IgniteError& err);

    /**
     * Check if the error is generic.
     *
     * @param err Error.
     * @return True if the error is generic.
     */
    bool IsTestError(const ignite::IgniteError& err);

    /**
     * Make test error.
     *
     * @return Test error.
     */
    inline ignite::IgniteError MakeTestError()
    {
        return ignite::IgniteError(TEST_ERROR, "Test error");
    }

    /**
     * Wait for condition.
     * @tparam T Type of condition function.
     * @param func Function that should check for condition and return true once it's performed.
     * @param timeout Timeout to wait.
     * @return True if condition was met, false if timeout has been reached.
     */
    template<typename F>
    bool WaitForCondition(F func, int32_t timeout)
    {
        using namespace boost::chrono;

        const int32_t span = 200;

        steady_clock::time_point begin = steady_clock::now();

        while (!func())
        {
            boost::this_thread::sleep_for(milliseconds(span));

            if (timeout && duration_cast<milliseconds>(steady_clock::now() - begin).count() >= timeout)
                return func();
        }

        return true;
    }
}

#endif // _IGNITE_CORE_TEST_TEST_UTILS
