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

#ifndef _IGNITE_THIN_CLIENT_TEST_TEST_UTILS
#define _IGNITE_THIN_CLIENT_TEST_TEST_UTILS

#include <string>

#include <boost/chrono.hpp>
#include <boost/thread.hpp>

#include <ignite/ignition.h>

#define MUTE_TEST_FOR_TEAMCITY \
    if (jetbrains::teamcity::underTeamcity()) \
    { \
        BOOST_TEST_MESSAGE("Muted on TeamCity because of periodical non-critical failures"); \
        BOOST_CHECK(jetbrains::teamcity::underTeamcity()); \
        return; \
    }

namespace ignite_test
{
    /**
     * @return Test config directory path.
     */
    std::string GetTestConfigDir();

    /**
     * Initialize configuration for a node.
     *
     * Inits Ignite node configuration from specified config file.
     * Config file is searched in path specified by IGNITE_NATIVE_TEST_CPP_THIN_CONFIG_PATH
     * environmental variable.
     *
     * @param cfg Ignite config.
     * @param cfgFile Ignite node config file name without path.
     */
    void InitConfig(ignite::IgniteConfiguration& cfg, const char* cfgFile);

    /**
     * Start Ignite node.
     *
     * Starts new Ignite node with the specified name and from specified config file.
     * Config file is searched in path specified by IGNITE_NATIVE_TEST_CPP_THIN_CONFIG_PATH
     * environmental variable.
     *
     * @param cfgFile Ignite node config file name without path.
     * @param name Node name.
     * @return New node.
     */
    ignite::Ignite StartServerNode(const char* cfgFile, const char* name);

    /**
     * Start Ignite node with config path corrected for specific platform.
     *
     * @param cfgFile Ignite node config file name without path.
     * @param name Node name.
     * @return New node.
     */
    ignite::Ignite StartCrossPlatformServerNode(const char* cfgFile, const char* name);

    /**
     * Remove all the LFS artifacts.
     */
    void ClearLfs();

    /**
     * Get a number of occurrences of a given string in the specified file.
     * @param filePath File path.
     * @param line Line to find.
     * @return Number of occurrences.
     */
    size_t GetLineOccurrencesInFile(const std::string& filePath, const std::string& line);

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

#endif // _IGNITE_THIN_CLIENT_TEST_TEST_UTILS