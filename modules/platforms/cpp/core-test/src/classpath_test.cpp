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

#include <string>

#include <boost/test/unit_test.hpp>

#include "ignite/jni/utils.h"

using namespace ignite::jni;

namespace
{
#ifdef _WIN32
    const char PATH_SEP = '\\';
#else
    const char PATH_SEP = '/';
#endif

    /**
     * Path to the compiled classes of a nested module, in the form the classpath builder produces.
     *
     * @param home Ignite home.
     * @param group Grouping directory under "modules", e.g. "thin-client".
     * @param module Nested module, e.g. "api".
     * @return Path to the module classes.
     */
    std::string NestedModuleClasses(const std::string& home, const std::string& group, const std::string& module)
    {
        std::string res(home);

        res += PATH_SEP; res += "modules";
        res += PATH_SEP; res += group;
        res += PATH_SEP; res += module;
        res += PATH_SEP; res += "target";
        res += PATH_SEP; res += "classes";

        return res;
    }

    /**
     * Check that the given path is a part of the classpath.
     *
     * @param classpath Classpath.
     * @param path Path.
     */
    void CheckOnClasspath(const std::string& classpath, const std::string& path)
    {
        BOOST_CHECK_MESSAGE(classpath.find(path) != std::string::npos, path + " is missing from the classpath");
    }
}

BOOST_AUTO_TEST_SUITE(ClasspathTestSuite)

/**
 * Nested maven modules sit one level deeper than the "modules" scan reaches, so every grouping directory
 * needs its own recursive pass. Missing classes are only discovered once a lazily resolved reference is
 * hit: ignite-core reaches the thin client from Ignition.startClient() and from management commands, so a
 * node starts up just fine without them.
 */
BOOST_AUTO_TEST_CASE(TestNestedModulesOnDevClasspath)
{
    std::string home = ResolveIgniteHome();

    BOOST_REQUIRE(!home.empty());

    std::string classpath = CreateIgniteHomeClasspath(home, true);

    CheckOnClasspath(classpath, NestedModuleClasses(home, "binary", "api"));
    CheckOnClasspath(classpath, NestedModuleClasses(home, "binary", "impl"));
    CheckOnClasspath(classpath, NestedModuleClasses(home, "thin-client", "api"));
    CheckOnClasspath(classpath, NestedModuleClasses(home, "thin-client", "impl"));
}

BOOST_AUTO_TEST_SUITE_END()
