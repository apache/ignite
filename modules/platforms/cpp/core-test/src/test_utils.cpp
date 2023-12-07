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

#include <boost/test/unit_test.hpp>

#include <cassert>

#include "ignite/test_utils.h"

namespace ignite_test
{
    std::string GetTestConfigDir()
    {
        using namespace ignite;

        std::string cfgPath = common::GetEnv("IGNITE_NATIVE_TEST_CPP_CONFIG_PATH");

        if (!cfgPath.empty())
            return cfgPath;

        std::string home = jni::ResolveIgniteHome();

        if (home.empty())
            return home;

        std::stringstream path;

        path << home << common::Fs
             << "modules" << common::Fs
             << "platforms" << common::Fs
             << "cpp" << common::Fs
             << "core-test" << common::Fs
             << "config";

        return path.str();
    }

    void InitConfig(ignite::IgniteConfiguration& cfg, const char* cfgFile)
    {
        using namespace ignite;

        assert(cfgFile != 0);

        cfg.igniteHome = jni::ResolveIgniteHome();

        cfg.jvmOpts.push_back("-Xdebug");
        cfg.jvmOpts.push_back("-Xnoagent");
        cfg.jvmOpts.push_back("-Djava.compiler=NONE");
        cfg.jvmOpts.push_back("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005");
        cfg.jvmOpts.push_back("-XX:+HeapDumpOnOutOfMemoryError");
        cfg.jvmOpts.push_back("-Duser.timezone=GMT");
        cfg.jvmOpts.push_back("-DIGNITE_QUIET=false");
        cfg.jvmOpts.push_back("-DIGNITE_UPDATE_NOTIFIER=false");
        cfg.jvmOpts.push_back("-DIGNITE_LOG_CLASSPATH_CONTENT_ON_STARTUP=false");
        cfg.jvmOpts.push_back("-Duser.language=en");
        cfg.jvmOpts.push_back("-Dlog4j.configurationFile=" + cfg.igniteHome + "/modules/core/src/test/config/log4j2-test.xml");

        cfg.jvmClassPath = jni::CreateIgniteHomeClasspath(cfg.igniteHome, true);

#ifdef IGNITE_TESTS_32
        cfg.jvmInitMem = 256;
        cfg.jvmMaxMem = 768;
#else
        cfg.jvmInitMem = 1024;
        cfg.jvmMaxMem = 4096;
#endif
        std::string cfgDir = GetTestConfigDir();

        if (cfgDir.empty())
            throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Failed to resolve test config directory");

        std::stringstream path;

        path << cfgDir << common::Fs << cfgFile;

        cfg.springCfgPath = path.str();
    }

    ignite::Ignite StartNode(const char* cfgFile)
    {
        using namespace ignite;

        IgniteConfiguration cfg;

        InitConfig(cfg, cfgFile);

        return Ignition::Start(cfg);
    }

    ignite::Ignite StartNode(const char* cfgFile, const char* name)
    {
        using namespace ignite;

        assert(name != 0);

        IgniteConfiguration cfg;

        InitConfig(cfg, cfgFile);

        return Ignition::Start(cfg, name);
    }

    ignite::Ignite StartPlatformNode(const char* cfg, const char* name)
    {
        std::string config(cfg);

#ifdef IGNITE_TESTS_32
        // Cutting off the ".xml" part.
        config.resize(config.size() - 4);
        config += "-32.xml";
#endif //IGNITE_TESTS_32

        return StartNode(config.c_str(), name);
    }

    std::string AppendPath(const std::string& base, const std::string& toAdd)
    {
        std::stringstream stream;

        stream << base << ignite::common::Fs << toAdd;

        return stream.str();
    }

    void ClearLfs()
    {
        std::string home = ignite::jni::ResolveIgniteHome();
        std::string workDir = AppendPath(home, "work");

        ignite::common::DeletePath(workDir);
    }

    bool IsGenericError(const ignite::IgniteError& err)
    {
        return err.GetCode() == ignite::IgniteError::IGNITE_ERR_GENERIC;
    }

    bool IsTestError(const ignite::IgniteError& err)
    {
        return err.GetCode() == TEST_ERROR;
    }
}
