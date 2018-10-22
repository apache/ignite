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

#include <cassert>

#include <ignite/common/platform_utils.h>

#include "test_utils.h"

namespace ignite_test
{
    OdbcClientError GetOdbcError(SQLSMALLINT handleType, SQLHANDLE handle)
    {
        SQLCHAR sqlstate[7] = {};
        SQLINTEGER nativeCode;

        SQLCHAR message[ODBC_BUFFER_SIZE];
        SQLSMALLINT reallen = 0;

        SQLGetDiagRec(handleType, handle, 1, sqlstate, &nativeCode, message, ODBC_BUFFER_SIZE, &reallen);

        return OdbcClientError(
            std::string(reinterpret_cast<char*>(sqlstate)),
            std::string(reinterpret_cast<char*>(message), reallen));
    }

    std::string GetOdbcErrorState(SQLSMALLINT handleType, SQLHANDLE handle)
    {
        SQLCHAR sqlstate[7] = {};
        SQLINTEGER nativeCode;

        SQLCHAR message[ODBC_BUFFER_SIZE];
        SQLSMALLINT reallen = 0;

        SQLGetDiagRec(handleType, handle, 1, sqlstate, &nativeCode, message, ODBC_BUFFER_SIZE, &reallen);

        return std::string(reinterpret_cast<char*>(sqlstate));
    }

    std::string GetOdbcErrorMessage(SQLSMALLINT handleType, SQLHANDLE handle)
    {
        SQLCHAR sqlstate[7] = {};
        SQLINTEGER nativeCode;

        SQLCHAR message[ODBC_BUFFER_SIZE];
        SQLSMALLINT reallen = 0;

        SQLGetDiagRec(handleType, handle, 1, sqlstate, &nativeCode, message, ODBC_BUFFER_SIZE, &reallen);

        std::string res(reinterpret_cast<char*>(sqlstate));

        if (!res.empty())
            res.append(": ").append(reinterpret_cast<char*>(message), reallen);
        else
            res = "No results";

        return res;
    }

    std::string GetTestConfigDir()
    {
        return ignite::common::GetEnv("IGNITE_NATIVE_TEST_ODBC_CONFIG_PATH");
    }

    void InitConfig(ignite::IgniteConfiguration& cfg, const char* cfgFile)
    {
        using namespace ignite;

        assert(cfgFile != 0);

        cfg.jvmOpts.push_back("-Xdebug");
        cfg.jvmOpts.push_back("-Xnoagent");
        cfg.jvmOpts.push_back("-Djava.compiler=NONE");
        cfg.jvmOpts.push_back("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005");
        cfg.jvmOpts.push_back("-XX:+HeapDumpOnOutOfMemoryError");
        cfg.jvmOpts.push_back("-Duser.timezone=GMT");
        cfg.jvmOpts.push_back("-DIGNITE_QUIET=false");
        cfg.jvmOpts.push_back("-DIGNITE_CONSOLE_APPENDER=false");
        cfg.jvmOpts.push_back("-DIGNITE_UPDATE_NOTIFIER=false");
        cfg.jvmOpts.push_back("-Duser.language=en");
        // Un-comment to debug SSL
        //cfg.jvmOpts.push_back("-Djavax.net.debug=ssl");

        cfg.igniteHome = jni::ResolveIgniteHome();
        cfg.jvmClassPath = jni::CreateIgniteHomeClasspath(cfg.igniteHome, true);

#ifdef IGNITE_TESTS_32
        cfg.jvmInitMem = 256;
        cfg.jvmMaxMem = 768;
#else
        cfg.jvmInitMem = 1024;
        cfg.jvmMaxMem = 4096;
#endif

        char* cfgPath = getenv("IGNITE_NATIVE_TEST_ODBC_CONFIG_PATH");

        assert(cfgPath != 0);

        cfg.springCfgPath = std::string(cfgPath).append("/").append(cfgFile);
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
}
