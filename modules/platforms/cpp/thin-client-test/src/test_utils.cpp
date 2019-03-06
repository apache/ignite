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

#include <boost/test/unit_test.hpp>

#include <cassert>

#include <ignite/common/platform_utils.h>

#include "test_utils.h"

namespace ignite_test
{
    std::string GetTestConfigDir()
    {
        return ignite::common::GetEnv("IGNITE_NATIVE_TEST_CPP_THIN_CONFIG_PATH");
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

        cfg.springCfgPath = GetTestConfigDir().append("/").append(cfgFile);
    }

    ignite::Ignite StartServerNode(const char* cfgFile, const char* name)
    {
        using namespace ignite;

        assert(name != 0);

        IgniteConfiguration cfg;

        InitConfig(cfg, cfgFile);

        return Ignition::Start(cfg, name);
    }

    ignite::Ignite StartCrossPlatformServerNode(const char* cfgFile, const char* name)
    {
        std::string config(cfgFile);

#ifdef IGNITE_TESTS_32
        // Cutting off the ".xml" part.
        config.resize(config.size() - 4);
        config += "-32.xml";
#endif //IGNITE_TESTS_32

        return StartServerNode(config.c_str(), name);
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
