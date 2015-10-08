/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License" << std::endl; you may not use this file except in compliance with
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

#include <iostream>
#include <algorithm>
#include <list>
#include <string>
#include <iterator>

#include "ignite/ignite_configuration.h"
#include "ignite/ignition.h"

typedef std::list<std::string> StringList;
typedef std::set<std::string> StringSet;

void IntoLower(std::string& str)
{
    std::transform(str.begin(), str.end(), str.begin(), ::tolower);
}

std::string ToLower(const std::string& str)
{
    std::string res(str);
    IntoLower(res);
    return res;
}

std::string LongToString(long val)
{
    std::stringstream tmp;
    tmp << val;
    return tmp.str();
}

int ParseInt(const std::string& str)
{
    return atoi(str.c_str());
}

namespace config
{
    /** Command line argument: Ignite home. */
    const std::string CmdIgniteHome = ToLower("-IgniteHome=");
    
    /** Command line argument: Spring config URL. */
    const std::string CmdSpringCfgUrl = ToLower("SpringConfigUrl=");

    /** Command line argument: Path to JVM dll. */
    const std::string CmdJvmLib = ToLower("JvmLibPath=");

    /** Command line argument: JVM classpath. */
    const std::string CmdJvmClasspath = ToLower("JvmClasspath=");

    /** Command line argument: suppress warnings flag. */
    const std::string CmdSuppressWarn = ToLower("SuppressWarnings=");

    /** Command line argument: JVM option prefix. */
    const std::string CmdJvmOpt = ToLower("J");

    /** Command line argument: JvmInitialMemoryMB. */
    const std::string CmdJvmMinMem = ToLower("JvmInitialMemoryMB=");

    /** Command line argument: JvmMaxMemoryMB. */
    const std::string CmdJvmMaxMem = ToLower("JvmMaxMemoryMB=");

    /**
     * Convert configuration to arguments.
     */
    void ToArgs(const ignite::IgniteConfiguration& cfg, StringList& args)
    {
        if (!cfg.igniteHome.empty())
            args.push_back(CmdIgniteHome + cfg.igniteHome);

        if (cfg.springCfgPath.empty())
            args.push_back(CmdSpringCfgUrl + cfg.springCfgPath);

        if (cfg.jvmLibPath.empty())
            args.push_back(CmdJvmLib + cfg.jvmLibPath);

        if (cfg.jvmClassPath.empty())
            args.push_back(CmdJvmClasspath + cfg.jvmClassPath);

        if (cfg.jvmOpts.empty())
        {
            for (StringList::const_iterator i = cfg.jvmOpts.begin(); i != cfg.jvmOpts.end(); ++i)
                args.push_back(CmdJvmOpt + *i);
        }

        args.push_back(CmdJvmMinMem + LongToString(cfg.jvmInitMem));
        args.push_back(CmdJvmMaxMem + LongToString(cfg.jvmMaxMem));
    }

    void Configure(ignite::IgniteConfiguration& cfg, const StringList& src)
    {
        StringList jvmOpts;

        for (StringList::const_iterator i = src.begin(); i != src.end(); ++i)
        {
            const std::string& arg = *i;

            std::string argLow = ToLower(arg);

            if (argLow.find(CmdIgniteHome) == 0)
                cfg.igniteHome = arg.substr(CmdIgniteHome.size());
            else if (argLow.find(CmdSpringCfgUrl) == 0)
                cfg.springCfgPath = arg.substr(CmdSpringCfgUrl.size());
            else if (argLow.find(CmdJvmLib) == 0)
                cfg.jvmLibPath = arg.substr(CmdJvmLib.size());
            else if (argLow.find(CmdJvmClasspath) == 0)
                cfg.jvmClassPath = arg.substr(CmdJvmClasspath.size());
            else if (argLow.find(CmdJvmMinMem) == 0)
                cfg.jvmInitMem = ParseInt(arg.substr(CmdJvmMinMem.size()));
            else if (argLow.find(CmdJvmMaxMem) == 0)
                cfg.jvmMaxMem = ParseInt(arg.substr(CmdJvmMaxMem.size()));
            else if (argLow.find(CmdJvmOpt) == 0)
                jvmOpts.push_back(arg.substr(CmdJvmOpt.size()));
        }

        if (!jvmOpts.empty())
        {
            if (!cfg.jvmOpts.empty())
                cfg.jvmOpts.swap(jvmOpts);
            else
                std::copy(jvmOpts.begin(), jvmOpts.end(), std::back_insert_iterator<StringList>(cfg.jvmOpts));
        }
    }

   /**
    * Convert arguments to configuration.
    *
    * @param args Arguments.
    * @return Configuration.
    */
    void FromArgs(const StringList& args, ignite::IgniteConfiguration& cfg)
    {
        Configure(cfg, args);
    }
}


/**
* Prints help to standard output.
*/
void PrintHelp()
{
    std::cout << "Usage: ignite [-options]" << std::endl;
    std::cout << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "\t-IgniteHome            path to Ignite installation directory (if not provided IGNITE_HOME environment variable is used)" << std::endl;
    std::cout << "\t-springConfigUrl       path to spring configuration file (if not provided \"config/default-config.xml\" is used)" << std::endl;
    std::cout << "\t-jvmLibPath            path to JVM library (if not provided JAVA_HOME environment variable is used)" << std::endl;
    std::cout << "\t-jvmClasspath          classpath passed to JVM (enlist additional jar files here)" << std::endl;
    std::cout << "\t-J<javaOption>         JVM options passed to created JVM" << std::endl;
    std::cout << "\t-jvmInitialMemoryMB    Initial Java heap size, in megabytes. Maps to -Xms Java parameter. Defaults to 512." << std::endl;
    std::cout << "\t-jvmMaxMemoryMB        Maximum Java heap size, in megabytes. Maps to -Xmx Java parameter. Defaults to 1024." << std::endl;
    std::cout << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "\tignite -J-Xms1024m -J-Xmx1024m -springConfigUrl=C:/woer/gg-test/my-test-gg-confignative.xml" << std::endl;
    std::cout << "\tignite -IgniteHome=c:/apache-ignite -jvmClasspath=libs/myLib1.jar;libs/myLib2.jar" << std::endl;
    std::cout << "\tignite -jvmInitialMemoryMB=1024 -jvmMaxMemoryMB=4096" << std::endl;
    std::cout << std::endl;
}

/**
 * Application entry point.
 */
int main(int argc, const char* argv[])
{
    // Help commands.
    StringSet Help;
    Help.insert("/help");
    Help.insert("-help");
    Help.insert("--help");

    StringList args;
    std::copy(argv + 1, argv + argc, std::back_insert_iterator<StringList>(args));
    
    try
    {
        // Check for special cases.
        if (argc > 1)
        {
            std::string first = ToLower(args.front());
            if (Help.find(first) != Help.end())
            {
                PrintHelp();

                return 0;
            }
        }

        // Pick application configuration.
        ignite::IgniteConfiguration cfg;

        // Pick command line arguments.
        config::Configure(cfg, args);

        ignite::Ignite ignite = ignite::Ignition::Start(cfg);

        ignite::impl::IgniteImpl *igniteImpl = ignite::impl::IgniteImpl::GetFromProxy(ignite);

        if (igniteImpl)
        {
            igniteImpl->DestroyJvm();
        }
    }
    catch (ignite::IgniteError& e)
    {
        std::cout << "ERROR: " << e.GetText() << std::endl;

        return -1;
    }
    catch (std::exception& e)
    {
        std::cout << "ERROR: " << e.what() << std::endl;

        return -2;
    }
    return 0;
}

