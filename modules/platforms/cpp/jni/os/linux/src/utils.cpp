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
#include <cstring>

#include <pthread.h>

#include <sys/stat.h>
#include <dirent.h>
#include <dlfcn.h>
#include <unistd.h>

#include "ignite/common/utils.h"
#include "ignite/common/fixed_size_array.h"
#include "ignite/jni/utils.h"
#include "ignite/jni/java.h"

using namespace ignite::common;
using namespace ignite::jni::java;

namespace ignite
{
    namespace jni
    {
        const char* JAVA_HOME = "JAVA_HOME";
        const char* JAVA_DLL = "/jre/lib/amd64/server/libjvm.so";

        const char* IGNITE_HOME = "IGNITE_HOME";

        const char* PROBE_BIN = "/bin";
        const char* PROBE_EXAMPLES = "/examples";

        const char* IGNITE_NATIVE_TEST_CLASSPATH = "IGNITE_NATIVE_TEST_CLASSPATH";

        /** Key indicating that the thread is attached. */
        static pthread_key_t attachKey;

        /** Helper to ensure that attach key is allocated only once. */
        static pthread_once_t attachKeyInit = PTHREAD_ONCE_INIT;
        
        void DestroyAttachKey(void* key)
        {
            delete reinterpret_cast<AttachHelper*>(key);
        }
        
        void AllocateAttachKey()
        {
            pthread_key_create(&attachKey, DestroyAttachKey);
        }
		
        AttachHelper::~AttachHelper()
        {
            JniContext::Detach();
        }
        
        void AttachHelper::OnThreadAttach()
        {
            pthread_once(&attachKeyInit, AllocateAttachKey);
            
            void* val = pthread_getspecific(attachKey);
            
            if (!val)
                pthread_setspecific(attachKey, new AttachHelper());
        }

        /**
         * Helper function for GG home resolution. Checks whether certain folders
         * exist in the path. Optionally goes upwards in directory hierarchy.
         *
         * @param path Path to evaluate.
         * @param up Whether to go upwards.
         * @param res Resolved directory.
         * @return Resolution result.
         */
        bool ResolveIgniteHome0(const std::string& path, bool up, std::string& res)
        {
            struct stat pathStat;

            if (stat(path.c_str(), &pathStat) == -1 || !S_ISDIR(pathStat.st_mode))
                return false;
            
            // Remove trailing slashes, otherwise we will have an infinite loop.
            std::string path0;

            size_t last = path.find_last_not_of("/\\ ");

            if (last != std::string::npos)
                path0.assign(path, 0, last + 1);

            std::string binStr = path0 + PROBE_BIN;
            struct stat binStat;

            std::string examplesStr = path0 + PROBE_EXAMPLES;
            struct stat examplesStat;

            if (stat(binStr.c_str(), &binStat) != -1 && S_ISDIR(binStat.st_mode) &&
                stat(examplesStr.c_str(), &examplesStat) != -1 && S_ISDIR(examplesStat.st_mode))
            {
                res = path0;

                return true;
            }

            if (!up)
                return false;

            // Evaluate parent directory.
            size_t slashPos = path0.find_last_of("/");

            if (slashPos == std::string::npos)
                return false;

            std::string parent(path0, 0, slashPos);

            return ResolveIgniteHome0(parent, true, res);
        }

        /**
         * Create classpath picking JARs from the given path.
         *
         * @path Path.
         * @return Classpath;
         */
        std::string ClasspathJars(const std::string& path)
        {
            std::string res;

            DIR* dir = opendir(path.c_str());

            if (dir)
            {
                struct dirent* entry;

                while ((entry = readdir(dir)) != NULL)
                {
                    char *dot = strrchr(entry->d_name, '.');
                    if (dot && !strcmp(dot, ".jar"))
                    {
                        res.append(path);
                        res.append("/");
                        res.append(entry->d_name);
                        res.append(":");
                    }
                }

                closedir(dir);
            }

            return res;
        }

        /**
         * Create classpath picking compiled classes from the given path.
         *
         * @path Path.
         * @return Classpath;
         */
        std::string ClasspathExploded(const std::string& path, bool down)
        {
            std::string res;

            if (FileExists(path))
            {
                // 1. Append "target\classes".
                std::string classesPath = path + "/target/classes";

                if (FileExists(classesPath)) {
                    res += classesPath;
                    res += ":";
                }

                // 2. Append "target\test-classes"
                std::string testClassesPath = path + "/target/test-classes";

                if (FileExists(testClassesPath)) {
                    res += testClassesPath;
                    res += ":";
                }

                // 3. Append "target\libs"
                std::string libsPath = path + "/target/libs";

                if (FileExists(libsPath)) {
                    std::string libsCp = ClasspathJars(libsPath);
                    res += libsCp;
                }

                // 4. Do the same for child if needed.
                if (down)
                {
                    DIR* dir = opendir(path.c_str());

                    if (dir)
                    {
                        struct dirent* entry;

                        while ((entry = readdir(dir)) != NULL)
                        {
                            std::string entryPath = entry->d_name;

                            if (entryPath.compare(".") != 0 && entryPath.compare("..") != 0)
                            {
                                std::string entryFullPath = path + "/" + entryPath;

                                struct stat entryFullStat;

                                if (stat(entryFullPath.c_str(), &entryFullStat) != -1 && S_ISDIR(entryFullStat.st_mode))
                                {
                                    std::string childCp = ClasspathExploded(entryFullPath, false);

                                    res += childCp;
                                }
                            }
                        }

                        closedir(dir);
                    }
                }
            }

            return res;
        }

        std::string CreateIgniteHomeClasspath(const std::string& home, bool forceTest)
        {
            std::string res = std::string();

            // 1. Add exploded test directories.
            if (forceTest)
            {
                std::string examplesPath = home + "/examples";
                std::string examplesCp = ClasspathExploded(examplesPath, true);
                res.append(examplesCp);

                std::string modulesPath = home + "/modules";
                std::string modulesCp = ClasspathExploded(modulesPath, true);
                res.append(modulesCp);
            }

            // 2. Add regular jars from "libs" folder excluding "optional".
            std::string libsPath = home + "/libs";

            if (FileExists(libsPath))
            {
                res.append(ClasspathJars(libsPath));

                // Append inner directories.
                DIR* dir = opendir(libsPath.c_str());

                if (dir)
                {
                    struct dirent* entry;

                    while ((entry = readdir(dir)) != NULL)
                    {
                        std::string entryPath = entry->d_name;

                        if (entryPath.compare(".") != 0 && entryPath.compare("..") != 0 &&
                            entryPath.compare("optional") != 0)
                        {
                            std::string entryFullPath = libsPath;

                            entryFullPath.append("/");
                            entryFullPath.append(entryPath);

                            struct stat entryFullStat;

                            if (stat(entryFullPath.c_str(), &entryFullStat) != -1 && 
                                S_ISDIR(entryFullStat.st_mode)) 
                                res.append(ClasspathJars(entryFullPath));
                        }                                                              
                    }

                    closedir(dir);
                }
            }

            // 3. Return.
            return res;
        }

        std::string FindJvmLibrary(const std::string& path)
        {
            // If path is provided explicitly, then check only it.
            if (!path.empty() && FileExists(path))
                return path;

            std::string javaEnv;

            if (GetEnv(JAVA_HOME, javaEnv))
            {
                std::string javaDll = javaEnv + JAVA_DLL;

                if (FileExists(javaDll))
                    return javaDll;
            }

            return std::string();
        }

        bool LoadJvmLibrary(const std::string& path)
        {
            void* hnd = dlopen(path.c_str(), RTLD_LAZY);
            
            return hnd != NULL;
        }

        /**
         * Create Ignite classpath based on user input and home directory.
         *
         * @param usrCp User's classpath.
         * @param home Ignite home directory.
         * @param forceTest Whether test classpath must be used.
         * @return Classpath.
         */
        std::string CreateIgniteClasspath(const std::string& usrCp, const std::string* home, bool forceTest)
        {
            // 1. Append user classpath if it exists.
            std::string cp;

            if (!usrCp.empty())
            {
                cp.append(usrCp);

                if (*cp.rbegin() != ';')
                    cp.push_back(';');
            }

            // 2. Append home classpath if home is defined.
            if (home)
            {
                std::string homeCp = CreateIgniteHomeClasspath(*home, forceTest);

                cp.append(homeCp);
            }

            // 3. Return.
            return cp;
        }

        std::string CreateIgniteClasspath(const std::string& usrCp)
        {
            if (usrCp.empty() || *usrCp.rbegin() == ';')
                return usrCp;

            return usrCp + ';';
        }

        std::string CreateIgniteClasspath(const std::string& usrCp, const std::string& home)
        {
            // 1. Append user classpath if it exists.
            std::string cp = CreateIgniteClasspath(usrCp);

            // 2. Append home classpath
            std::string env;
            bool envFound = GetEnv(IGNITE_NATIVE_TEST_CLASSPATH, env);

            bool forceTest = envFound && env.compare("true") == 0;

            std::string homeCp = CreateIgniteHomeClasspath(home, forceTest);

            cp.append(homeCp);

            // 3. Return.
            return cp;
        }

        bool ResolveIgniteHome(const std::string& path, std::string& home)
        {
            if (!path.empty())
                // 1. Check passed argument.
                return ResolveIgniteHome0(path, false, home);

            // 2. Check environment variable.
            std::string env;

            if (GetEnv(IGNITE_HOME, env))
                return ResolveIgniteHome0(env, false, home);

            // 3. Check current work dir.
            FixedSizeArray<char> curDir(1024 * 16);

            char* res = getcwd(curDir.GetData(), curDir.GetSize());

            if (!res)
                return false;

            std::string curDirStr(curDir.GetData());

            return ResolveIgniteHome0(curDirStr, true, home);
        }
    }
}
