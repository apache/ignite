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

#include <algorithm>
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
        const char* JAVA_DLL1 = "/jre/lib/amd64/server/libjvm.so";
        const char* JAVA_DLL2 = "/lib/server/libjvm.so";
        const char* JAVA_DLL_DARWIN = "libjvm.dylib";

        const char* IGNITE_HOME = "IGNITE_HOME";

        const char* IGNITE_NATIVE_TEST_CLASSPATH = "IGNITE_NATIVE_TEST_CLASSPATH";

        /** Excluded modules from test classpath. */
        const char* TEST_EXCLUDED_MODULES[] = { "rest-http" };

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
         * Checks if the path looks like binary release home directory.
         * Internally checks for presence of some directories, that are
         * @return @c true if the path looks like binary release home directory.
         */
        bool LooksLikeBinaryReleaseHome(const std::string& path)
        {
            static const char* PROBE_CORE_LIB = "/libs/ignite-core*.jar";

            std::string coreLibProbe = path + PROBE_CORE_LIB;

            return FileExists(coreLibProbe);
        }

        /**
         * Checks if the path looks like source release home directory.
         * Internally checks for presence of core source directory.
         * @return @c true if the path looks like binary release home directory.
         */
        bool LooksLikeSourceReleaseHome(const std::string& path)
        {
            static const char* PROBE_CORE_SOURCE = "/modules/core/src/main/java/org/apache/ignite";

            std::string coreSourcePath = path + PROBE_CORE_SOURCE;

            return IsValidDirectory(coreSourcePath);
        }

        /**
         * Helper function for Ignite home resolution.
         * Goes upwards in directory hierarchy and checks whether certain
         * folders exist in the path.
         *
         * @param path Path to evaluate.
         * @return res Resolved directory. Empty string if not found.
         */
        std::string ResolveIgniteHome0(const std::string& path)
        {
            if (!IsValidDirectory(path))
                return std::string();

            // Remove trailing slashes, otherwise we will have an infinite loop.
            size_t last = path.find_last_not_of("/ ");

            if (last == std::string::npos)
                return std::string();

            std::string path0(path, 0, last + 1);

            if (LooksLikeBinaryReleaseHome(path0) || LooksLikeSourceReleaseHome(path0))
                return path0;

            // Evaluate parent directory.
            size_t slashPos = path0.find_last_of("/");

            if (slashPos == std::string::npos)
                return std::string();

            std::string parent(path0, 0, slashPos);

            return ResolveIgniteHome0(parent);
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
         * Check if path corresponds to excluded module.
         *
         * @path Path.
         * @return True if path should be excluded.
         */
        bool IsExcludedModule(const std::string& path) {
            std::string lower_path = path;
            std::transform(path.begin(), path.end(), lower_path.begin(), ::tolower);

            for (size_t i = 0; i < sizeof(TEST_EXCLUDED_MODULES) / sizeof(char*); i++) {
                if (lower_path.find(TEST_EXCLUDED_MODULES[i]) != std::string::npos)
                    return true;
            }

            return false;
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

            if (FileExists(path) && !IsExcludedModule(path))
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
#ifdef __APPLE__
            return JAVA_DLL_DARWIN;
#else
            // If path is provided explicitly, then check only it.
            if (!path.empty() && FileExists(path))
                return path;

            std::string javaEnv = GetEnv(JAVA_HOME);

            if (!javaEnv.empty())
            {
                std::string javaDll = javaEnv + JAVA_DLL1;

                if (FileExists(javaDll))
                    return javaDll;

                javaDll = javaEnv + JAVA_DLL2;

                if (FileExists(javaDll))
                    return javaDll;
            }

            return std::string();
#endif
        }

        bool LoadJvmLibrary(const std::string& path)
        {
#ifdef __APPLE__
            return RTLD_DEFAULT;
#else
            void* hnd = dlopen(path.c_str(), RTLD_LAZY);
            
            return hnd != NULL;
#endif
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

        /**
         * Adds semicolon at the end of the path if needed.
         * @param usrCp Classpath provided by user.
         * @return Normalized classpath.
         */
        std::string NormalizeClasspath(const std::string& usrCp)
        {
            if (usrCp.empty() || *usrCp.rbegin() == ';')
                return usrCp;

            return usrCp + ';';
        }

        std::string CreateIgniteClasspath(const std::string& usrCp, const std::string& home)
        {
            // 1. Append user classpath if it exists.
            std::string cp = NormalizeClasspath(usrCp);

            // 2. Append home classpath
            if (!home.empty())
            {
                std::string env = GetEnv(IGNITE_NATIVE_TEST_CLASSPATH, "false");

                bool forceTest = ToLower(env) == "true";

                std::string homeCp = CreateIgniteHomeClasspath(home, forceTest);

                cp.append(homeCp);
            }

            // 3. Return.
            return cp;
        }

        std::string ResolveIgniteHome(const std::string& path)
        {
            // 1. Check passed argument.
            if (IsValidDirectory(path))
                return path;

            // 2. Check environment variable.
            std::string home = GetEnv(IGNITE_HOME);

            if (IsValidDirectory(home))
                return home;

            // 3. Check current work dir.
            FixedSizeArray<char> curDir(1024 * 16);

            char* res = getcwd(curDir.GetData(), curDir.GetSize());

            if (!res)
                return std::string();

            std::string curDirStr(curDir.GetData());

            return ResolveIgniteHome0(curDirStr);
        }
    }
}
