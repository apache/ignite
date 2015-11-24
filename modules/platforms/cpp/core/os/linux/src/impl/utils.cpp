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
#include <sys/stat.h>
#include <dirent.h>
#include <dlfcn.h>

#include "ignite/impl/utils.h"

namespace ignite
{
    namespace impl
    {
        namespace utils
        {
            const char* JAVA_HOME = "JAVA_HOME";
            const char* JAVA_DLL = "/jre/lib/amd64/server/libjvm.so";

            const char* IGNITE_HOME = "IGNITE_HOME";

            const char* PROBE_BIN = "/bin";
            const char* PROBE_EXAMPLES = "/examples";

            const char* IGNITE_NATIVE_TEST_CLASSPATH = "IGNITE_NATIVE_TEST_CLASSPATH";

            /**
             * Helper method to set boolean result to reference with proper NULL-check.
             *
             * @param res Result.
             * @param outRes Where to set the result.
             */
            inline void SetBoolResult(bool res, bool* outRes)
            {
                if (outRes)
                    *outRes = res;
            }

            /**
             * Check if string ends with the given ending.
             *
             * @param str String to check.
             * @param ending Ending.
             * @return Result.
             */
            inline bool StringEndsWith(const std::string& str, const std::string& ending)
            {
                if (str.length() > ending.length())
                    return str.compare(str.length() - ending.length(), ending.length(), ending) == 0;

                return false;
            }
                
            /**
             * Helper function for GG home resolution. Checks whether certain folders
             * exist in the path. Optionally goes upwards in directory hierarchy.
             *
             * @param path Path to evaluate.
             * @param up Whether to go upwards.
             * @res Resolution result.
             * @return Resolved directory.
             */
            std::string ResolveIgniteHome0(const std::string& path, bool up, bool* res)
            {
                struct stat pathStat;
                
                if (stat(path.c_str(), &pathStat) != -1 && S_ISDIR(pathStat.st_mode)) 
                {
                    // Remove trailing slashes, otherwise we will have an infinite loop.
                    std::string path0 = path;

                    while (true) {
                        char lastChar = *path0.rbegin();

                        if (lastChar == '/' || lastChar == ' ') {
                            size_t off = path0.find_last_of(lastChar);

                            path0.erase(off, 1);
                        }
                        else
                            break;
                    }

                    std::string binStr = path0 + PROBE_BIN;
                    struct stat binStat;

                    std::string examplesStr = path0 + PROBE_EXAMPLES;
                    struct stat examplesStat;

                    if (stat(binStr.c_str(), &binStat) != -1 && S_ISDIR(binStat.st_mode) &&
                        stat(examplesStr.c_str(), &examplesStat) != -1 && S_ISDIR(examplesStat.st_mode))
                    {
                        SetBoolResult(true, res);

                        return std::string(path0);
                    }

                    if (up)
                    {
                        // Evaluate parent directory.
                        size_t slashPos = path0.find_last_of("/");

                        if (slashPos != std::string::npos)
                        {
                            std::string parent = path0.substr(0, slashPos);

                            return ResolveIgniteHome0(parent, true, res);
                        }
                    }

                }

                SetBoolResult(false, res);

                return std::string();
            }

            /**
             * Create classpath picking JARs from the given path.
             *
             * @path Path.
             * @return Classpath;
             */
            std::string ClasspathJars(const std::string& path)
            {
                std::string res = std::string();

                DIR* dir = opendir(path.c_str());

                if (dir)
                {
                    struct dirent* entry;

                    while ((entry = readdir(dir)) != NULL)
                    {
                        if (strstr(entry->d_name, ".jar"))
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
                std::string res = std::string();

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

            /**
             * Helper function to create classpath based on Ignite home directory.
             *
             * @param home Home directory; expected to be valid.
             * @param forceTest Force test classpath.
             */
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

            char* CopyChars(const char* val)
            {
                if (val) {
                    size_t len = strlen(val);
                    char* dest = new char[len + 1];
                    strcpy(dest, val);
                    *(dest + len) = 0;
                    return dest;
                }
                else
                    return NULL;
            }

            void ReleaseChars(char* val)
            {
                if (val)
                    delete[] val;
            }

            std::string GetEnv(const std::string& name, bool* found)
            {
                char* val = std::getenv(name.c_str());
                
                if (val) {
                    SetBoolResult(true, found);
                    
                    return std::string(val);
                }
                else {
                    SetBoolResult(false, found);
                    
                    return std::string();
                }
            }

            bool FileExists(const std::string& path)
            {
                struct stat s;
                
                int res = stat(path.c_str(), &s);

                return res != -1;
            }

            std::string FindJvmLibrary(const std::string* path, bool* found)
            {
                SetBoolResult(true, found); // Optimistically assume that we will find it.

                if (path) {
                    // If path is provided explicitly, then check only it.
                    if (FileExists(*path))                            
                        return std::string(path->data());
                }
                else
                {
                    bool javaEnvFound;
                    std::string javaEnv = GetEnv(JAVA_HOME, &javaEnvFound);

                    if (javaEnvFound)
                    {
                        std::string javaDll = javaEnv + JAVA_DLL;

                        if (FileExists(javaDll))
                            return std::string(javaDll);
                    }
                }

                SetBoolResult(false, found);

                return std::string();
            }

            bool LoadJvmLibrary(const std::string& path)
            {
                void* hnd = dlopen(path.c_str(), RTLD_LAZY);
                
                return hnd != NULL;
            }                

            std::string ResolveIgniteHome(const std::string* path, bool* found)
            {
                if (path)
                    // 1. Check passed argument.
                    return ResolveIgniteHome0(*path, false, found);
                else
                {
                    // 2. Check environment variable.
                    bool envFound;
                    std::string env = GetEnv(IGNITE_HOME, &envFound);

                    if (envFound)
                        return ResolveIgniteHome0(env, false, found);
                }

                SetBoolResult(false, found);
                        
                return std::string();
            }

            std::string CreateIgniteClasspath(const std::string* usrCp, const std::string* home)
            {
                bool forceTest = false;

                if (home)
                {
                    bool envFound;
                    std::string env = GetEnv(IGNITE_NATIVE_TEST_CLASSPATH, &envFound);

                    forceTest = envFound && env.compare("true") == 0;
                }

                return CreateIgniteClasspath(usrCp, home, forceTest);
            }

            std::string CreateIgniteClasspath(const std::string* usrCp, const std::string* home, bool forceTest)
            {
                // 1. Append user classpath if it exists.
                std::string cp = std::string();

                if (usrCp)
                {
                    cp.append(*usrCp);

                    if (*cp.rbegin() != ':')
                        cp.append(":");
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
        }
    }
}