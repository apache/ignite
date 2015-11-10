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
#ifndef _IGNITE_COMMON_UTILS
#define _IGNITE_COMMON_UTILS

#include <cstring>
#include <string>
#include <sstream>
#include <algorithm>

#include <ignite/common/common.h>

#ifdef IGNITE_FRIEND
#   define IGNITE_FRIEND_EXPORT IGNITE_EXPORT
#else
#   define IGNITE_FRIEND_EXPORT
#endif

namespace ignite
{
    namespace common
    {
        namespace utils
        {
            /**
             * Transform string into lowercase.
             *
             * @param str String to be transformed.
             */
            inline void IntoLower(std::string& str)
            {
                std::transform(str.begin(), str.end(), str.begin(), ::tolower);
            }

            /**
             * Get lowercase version of the string.
             *
             * @param str Input string.
             * @return Lowercased version of the string.
             */
            inline std::string ToLower(const std::string& str)
            {
                std::string res(str);
                IntoLower(res);
                return res;
            }

            /**
             * Get string representation of long in decimal form.
             *
             * @param val Long value to be converted to string.
             * @return String contataining decimal representation of the value.
             */
            inline std::string LongToString(long val)
            {
                std::stringstream tmp;
                tmp << val;
                return tmp.str();
            }

            /**
             * Parse string to try and get int value.
             *
             * @param str String to be parsed.
             * @return String contataining decimal representation of the value.
             */
            inline int ParseInt(const std::string& str)
            {
                return atoi(str.c_str());
            }

            /**
             * Copy characters.
             *
             * @param val Value.
             * @return Result.
             */
            IGNITE_FRIEND_EXPORT char* CopyChars(const char* val);

            /**
             * Release characters.
             *
             * @param val Value.
             */
            IGNITE_FRIEND_EXPORT void ReleaseChars(char* val);
            
            /**
             * Read system environment variable taking thread-safety in count.
             *
             * @param name Environment variable name.
             * @param found Whether environment variable with such name was found.
             * @return Environment variable value.
             */
            IGNITE_FRIEND_EXPORT std::string GetEnv(const std::string& name, bool* found);
                                
            /**
             * Ensure that file on the given path exists in the system.
             *
             * @param path Path.
             * @return True if file exists, false otherwise.
             */
            IGNITE_FRIEND_EXPORT bool FileExists(const std::string& path);

            /**
             * Attempts to find JVM library to load it into the process later.
             * First search is performed using the passed path argument (is not NULL).
             * Then JRE_HOME is evaluated. Last, JAVA_HOME is evaluated.
             *
             * @param Explicitly defined path (optional).
             * @param found Whether library was found.
             * @return Path to the file.
             */
            IGNITE_FRIEND_EXPORT std::string FindJvmLibrary(const std::string* path, bool* found);

            /**
             * Load JVM library into the process.
             *
             * @param path Optional path to the library.
             * @return Whether load was successful.
             */
            IGNITE_FRIEND_EXPORT bool LoadJvmLibrary(const std::string& path);

            /**
             * Resolve IGNITE_HOME directory. Resolution is performed in several
             * steps:
             * 1) Check for path provided as argument.
             * 2) Check for environment variable.
             * 3) Check for current working directory.
             * Result of these 3 checks are evaluated based on existence of certain
             * predefined folders inside possible GG home. If they are found, 
             * IGNITE_HOME is considered resolved.
             *
             * @param path Optional path to evaluate.
             * @param found Whether IGNITE_HOME home was found.
             * @return Resolved GG home.
             */
            IGNITE_FRIEND_EXPORT std::string ResolveIgniteHome(const std::string* path, bool* found);

            /**
             * Create Ignite classpath based on user input and home directory.
             *
             * @param usrCp User's classpath.
             * @param home Ignite home directory.
             * @return Classpath.
             */
            IGNITE_FRIEND_EXPORT std::string CreateIgniteClasspath(const std::string* usrCp, const std::string* home);

            /**
             * Create Ignite classpath based on user input and home directory.
             *
             * @param usrCp User's classpath.
             * @param home Ignite home directory.
             * @param test Whether test classpath must be used.
             * @return Classpath.
             */
            IGNITE_FRIEND_EXPORT std::string CreateIgniteClasspath(const std::string* usrCp, const std::string* home, bool test);

            /**
             * Safe array which automatically reclaims occupied memory when out of scope.
             */
            template<typename T>
            struct IGNITE_FRIEND_EXPORT SafeArray
            {
                /** Target array. */
                T* target;

                /**
                 * Constructor.
                 */
                SafeArray(int cap)
                {
                    target = new T[cap];
                }

                /**
                 * Destructor.
                 */
                ~SafeArray()
                {
                    delete[] target;
                }

                IGNITE_NO_COPY_ASSIGNMENT(SafeArray);
            };
        }
    }
}

#endif