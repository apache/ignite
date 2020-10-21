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
#ifndef _IGNITE_COMMON_PLATFORM_UTILS
#define _IGNITE_COMMON_PLATFORM_UTILS

#include <iostream>
#include <ignite/common/common.h>

namespace ignite
{
    namespace common
    {
        typedef std::basic_ostream<char, std::char_traits<char> > StdCharOutStream;

        /**
         * Convert struct tm to time_t (UTC).
         *
         * @param time Standard C type struct tm value.
         * @return Standard C type time_t value.
         */
        IGNITE_IMPORT_EXPORT time_t IgniteTimeGm(const tm& time);

        /**
         * Convert struct tm to time_t (Local time).
         *
         * @param time Standard C type struct tm value.
         * @return Standard C type time_t value.
         */
        IGNITE_IMPORT_EXPORT time_t IgniteTimeLocal(const tm& time);

        /**
         * Convert time_t to struct tm (UTC).
         *
         * @param in Standard C type time_t value.
         * @param out Standard C type struct tm value.
         * @return True on success.
         */
        IGNITE_IMPORT_EXPORT bool IgniteGmTime(time_t in, tm& out);

        /**
         * Convert time_t to struct tm (Local time).
         *
         * @param in Standard C type time_t value.
         * @param out Standard C type struct tm value.
         * @return True on success.
         */
        IGNITE_IMPORT_EXPORT bool IgniteLocalTime(time_t in, tm& out);

        /**
         * Read system environment variable taking thread-safety in count.
         *
         * @param name Environment variable name.
         * @return Environment variable value if found and empty string otherwise.
         */
        IGNITE_IMPORT_EXPORT std::string GetEnv(const std::string& name);

        /**
         * Read system environment variable taking thread-safety in count.
         *
         * @param name Environment variable name.
         * @param dflt Default value to return on fail.
         * @return Environment variable value if found and @c dflt otherwise.
         */
        IGNITE_IMPORT_EXPORT std::string GetEnv(const std::string& name, const std::string& dflt);

        /**
         * Ensure that file on the given path exists in the system.
         *
         * @param path Path.
         * @return True if file exists, false otherwise.
         */
        IGNITE_IMPORT_EXPORT bool FileExists(const std::string& path);

        /**
         * Check if the provided path is the valid directory.
         * @return @c true if the provided path is the valid directory.
         */
        IGNITE_IMPORT_EXPORT bool IsValidDirectory(const std::string& path);

        /**
         * Deletes provided filesystem element if exists.
         * @return @c true if the provided path exists.
         */
        IGNITE_IMPORT_EXPORT bool DeletePath(const std::string& path);

        /**
         * Write file separator to a stream.
         * @param ostr Stream.
         * @return The same stream for chaining.
         */
        IGNITE_IMPORT_EXPORT StdCharOutStream& Fs(StdCharOutStream& ostr);

        /**
         * Write dynamic library expansion to a stream.
         * @param ostr Stream.
         * @return The same stream for chaining.
         */
        IGNITE_IMPORT_EXPORT StdCharOutStream& Dle(StdCharOutStream& ostr);

        /**
         * Get random seed.
         *
         * @return Random seed.
         */
        IGNITE_IMPORT_EXPORT unsigned GetRandSeed();
    }
}

#endif //_IGNITE_COMMON_PLATFORM_UTILS
