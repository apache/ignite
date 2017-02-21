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

#include <ignite/common/common.h>

namespace ignite
{
    namespace common
    {
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
         * @param found Whether environment variable with such name was found.
         * @return Environment variable value.
         */
        IGNITE_IMPORT_EXPORT std::string GetEnv(const std::string& name, bool& found);

        /**
         * Ensure that file on the given path exists in the system.
         *
         * @param path Path.
         * @return True if file exists, false otherwise.
         */
        IGNITE_IMPORT_EXPORT bool FileExists(const std::string& path);
    }
}

#endif //_IGNITE_COMMON_PLATFORM_UTILS