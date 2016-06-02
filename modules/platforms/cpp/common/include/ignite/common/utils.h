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

#include <stdint.h>

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
        /**
         * Replace all alphabetic symbols of the string with their lowercase
         * versions.
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
         * Copy characters.
         *
         * @param val Value.
         * @return Result.
         */
        IGNITE_IMPORT_EXPORT char* CopyChars(const char* val);

        /**
         * Release characters.
         *
         * @param val Value.
         */
        IGNITE_IMPORT_EXPORT void ReleaseChars(char* val);

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

        /**
         * Casts value of one type to another type, using stringstream.
         *
         * @param val Input value.
         * @param res Resulted value.
         */
        template<typename T1, typename T2>
        void LexicalCast(const T2& val, T1& res)
        {
            std::stringstream converter;

            converter << val;
            converter >> res;
        }

        /**
         * Casts value of one type to another type, using stringstream.
         *
         * @param val Input value.
         * @return Resulted value.
         */
        template<typename T1, typename T2>
        T1 LexicalCast(const T2& val)
        {
            T1 res;

            LexicalCast<T1, T2>(val, res);

            return res;
        }
    }
}

#endif //_IGNITE_COMMON_UTILS