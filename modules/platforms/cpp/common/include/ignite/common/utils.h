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

#include <string>
#include <sstream>
#include <algorithm>

namespace ignite
{
    namespace common
    {
        namespace util
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
        }
    }
}

#endif