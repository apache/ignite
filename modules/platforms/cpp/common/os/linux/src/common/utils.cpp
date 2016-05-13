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
 
#include <time.h>

#include <sys/stat.h>
#include <dirent.h>
#include <dlfcn.h>

#include <ignite/common/utils.h>

namespace ignite
{
    namespace common
    {
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

        time_t IgniteTimeGm(const tm& time)
        {
            tm tmc = time;

            return timegm(&tmc);
        }

        time_t IgniteTimeLocal(const tm& time)
        {
            tm tmc = time;

            return mktime(&tmc);
        }

        bool IgniteGmTime(time_t in, tm& out)
        {
            return gmtime_r(&in, &out) != NULL;
        }

        bool IgniteLocalTime(time_t in, tm& out)
        {
            return localtime_r(&in, &out) == 0;
        }

        int LeadingZeroesForOctet(int8_t octet) {
            if (octet == 0)
                return 8;

            int zeroes = 1;

            if (octet >> 4 == 0) {
                zeroes += 4;
                octet <<= 4;
            }

            if (octet >> 6 == 0) {
                zeroes += 2;
                octet <<= 2;
            }

            zeroes -= octet >> 7;

            return zeroes;
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

        std::string GetEnv(const std::string& name, bool& found)
        {
            char* val = std::getenv(name.c_str());
            
            if (val)
            {
                found = true;
                
                return std::string(val);
            }
            else
            {
                found = false;
                
                return std::string();
            }
        }

        bool FileExists(const std::string& path)
        {
            struct stat s;
            
            int res = stat(path.c_str(), &s);

            return res != -1;
        }
    }
}
