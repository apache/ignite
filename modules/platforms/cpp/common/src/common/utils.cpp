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

        void StripSurroundingWhitespaces(std::string& str)
        {
            std::string::size_type newBegin = 0;
            while (newBegin < str.size() && ::isspace(str[newBegin]))
                ++newBegin;

            if (newBegin == str.size())
            {
                str.clear();

                return;
            }

            std::string::size_type newEnd = str.size() - 1;
            while (::isspace(str[newEnd]))
                --newEnd;

            str.assign(str, newBegin, (newEnd - newBegin) + 1);
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

            return 0;
        }

        void ReleaseChars(char* val)
        {
            // Its OK to delete null-pointer.
            delete[] val;
        }

        uint32_t ToBigEndian(uint32_t value)
        {
            // The answer is 42
            static const int num = 42;
            static const bool isLittleEndian = (*reinterpret_cast<const char*>(&num) == num);

            if (isLittleEndian)
                return ((value & 0xFF) << 24) | (((value >> 8) & 0xFF) << 16) | (((value >> 16) & 0xFF) << 8) | ((value >> 24) & 0xFF);

            return value;
        }

        IGNITE_FRIEND_EXPORT Date MakeDateGmt(int year, int month, int day, int hour,
            int min, int sec)
        {
            tm date;

            std::memset(&date, 0, sizeof(date));

            date.tm_year = year - 1900;
            date.tm_mon = month - 1;
            date.tm_mday = day;
            date.tm_hour = hour;
            date.tm_min = min;
            date.tm_sec = sec;

            return CTmToDate(date);
        }

        IGNITE_FRIEND_EXPORT Date MakeDateLocal(int year, int month, int day, int hour,
            int min, int sec)
        {
            tm date;

            std::memset(&date, 0, sizeof(date));

            date.tm_year = year - 1900;
            date.tm_mon = month - 1;
            date.tm_mday = day;
            date.tm_hour = hour;
            date.tm_min = min;
            date.tm_sec = sec;

            time_t localTime = common::IgniteTimeLocal(date);

            return CTimeToDate(localTime);
        }

        IGNITE_FRIEND_EXPORT Time MakeTimeGmt(int hour, int min, int sec)
        {
            tm date;

            std::memset(&date, 0, sizeof(date));

            date.tm_year = 70;
            date.tm_mon = 0;
            date.tm_mday = 1;
            date.tm_hour = hour;
            date.tm_min = min;
            date.tm_sec = sec;

            return CTmToTime(date);
        }

        IGNITE_FRIEND_EXPORT Time MakeTimeLocal(int hour, int min, int sec)
        {
            tm date;

            std::memset(&date, 0, sizeof(date));

            date.tm_year = 70;
            date.tm_mon = 0;
            date.tm_mday = 1;
            date.tm_hour = hour;
            date.tm_min = min;
            date.tm_sec = sec;

            time_t localTime = common::IgniteTimeLocal(date);

            return CTimeToTime(localTime);
        }

        IGNITE_FRIEND_EXPORT Timestamp MakeTimestampGmt(int year, int month, int day,
            int hour, int min, int sec, long ns)
        {
            tm date;

            std::memset(&date, 0, sizeof(date));

            date.tm_year = year - 1900;
            date.tm_mon = month - 1;
            date.tm_mday = day;
            date.tm_hour = hour;
            date.tm_min = min;
            date.tm_sec = sec;

            return CTmToTimestamp(date, ns);
        }

        IGNITE_FRIEND_EXPORT Timestamp MakeTimestampLocal(int year, int month, int day,
            int hour, int min, int sec, long ns)
        {
            tm date;

            std::memset(&date, 0, sizeof(date));

            date.tm_year = year - 1900;
            date.tm_mon = month - 1;
            date.tm_mday = day;
            date.tm_hour = hour;
            date.tm_min = min;
            date.tm_sec = sec;

            time_t localTime = common::IgniteTimeLocal(date);

            return CTimeToTimestamp(localTime, ns);
        }

        IGNITE_IMPORT_EXPORT std::string GetDynamicLibraryName(const char* name)
        {
            std::stringstream libNameBuffer;

            libNameBuffer << name << Dle;

            return libNameBuffer.str();
        }

        IGNITE_IMPORT_EXPORT bool AllDigits(const std::string &val)
        {
            std::string::const_iterator i = val.begin();

            while (i != val.end() && isdigit(*i))
                ++i;

            return i == val.end();
        }

    }
}
