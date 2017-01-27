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
#include <ignite/common/platform_utils.h>

#include <ignite/date.h>
#include <ignite/timestamp.h>

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
         * Strips leading and trailing whitespaces from string.
         *
         * @param str String to be transformed.
         */
        IGNITE_IMPORT_EXPORT void StripSurroundingWhitespaces(std::string& str);

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
        IGNITE_IMPORT_EXPORT char* CopyChars(const char* val);

        /**
         * Release characters.
         *
         * @param val Value.
         */
        IGNITE_IMPORT_EXPORT void ReleaseChars(char* val);

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

        /**
         * Check if the predicate returns true for all the elements of the
         * sequence.
         *
         * @return True if the predicate returns true for all the elements
         *     of the sequence and false otherwise.
         */
        template<typename Iter, typename Pred>
        bool AllOf(Iter begin, Iter end, Pred pred)
        {
            Iter i = begin;

            while (i != end && pred(*i))
                ++i;

            return i == end;
        }

        /**
         * Converts 32-bit integer to big endian format
         *
         * @param value Input value
         * @return Resulting value
         */
        IGNITE_IMPORT_EXPORT uint32_t ToBigEndian(uint32_t value);

        /**
         * Convert Date type to standard C type time_t.
         *
         * @param date Date type value.
         * @return Corresponding value of time_t.
         */
        inline time_t DateToCTime(const Date& date)
        {
            return static_cast<time_t>(date.GetSeconds());
        }

        /**
         * Convert Timestamp type to standard C type time_t.
         *
         * @param ts Timestamp type value.
         * @return Corresponding value of time_t.
         */
        inline time_t TimestampToCTime(const Timestamp& ts)
        {
            return static_cast<time_t>(ts.GetSeconds());
        }

        /**
         * Convert Date type to standard C type time_t.
         *
         * @param date Date type value.
         * @param ctime Corresponding value of struct tm.
         * @return True on success.
         */
        inline bool DateToCTm(const Date& date, tm& ctime)
        {
            time_t tmt = DateToCTime(date);

            return common::IgniteGmTime(tmt, ctime);
        }

        /**
         * Convert Timestamp type to standard C type struct tm.
         *
         * @param ts Timestamp type value.
         * @param ctime Corresponding value of struct tm.
         * @return True on success.
         */
        inline bool TimestampToCTm(const Timestamp& ts, tm& ctime)
        {
            time_t tmt = TimestampToCTime(ts);

            return common::IgniteGmTime(tmt, ctime);
        }

        /**
         * Convert standard C type time_t to Date struct tm.
         *
         * @param ctime Standard C type time_t.
         * @return Corresponding value of Date.
         */
        inline Date CTimeToDate(time_t ctime)
        {
            return Date(ctime * 1000);
        }

        /**
         * Convert standard C type time_t to Timestamp type.
         *
         * @param ctime Standard C type time_t.
         * @param ns Nanoseconds second fraction.
         * @return Corresponding value of Timestamp.
         */
        inline Timestamp CTimeToTimestamp(time_t ctime, int32_t ns)
        {
            return Timestamp(ctime, ns);
        }

        /**
         * Convert standard C type struct tm to Date type.
         *
         * @param ctime Standard C type struct tm.
         * @return Corresponding value of Date.
         */
        inline Date CTmToDate(const tm& ctime)
        {
            time_t time = common::IgniteTimeGm(ctime);

            return CTimeToDate(time);
        }

        /**
         * Convert standard C type struct tm to Timestamp type.
         *
         * @param ctime Standard C type struct tm.
         * @param ns Nanoseconds second fraction.
         * @return Corresponding value of Timestamp.
         */
        inline Timestamp CTmToTimestamp(const tm& ctime, int32_t ns)
        {
            time_t time = common::IgniteTimeGm(ctime);

            return CTimeToTimestamp(time, ns);
        }

        /**
         * Make Date in human understandable way.
         *
         * Created Date uses GMT timezone.
         *
         * @param year Year.
         * @param month Month.
         * @param day Day.
         * @param hour Hour.
         * @param min Min.
         * @param sec Sec.
         * @return Date.
         */
        Date MakeDateGmt(int year = 1900, int month = 1,
            int day = 1, int hour = 0, int min = 0, int sec = 0);

        /**
         * Make Date in human understandable way.
         *
         * Created Date uses local timezone.
         *
         * @param year Year.
         * @param month Month.
         * @param day Day.
         * @param hour Hour.
         * @param min Min.
         * @param sec Sec.
         * @return Date.
         */
        Date MakeDateLocal(int year = 1900, int month = 1,
            int day = 1, int hour = 0, int min = 0, int sec = 0);

        /**
         * Make Date in human understandable way.
         *
         * Created Timestamp uses GMT timezone.
         *
         * @param year Year.
         * @param month Month.
         * @param day Day.
         * @param hour Hour.
         * @param min Minute.
         * @param sec Second.
         * @param ns Nanosecond.
         * @return Timestamp.
         */
        Timestamp MakeTimestampGmt(int year = 1900, int month = 1,
            int day = 1, int hour = 0, int min = 0, int sec = 0, long ns = 0);

        /**
         * Make Date in human understandable way.
         *
         * Created Timestamp uses Local timezone.
         *
         * @param year Year.
         * @param month Month.
         * @param day Day.
         * @param hour Hour.
         * @param min Minute.
         * @param sec Second.
         * @param ns Nanosecond.
         * @return Timestamp.
         */
        Timestamp MakeTimestampLocal(int year = 1900, int month = 1,
            int day = 1, int hour = 0, int min = 0, int sec = 0, long ns = 0);

        /**
         * Meta-programming class.
         * Defines T1 as ::type if the condition is true, otherwise
         * defines T2 as ::type.
         */
        template<bool, typename T1, typename T2>
        struct Conditional
        {
            typedef T1 type;
        };

        /**
         * Specialization for the false case.
         */
        template<typename T1, typename T2>
        struct Conditional<false, T1, T2>
        {
            typedef T2 type;
        };
    }
}

#endif //_IGNITE_COMMON_UTILS