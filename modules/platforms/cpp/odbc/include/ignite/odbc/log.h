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

#ifndef _IGNITE_ODBC_LOG
#define _IGNITE_ODBC_LOG

#include <string>
#include <sstream>
#include <fstream>

#include "ignite/common/common.h"
#include "ignite/common/concurrent.h"

#   define LOG_MSG(param)                                      \
    if (ignite::odbc::Logger* p = ignite::odbc::Logger::Get()) \
    {                                                          \
        ignite::odbc::LogStream lstream(p);                    \
        lstream << __FUNCTION__ << ": " << param;              \
    }

namespace ignite
{
    namespace odbc
    {
        /* Forward declaration */
        class Logger;

        /**
         * Helper object providing stream operations for single log line.
         * Writes resulting string to Logger object upon destruction.
         */
        class LogStream: public std::basic_ostream<char>
        {
        public:
            /**
             * Constructor.
             * @param parent pointer to Logger.
             */
            LogStream(Logger* parent);

            /**
             * Conversion operator helpful to determine if log is enabled
             * @return True if logger is enabled
             */
            bool operator()();

            /**
             * Destructor.
             */
            virtual ~LogStream();

        private:
            IGNITE_NO_COPY_ASSIGNMENT(LogStream);

            /** String buffer. */
            std::basic_stringbuf<char> strbuf;

            /** Parent logger object */
            Logger* logger;
        };

        /**
         * Logging facility.
         */
        class Logger
        {
        public:
            /**
             * Get instance of Logger, if enabled.
             * @return Logger instance if logging is enabled. Null otherwise.
             */
            static Logger* Get();

            /**
             * Checks if logging is enabled.
             * @return True, if logging is enabled.
             */
            bool IsEnabled() const;

            /**
             * Outputs the message to log file
             * @param message The message to write
             */
            void WriteMessage(std::string const& message);

        private:
            /**
             * Constructor.
             * @param path to log file.
             */
            Logger(const char* path);

            /**
             * Destructor.
             */
            ~Logger();

            IGNITE_NO_COPY_ASSIGNMENT(Logger);

            /** Mutex for writes synchronization. */
            ignite::common::concurrent::CriticalSection mutex;

            /** File stream. */
            std::ofstream stream;
        };
    }
}

#endif //_IGNITE_ODBC_LOG
