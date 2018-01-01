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

#include <cstdlib>

#include "ignite/odbc/log.h"

namespace ignite
{
    namespace odbc
    {
        LogStream::LogStream(Logger* parent) :
            std::basic_ostream<char>(0),
            strbuf(),
            logger(parent)
        {
            init(&strbuf);
        }

        bool LogStream::operator()()
        {
            return logger != 0;
        }

        LogStream::~LogStream()
        {
            if (logger)
            {
                logger->WriteMessage(strbuf.str());
            }
        }

        Logger::Logger(const char* path) :
            mutex(),
            stream()
        {
            if (path)
            {
                stream.open(path);
            }
        }

        Logger::~Logger()
        {
        }

        bool Logger::IsEnabled() const
        {
            return stream.is_open();
        }

        void Logger::WriteMessage(std::string const& message)
        {
            if (IsEnabled())
            {
                ignite::common::concurrent::CsLockGuard guard(mutex);
                stream << message << std::endl;
            }
        }

        Logger* Logger::Get()
        {
            const char* envVarName = "IGNITE_ODBC_LOG_PATH";
            static Logger logger(getenv(envVarName));
            return logger.IsEnabled() ? &logger : 0;
        }
    }
}

