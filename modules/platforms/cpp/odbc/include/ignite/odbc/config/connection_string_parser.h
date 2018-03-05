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

#ifndef _IGNITE_ODBC_CONFIG_CONNECTION_STRING_PARSER
#define _IGNITE_ODBC_CONFIG_CONNECTION_STRING_PARSER

#include <string>

#include "connection_string_parser_base.h"
#include "configuration.h"

namespace ignite
{
    namespace odbc
    {
        namespace config
        {
            /**
             * ODBC configuration parser abstraction.
             */
            class ConnectionStringParser : public ConnectionStringParserBase
            {
            public:

                /**
                 * Default constructor.
                 *
                 * @param cfg Configuration.
                 */
                ConnectionStringParser(Configuration& cfg);

                /**
                 * Destructor.
                 */
                virtual ~ConnectionStringParser()
                {
                    // No-op.
                }

            private:
                /**
                 * Result of parsing string value to bool.
                 */
                struct BoolParseResult
                {
                    enum Type
                    {
                        AI_FALSE,

                        AI_TRUE,

                        AI_UNRECOGNIZED
                    };
                };

                /**
                 * Handle new attribute pair callback.
                 *
                 * @param key Key.
                 * @param value Value.
                 * @param diag Diagnostics collector.
                 */
                virtual void HandleAttributePair(const std::string& key, const std::string& value,
                    diagnostic::Diagnosable* diag);

                /**
                 * Parse address.
                 *
                 * @param value String value to parse.
                 * @param endPoints End ponts list.
                 * @param diag Diagnostics collector.
                 */
                static void ParseAddress(const std::string& value, std::vector<Configuration::EndPoint>& endPoints,
                    diagnostic::Diagnosable* diag);

                /**
                 * Parse single address.
                 *
                 * @param value String value to parse.
                 * @param endPoint End pont.
                 * @param diag Diagnostics collector.
                 * @return @c true, if parsed successfully, and @c false otherwise.
                 */
                static bool ParseSingleAddress(const std::string& value, Configuration::EndPoint& endPoint,
                    diagnostic::Diagnosable* diag);

                /**
                 * Convert string to boolean value.
                 *
                 * @param value Value to convert to bool.
                 * @return Result.
                 */
                static BoolParseResult::Type StringToBool(const std::string& value);

                /**
                 * Convert string to boolean value.
                 *
                 * @param msg Error message.
                 * @param key Key.
                 * @param value Value.
                 * @return Resulting error message.
                 */
                static std::string MakeErrorMessage(const std::string& msg, const std::string& key,
                    const std::string& value);

                /** Configuration. */
                Configuration& cfg;
            };
        }

    }
}

#endif //_IGNITE_ODBC_CONFIG_CONNECTION_STRING_PARSER
