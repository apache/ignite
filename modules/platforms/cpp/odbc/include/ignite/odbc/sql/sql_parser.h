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

#ifndef _IGNITE_ODBC_SQL_PARSER
#define _IGNITE_ODBC_SQL_PARSER

#include <stdint.h>
#include <string>

#include <ignite/odbc/sql/sql_lexer.h>

namespace ignite
{
    namespace odbc
    {
        /**
         * SQL command.
         */
        class SqlCommand
        {
        public:
            /**
             * Default constructor.
             */
            SqlCommand();

            /**
             * Destructor.
             */
            ~SqlCommand();

        private:
        };

        /**
         * SQL parser.
         */
        class SqlParser
        {
        public:
            /**
             * Default constructor.
             *
             * @param sql SQL request.
             */
            SqlParser(const std::string& sql);

            /**
             * Destructor.
             */
            ~SqlParser();

            /**
             * Parse SQL into SqlCommand.
             *
             * @param sql SQL string.
             * @param cmd Parsed command.
             * @return @c true on success and @c false on failure.
             */
            bool ParseSql(const std::string& sql, SqlCommand& cmd);

            /**
             * Shift to the next command.
             *
             * @return @c true if the next command is found.
             */
            bool ShiftToNextCommand();

        private:
            /** SQL lexer. */
            SqlLexer lexer;
        };
    }
}

#endif //_IGNITE_ODBC_SQL_PARSER