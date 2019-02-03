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

#ifndef _IGNITE_ODBC_SQL_SQL_PARSER
#define _IGNITE_ODBC_SQL_SQL_PARSER

#include <string>
#include <memory>

#include <ignite/odbc/sql/sql_lexer.h>
#include <ignite/odbc/sql/sql_command.h>

namespace ignite
{
    namespace odbc
    {
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
             * Get next command.
             *
             * @return Parsed command on success and null on failure.
             */
            std::auto_ptr<SqlCommand> GetNextCommand();

        private:
            /**
             * 
             */
            std::auto_ptr<SqlCommand> ProcessCommand();

            /** SQL lexer. */
            SqlLexer lexer;
        };
    }
}

#endif //_IGNITE_ODBC_SQL_SQL_PARSER