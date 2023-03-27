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

#ifndef _IGNITE_ODBC_SQL_SQL_LEXER
#define _IGNITE_ODBC_SQL_SQL_LEXER

#include <stdint.h>
#include <string>

#include <ignite/odbc/odbc_error.h>
#include <ignite/odbc/sql/sql_token.h>

namespace ignite
{
    namespace odbc
    {
        /**
         * SQL lexer.
         */
        class SqlLexer
        {
        public:
            /**
             * Constructor.
             *
             * @param sql SQL string.
             */
            SqlLexer(const std::string& sql);

            /**
             * Destructor.
             */
            ~SqlLexer();

            /**
             * Move to the next token.
             *
             * @return @c true if next token was found and @c false otherwise.
             */
            OdbcExpected<bool> Shift();

            /**
             * Check that the following token is the expected one.
             * Shifts to next token if possible.
             *
             * @param typ Token type.
             * @param expected Expected token. Should be lowercase.
             * @return @c true if the next token is expected and @c false otherwise.
             */
            bool ExpectNextToken(TokenType::Type typ, const char* expected);

            /**
             * Check if the end of data reached.
             *
             * @return @c true if the end of data reached.
             */
            bool IsEod() const;

            /**
             * Get current token.
             *
             * @return Current token.
             */
            const SqlToken& GetCurrentToken() const
            {
                return currentToken;
            }

        private:
            /**
             * Set end of data state.
             */
            void SetEod();

            /**
             * Have enough data.
             *
             * @param num Number of chars we need.
             * @return @c true if we have and false otherwise.
             */
            bool HaveData(int32_t num) const;

            /**
             * Check if the char is delimiter.
             *
             * @param c Character
             * @return True if the character is delimiter.
             */
            static bool IsDelimiter(int c);

            /** SQL string. */
            const std::string& sql;

            /** Current lexer position in string. */
            int32_t pos;

            /** Current token. */
            SqlToken currentToken;
        };
    }
}

#endif //_IGNITE_ODBC_SQL_SQL_LEXER