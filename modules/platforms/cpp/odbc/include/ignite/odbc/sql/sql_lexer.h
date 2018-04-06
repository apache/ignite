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

#ifndef _IGNITE_ODBC_SQL_LEXER
#define _IGNITE_ODBC_SQL_LEXER

#include <stdint.h>
#include <string>

namespace ignite
{
    namespace odbc
    {
        /**
         * Token type.
         */
        struct TokenType
        {
            enum Type
            {
                /** Minus token. */
                MINUS,

                /** End of data. */
                EOD,

                /** Parsing error. */
                ERROR,
            };
        };

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
            bool Shift();

            /**
             * Check if the end of data reached.
             *
             * @return @c true if the end of data reached.
             */
            bool IsEod() const;

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
            bool HaveData(int32_t num);

            /** SQL string. */
            const std::string& sql;

            /** Current token position. */
            int32_t pos;

            /** Current token size. */
            int32_t size;

            /** Current token type. */
            TokenType::Type type;
        };
    }
}

#endif //_IGNITE_ODBC_SQL_LEXER