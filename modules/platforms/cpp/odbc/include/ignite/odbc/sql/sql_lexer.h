/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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