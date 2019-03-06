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

#ifndef _IGNITE_ODBC_SQL_SQL_TOKEN
#define _IGNITE_ODBC_SQL_SQL_TOKEN

#include <stdint.h>

#include <ignite/common/utils.h>

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

                /** Quoted token. */
                QUOTED,

                /** String literal token. */
                STRING,

                /** Dot. */
                DOT,

                /** Comma. */
                COMMA,

                /** Parenthesis: left. */
                PARENTHESIS_LEFT,

                /** Parenthesis: right. */
                PARENTHESIS_RIGHT,

                /** Semicolon. */
                SEMICOLON,

                /** Simple word. */
                WORD,

                /** End of data. */
                EOD
            };
        };

        /**
         * SQL token.
         */
        class SqlToken
        {
        public:
            /**
             * Constructor.
             *
             * @param token Token begin pointer.
             * @param size Token size in characters.
             * @param typ Token type.
             */
            SqlToken(const char* token, int32_t size, TokenType::Type typ) :
                token(token),
                size(size),
                typ(typ)
            {
                // No-op.
            }

            /**
             * Get type.
             *
             * @return Current token type.
             */
            TokenType::Type GetType() const
            {
                return typ;
            }

            /**
             * Get token value.
             *
             * @return Pointer to the beginning of the value. Size of the value can be obtained with @c GetSize().
             */
            const char* GetValue() const
            {
                return token;
            }

            /**
             * Get size.
             *
             * @return Size.
             */
            int32_t GetSize() const
            {
                return size;
            }

            /**
             * Convert to string.
             *
             * @return String token.
             */
            std::string ToString() const
            {
                if (!token || size <= 0)
                    return std::string();

                return std::string(token, static_cast<size_t>(size));
            }

            /**
             * Convert to lowercase string.
             *
             * @return Lowercase string token.
             */
            std::string ToLower() const
            {
                std::string str(ToString());

                common::IntoLower(str);

                return str;
            }

        private:
            /** Current token begin. */
            const char* token;

            /** Current token size. */
            int32_t size;

            /** Current token type. */
            TokenType::Type typ;
        };
    }
}

#endif //_IGNITE_ODBC_SQL_SQL_TOKEN