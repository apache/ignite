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

#include <cctype>

#include "ignite/odbc/odbc_error.h"
#include "ignite/odbc/sql/sql_lexer.h"


namespace ignite
{
    namespace odbc
    {
        SqlLexer::SqlLexer(const std::string& sql) :
            sql(sql),
            pos(0),
            currentToken(0, 0, TokenType::EOD)
        {
            // No-op.
        }

        SqlLexer::~SqlLexer()
        {
            // No-op.
        }

        OdbcExpected<bool> SqlLexer::Shift()
        {
            if (IsEod())
            {
                SetEod();

                return false;
            }

            TokenType::Type tokenType = TokenType::EOD;

            while (!IsEod())
            {
                int32_t tokenBegin = pos;

                switch (sql[pos])
                {
                    case '-':
                    {
                        // Full-line comment.
                        if (HaveData(1) && sql[pos + 1] == '-')
                        {
                            pos += 2;

                            while (!IsEod() && sql[pos] != '\n' && sql[pos] != '\r')
                                ++pos;

                            continue;
                        }

                        // Minus.
                        tokenType = TokenType::MINUS;

                        break;
                    }

                    case '"':
                    {
                        // Quoted string.
                        while (true)
                        {
                            ++pos;

                            if (IsEod())
                                return OdbcError(SqlState::SHY000_GENERAL_ERROR, "Unclosed quoted identifier.");

                            if (sql[pos] == '"')
                            {
                                if (!HaveData(2) || sql[pos + 1] != '"')
                                    break;

                                ++pos;
                            }
                        }

                        tokenType = TokenType::QUOTED;

                        break;
                    }

                    case '\'':
                    {
                        // String literal.
                        while (true)
                        {
                            ++pos;

                            if (IsEod())
                                return OdbcError(SqlState::SHY000_GENERAL_ERROR, "Unclosed string literal.");

                            if (sql[pos] == '\'')
                            {
                                if (!HaveData(2) || sql[pos + 1] != '\'')
                                    break;

                                ++pos;
                            }
                        }

                        tokenType = TokenType::STRING;

                        break;
                    }

                    case '.':
                    {
                        tokenType = TokenType::DOT;

                        break;
                    }

                    case ',':
                    {
                        tokenType = TokenType::COMMA;

                        break;
                    }

                    case ';':
                    {
                        tokenType = TokenType::SEMICOLON;

                        break;
                    }

                    case '(':
                    {
                        tokenType = TokenType::PARENTHESIS_LEFT;

                        break;
                    }

                    case ')':
                    {
                        tokenType = TokenType::PARENTHESIS_RIGHT;

                        break;
                    }

                    default:
                    {
                        // Skipping spaces.
                        if (iscntrl(sql[pos]) || isspace(sql[pos]))
                        {
                            do
                            {
                                ++pos;
                            }
                            while (!IsEod() && (iscntrl(sql[pos]) || isspace(sql[pos])));

                            continue;
                        }

                        // Word.
                        while (!IsEod() && !IsDelimiter(sql[pos]))
                            ++pos;

                        --pos;

                        tokenType = TokenType::WORD;

                        break;
                    }
                }

                ++pos;

                if (tokenType != TokenType::EOD)
                {
                    currentToken = SqlToken(&sql[tokenBegin], pos - tokenBegin, tokenType);

                    return true;
                }
            }

            SetEod();

            return false;
        }

        bool SqlLexer::ExpectNextToken(TokenType::Type typ, const char* expected)
        {
            OdbcExpected<bool> hasNext = Shift();

            if (!hasNext.IsOk() || !*hasNext)
                return false;

            const SqlToken& token = GetCurrentToken();

            return token.GetType() == typ && token.ToLower() == expected;
        }

        bool SqlLexer::IsEod() const
        {
            return pos >= static_cast<int32_t>(sql.size());
        }

        void SqlLexer::SetEod()
        {
            pos = static_cast<int32_t>(sql.size());

            currentToken = SqlToken(0, 0, TokenType::EOD);
        }

        bool SqlLexer::HaveData(int32_t num) const
        {
            return static_cast<size_t>(pos + num) < sql.size();
        }

        bool SqlLexer::IsDelimiter(int c)
        {
            return !isalnum(c) && c != '_';
        }
    }
}

