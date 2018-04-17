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

#include "ignite/odbc/sql/sql_lexer.h"
#include "ignite/odbc/odbc_error.h"

namespace ignite
{
    namespace odbc
    {
        SqlLexer::SqlLexer(const std::string& sql) :
            sql(sql),
            pos(0),
            tokenBegin(0),
            tokenSize(0),
            tokenType(TokenType::EOD)
        {
            // No-op.
        }

        SqlLexer::~SqlLexer()
        {
            // No-op.
        }

        bool SqlLexer::Shift()
        {
            if (IsEod())
            {
                SetEod();

                return false;
            }

            tokenType = TokenType::EOD;

            while (!IsEod())
            {
                tokenBegin = pos;

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
                        do {
                            ++pos;

                            if (IsEod())
                                throw OdbcError(SqlState::SHY000_GENERAL_ERROR, "Unclosed quoted identifier.");

                        } while (sql[pos] != '"' || (HaveData(2) && sql[pos + 1] == '"'));

                        tokenType = TokenType::QUOTED;

                        break;
                    }

                    case '\'':
                    {
                        // String literal.
                        do
                        {
                            ++pos;

                            if (IsEod())
                                throw OdbcError(SqlState::SHY000_GENERAL_ERROR, "Unclosed string literal.");

                        } while (sql[pos] != '\'' || (HaveData(2) && sql[pos + 1] == '\''));

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
                            ++pos;

                            continue;
                        }

                        // Word.
                        while (!IsEod() && !isspace(sql[pos]) && !iscntrl(sql[pos]))
                            ++pos;

                        --pos;

                        tokenType = TokenType::WORD;

                        break;
                    }
                }

                ++pos;

                if (tokenType != TokenType::EOD)
                {
                    tokenSize = pos - tokenBegin;

                    break;
                }
            }

            return true;
        }

        bool SqlLexer::IsEod() const
        {
            return pos >= static_cast<int32_t>(sql.size());
        }

        void SqlLexer::SetEod()
        {
            pos = sql.size();

            tokenType = TokenType::EOD;

            tokenSize = 0;
        }

        bool SqlLexer::HaveData(int32_t num) const
        {
            return static_cast<size_t>(pos + num) < sql.size();
        }
    }
}

