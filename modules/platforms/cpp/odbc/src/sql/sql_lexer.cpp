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

#include "ignite/odbc/sql/sql_lexer.h"

namespace ignite
{
    namespace odbc
    {
        SqlLexer::SqlLexer(const std::string& sql) :
            sql(sql),
            pos(0),
            size(0),
            type(TokenType::EOD)
        {
            // No-op.
        }

        SqlLexer::~SqlLexer()
        {
            // No-op.
        }

        bool SqlLexer::Shift()
        {
            while (!IsEod())
            {
                int32_t tokenBegin = pos;

                switch (sql[pos])
                {
                    case '-':
                    {
                        if (HaveData(1) && sql[pos + 1] == '-')
                        {
                            // Full-line comment.
                            pos += 2;

                            while (!IsEod() && sql[pos] != '\n' && sql[pos] != '\r')
                                ++pos;
                        }
                        else
                        {
                            // Minus.
                            type = TokenType::MINUS;

                            size = 1;
                        }

                        break;
                    }

                    case '"':
                    {
                        // Quoted string.
                        do {
                            ++pos;

                            if (IsEod())
                            {
                                //
                            }


                        } while (sql[pos] != '"');

                        break;
                    }

                }

                //
            }

            return !IsEod();
        }

        bool SqlLexer::IsEod() const
        {
            return pos >= sql.size();
        }

        void SqlLexer::SetEod()
        {
            pos = sql.size();

            type = TokenType::EOD;

            size = 0;
        }

        bool SqlLexer::HaveData(int32_t num)
        {
            return static_cast<size_t>(pos + num) < sql.size();
        }
    }
}

