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

#include <ignite/odbc/odbc_error.h>

#include <ignite/odbc/sql/sql_lexer.h>
#include <ignite/odbc/sql/sql_utils.h>

namespace ignite
{
    namespace odbc
    {
        namespace sql_utils
        {
            bool IsInternalCommand(const std::string& sql)
            {
                SqlLexer lexer(sql);

                while (true)
                {
                    OdbcExpected<bool> hasNext = lexer.Shift();

                    if (!hasNext.IsOk() || !*hasNext)
                        return false;

                    const SqlToken& token = lexer.GetCurrentToken();

                    if (token.GetType() != TokenType::SEMICOLON)
                    {
                        if (token.GetType() == TokenType::WORD && token.ToLower() == "set")
                            break;

                        return false;
                    }
                }

                return lexer.ExpectNextToken(TokenType::WORD, "streaming");
            }
        }
    }
}
