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

#include <ignite/common/utils.h>

#include <ignite/odbc/odbc_error.h>
#include <ignite/odbc/sql/sql_set_streaming_command.h>
#include <ignite/odbc/sql/sql_parser.h>

const static std::string WORD_SET("set");

const static std::string WORD_STREAMING("streaming");

namespace ignite
{
    namespace odbc
    {
        SqlParser::SqlParser(const std::string& sql) :
            lexer(sql)
        {
            // No-op.
        }

        SqlParser::~SqlParser()
        {
            // No-op.
        }

        std::auto_ptr<SqlCommand> SqlParser::GetNextCommand()
        {
            while (true)
            {
                if (!*lexer.Shift())
                    return std::auto_ptr<SqlCommand>();

                const SqlToken& token = lexer.GetCurrentToken();

                switch (token.GetType())
                {
                    case TokenType::SEMICOLON:
                        // Empty command. Skipping.
                        continue;

                    case TokenType::WORD:
                        return ProcessCommand();

                    case TokenType::QUOTED:
                    case TokenType::MINUS:
                    case TokenType::DOT:
                    case TokenType::COMMA:
                    case TokenType::PARENTHESIS_LEFT:
                    case TokenType::PARENTHESIS_RIGHT:
                    default:
                    {
                        throw OdbcError(SqlState::S42000_SYNTAX_ERROR_OR_ACCESS_VIOLATION,
                            "Unexpected token: '" + token.ToString() + "'");
                    }
                }
            }
        }

        std::auto_ptr<SqlCommand> SqlParser::ProcessCommand()
        {
            const SqlToken& token = lexer.GetCurrentToken();

            if (WORD_SET == token.ToLower() &&
                *lexer.Shift() &&
                token.GetType() == TokenType::WORD &&
                WORD_STREAMING == token.ToLower())
            {
                std::auto_ptr<SqlCommand> cmd(new SqlSetStreamingCommand);

                cmd->Parse(lexer);

                return cmd;
            }

            throw OdbcError(SqlState::S42000_SYNTAX_ERROR_OR_ACCESS_VIOLATION,
                "Unexpected token: '" + token.ToString() + "'");
        }
    }
}

