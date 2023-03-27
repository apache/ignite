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
#include <ignite/odbc/sql/sql_lexer.h>
#include <ignite/odbc/sql/sql_token.h>
#include <ignite/odbc/sql/sql_utils.h>
#include <ignite/odbc/sql/sql_set_streaming_command.h>

const static std::string WORD_BATCH_SIZE("batch_size");

const static std::string WORD_PER_NODE_BUFFER_SIZE("per_node_buffer_size");

const static std::string WORD_PER_NODE_PARALLEL_OPERATIONS("per_node_parallel_operations");

const static std::string WORD_ALLOW_OVERWRITE("allow_overwrite");

const static std::string WORD_FLUSH_FREQUENCY("flush_frequency");

const static std::string WORD_ORDERED("ordered");

namespace ignite
{
    namespace odbc
    {
        SqlSetStreamingCommand::SqlSetStreamingCommand() :
            SqlCommand(SqlCommandType::SET_STREAMING),
            enabled(false),
            allowOverwrite(false),
            batchSize(DEFAULT_STREAM_BATCH_SIZE),
            parallelOpsPerNode(0),
            bufferSizePerNode(0),
            flushFrequency(0),
            ordered(false)
        {
            // No-op.
        }

        SqlSetStreamingCommand::~SqlSetStreamingCommand()
        {
            // No-op.
        }

        void SqlSetStreamingCommand::Parse(SqlLexer& lexer)
        {
            enabled = ExpectBool(lexer);

            const SqlToken& token = lexer.GetCurrentToken();

            while (*lexer.Shift() && token.GetType() != TokenType::SEMICOLON)
            {
                if (token.ToLower() == WORD_BATCH_SIZE)
                {
                    CheckEnabled(token);

                    batchSize = ExpectPositiveInteger(lexer, "batch size");

                    continue;
                }

                if (token.ToLower() == WORD_PER_NODE_BUFFER_SIZE)
                {
                    CheckEnabled(token);

                    bufferSizePerNode = ExpectPositiveInteger(lexer, "per node buffer size");

                    continue;
                }

                if (token.ToLower() == WORD_PER_NODE_PARALLEL_OPERATIONS)
                {
                    CheckEnabled(token);

                    parallelOpsPerNode = ExpectPositiveInteger(lexer, "per node parallel operations number");

                    continue;
                }

                if (token.ToLower() == WORD_ALLOW_OVERWRITE)
                {
                    CheckEnabled(token);

                    allowOverwrite = ExpectBool(lexer);

                    continue;
                }

                if (token.ToLower() == WORD_FLUSH_FREQUENCY)
                {
                    CheckEnabled(token);

                    flushFrequency = ExpectPositiveInteger(lexer, "flush frequency");

                    continue;
                }

                if (token.ToLower() == WORD_ORDERED)
                {
                    CheckEnabled(token);

                    ordered = true;

                    continue;
                }

                ThrowUnexpectedTokenError(token, "additional parameter of SET STREAMING command or semicolon");
            }
        }

        void SqlSetStreamingCommand::CheckEnabled(const SqlToken& token) const
        {
            if (!enabled)
                ThrowUnexpectedTokenError(token, "no parameters with STREAMING OFF command");
        }

        void SqlSetStreamingCommand::ThrowUnexpectedTokenError(const SqlToken& token, const std::string& expected)
        {
            throw OdbcError(SqlState::S42000_SYNTAX_ERROR_OR_ACCESS_VIOLATION,
                "Unexpected token: '" + token.ToString() + "', " + expected + " expected.");
        }

        void SqlSetStreamingCommand::ThrowUnexpectedEndOfStatement(const std::string& expected)
        {
            throw OdbcError(SqlState::S42000_SYNTAX_ERROR_OR_ACCESS_VIOLATION,
                "Unexpected end of statement: " + expected + " expected.");
        }

        int32_t SqlSetStreamingCommand::ExpectInt(SqlLexer& lexer)
        {
            if (!*lexer.Shift())
                ThrowUnexpectedEndOfStatement("integer number");

            const SqlToken& token = lexer.GetCurrentToken();

            int sign = 1;

            if (token.GetType() == TokenType::MINUS)
            {
                sign = -1;

                if (!*lexer.Shift())
                    ThrowUnexpectedEndOfStatement("integer number");
            }

            std::string str = token.ToString();

            if (token.GetType() == TokenType::WORD && common::AllDigits(str))
            {
                int64_t val = sign * common::LexicalCast<int64_t>(str);

                if (val >= INT32_MIN && val <= INT32_MAX)
                    return static_cast<int32_t>(val);
            }

            ThrowUnexpectedTokenError(token, "integer number");

            return 0;
        }

        int32_t SqlSetStreamingCommand::ExpectPositiveInteger(SqlLexer& lexer, const std::string& description)
        {
            int32_t val = ExpectInt(lexer);

            if (val <= 0)
            {
                throw OdbcError(SqlState::S42000_SYNTAX_ERROR_OR_ACCESS_VIOLATION,
                    "Invalid " + description + " - positive integer number is expected.");
            }

            return val;
        }

        bool SqlSetStreamingCommand::ExpectBool(SqlLexer& lexer)
        {
            const SqlToken& token = lexer.GetCurrentToken();

            if (!*lexer.Shift())
                ThrowUnexpectedTokenError(token, "ON or OFF");

            return *sql_utils::TokenToBoolean(token);
        }
    }
}
