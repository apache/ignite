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

#include <boost/test/unit_test.hpp>

#include <string>

#include <ignite/ignite.h>
#include <ignite/ignition.h>

#include <ignite/odbc/odbc_error.h>
#include <ignite/odbc/sql/sql_lexer.h>
#include <ignite/odbc/sql/sql_parser.h>
#include <ignite/odbc/sql/sql_utils.h>
#include <ignite/odbc/sql/sql_set_streaming_command.h>

#include "test_utils.h"


using namespace ignite;
using namespace ignite_test;

using namespace boost::unit_test;

void CheckNextToken(odbc::SqlLexer& lexer, odbc::TokenType::Type tokenType, const std::string& expected)
{
    BOOST_REQUIRE(!lexer.IsEod());

    odbc::OdbcExpected<bool> hasNext = lexer.Shift();

    if (!hasNext.IsOk())
        BOOST_FAIL(hasNext.GetError().GetErrorMessage());

    BOOST_CHECK(*hasNext);

    const odbc::SqlToken& token = lexer.GetCurrentToken();
    
    BOOST_REQUIRE(token.GetValue());
    BOOST_CHECK_GT(token.GetSize(), 0);
    BOOST_CHECK_EQUAL(token.GetType(), tokenType);
    BOOST_CHECK_EQUAL(token.ToString(), expected);
}

void CheckSetStreamingCommand(
    odbc::SqlParser& parser,
    bool enabled,
    bool allowOverwrite,
    int32_t batchSize,
    int32_t bufferSizePerNode,
    int32_t parallelOperationsPerNode,
    int64_t flushFrequency,
    bool ordered)
{
    std::auto_ptr<odbc::SqlCommand> cmd = parser.GetNextCommand();

    BOOST_REQUIRE(cmd.get() != 0);
    BOOST_REQUIRE_EQUAL(cmd->GetType(), odbc::SqlCommandType::SET_STREAMING);

    odbc::SqlSetStreamingCommand& cmd0 = static_cast<odbc::SqlSetStreamingCommand&>(*cmd);

    BOOST_CHECK_EQUAL(cmd0.IsEnabled(), enabled);
    BOOST_CHECK_EQUAL(cmd0.IsAllowOverwrite(), allowOverwrite);
    BOOST_CHECK_EQUAL(cmd0.GetBatchSize(), batchSize);
    BOOST_CHECK_EQUAL(cmd0.GetBufferSizePerNode(), bufferSizePerNode);
    BOOST_CHECK_EQUAL(cmd0.GetParallelOperationsPerNode(), parallelOperationsPerNode);
    BOOST_CHECK_EQUAL(cmd0.GetFlushFrequency(), flushFrequency);
    BOOST_CHECK_EQUAL(cmd0.IsOrdered(), ordered);
}

void CheckSingleSetStreamingCommand(
    const std::string& sql,
    bool enabled,
    bool allowOverwrite,
    int32_t batchSize,
    int32_t bufferSizePerNode,
    int32_t parallelOperationsPerNode,
    int64_t flushFrequency,
    bool ordered)
{
    BOOST_CHECK(odbc::sql_utils::IsInternalCommand(sql));

    odbc::SqlParser parser(sql);

    CheckSetStreamingCommand(parser, enabled, allowOverwrite, batchSize, bufferSizePerNode,
        parallelOperationsPerNode, flushFrequency, ordered);

    std::auto_ptr<odbc::SqlCommand> cmd = parser.GetNextCommand();
    BOOST_CHECK(cmd.get() == 0);
}

void CheckUnexpectedTokenError(const std::string& sql, const std::string& token, const std::string& expected = "additional parameter of SET STREAMING command or semicolon")
{
    odbc::SqlParser parser(sql);

    try
    {
        parser.GetNextCommand();

        BOOST_FAIL("Exception expected.");
    }
    catch (const odbc::OdbcError& err)
    {
        std::string expErr = "Unexpected token: '" + token + "'";

        if (!expected.empty())
            expErr += ", " + expected + " expected.";

        BOOST_CHECK_EQUAL(err.GetStatus(), odbc::SqlState::S42000_SYNTAX_ERROR_OR_ACCESS_VIOLATION);
        BOOST_CHECK_EQUAL(err.GetErrorMessage(), expErr);
    }
}

void CheckUnexpectedEndOfStatement(const std::string& sql, const std::string& expected)
{
    odbc::SqlParser parser(sql);

    try
    {
        parser.GetNextCommand();

        BOOST_FAIL("Exception expected.");
    }
    catch (const odbc::OdbcError& err)
    {
        std::string expErr = "Unexpected end of statement: " + expected + " expected.";

        BOOST_CHECK_EQUAL(err.GetStatus(), odbc::SqlState::S42000_SYNTAX_ERROR_OR_ACCESS_VIOLATION);
        BOOST_CHECK_EQUAL(err.GetErrorMessage(), expErr);
    }
}

BOOST_AUTO_TEST_SUITE(SqlParsingTestSuite)

BOOST_AUTO_TEST_CASE(LexerTokens)
{
    std::string sql(
        "word "
        "otherword "
        "and "
        "another "
        "\"quoted\" "
        "\"quoted with \"\"qoutes\"\" inside \" "
        "'string' "
        "'string with \"quotes\"' "
        " \n \r   \t   "
        "'string with ''string quotes'' inside' "
        "some.val "
        "three,vals,here "
        "\"Lorem;,.\";'Ipsum,;.--' "
        "(something) "
        "-- comment "
        "still comment \n"
        "-5 end");

    odbc::SqlLexer lexer(sql);

    CheckNextToken(lexer, odbc::TokenType::WORD, "word");
    CheckNextToken(lexer, odbc::TokenType::WORD, "otherword");
    CheckNextToken(lexer, odbc::TokenType::WORD, "and");
    CheckNextToken(lexer, odbc::TokenType::WORD, "another");
    CheckNextToken(lexer, odbc::TokenType::QUOTED, "\"quoted\"");
    CheckNextToken(lexer, odbc::TokenType::QUOTED, "\"quoted with \"\"qoutes\"\" inside \"");
    CheckNextToken(lexer, odbc::TokenType::STRING, "'string'");
    CheckNextToken(lexer, odbc::TokenType::STRING, "'string with \"quotes\"'");
    CheckNextToken(lexer, odbc::TokenType::STRING, "'string with ''string quotes'' inside'");
    CheckNextToken(lexer, odbc::TokenType::WORD, "some");
    CheckNextToken(lexer, odbc::TokenType::DOT, ".");
    CheckNextToken(lexer, odbc::TokenType::WORD, "val");
    CheckNextToken(lexer, odbc::TokenType::WORD, "three");
    CheckNextToken(lexer, odbc::TokenType::COMMA, ",");
    CheckNextToken(lexer, odbc::TokenType::WORD, "vals");
    CheckNextToken(lexer, odbc::TokenType::COMMA, ",");
    CheckNextToken(lexer, odbc::TokenType::WORD, "here");
    CheckNextToken(lexer, odbc::TokenType::QUOTED, "\"Lorem;,.\"");
    CheckNextToken(lexer, odbc::TokenType::SEMICOLON, ";");
    CheckNextToken(lexer, odbc::TokenType::STRING, "'Ipsum,;.--'");
    CheckNextToken(lexer, odbc::TokenType::PARENTHESIS_LEFT, "(");
    CheckNextToken(lexer, odbc::TokenType::WORD, "something");
    CheckNextToken(lexer, odbc::TokenType::PARENTHESIS_RIGHT, ")");
    CheckNextToken(lexer, odbc::TokenType::MINUS, "-");
    CheckNextToken(lexer, odbc::TokenType::WORD, "5");
    CheckNextToken(lexer, odbc::TokenType::WORD, "end");

    BOOST_REQUIRE(lexer.IsEod());
    
    odbc::OdbcExpected<bool> hasNext = lexer.Shift();

    if (!hasNext.IsOk())
        BOOST_FAIL(hasNext.GetError().GetErrorMessage());

    BOOST_CHECK(!*hasNext);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOff)
{
    CheckSingleSetStreamingCommand("set streaming off", false, false, 2048, 0, 0, 0, false);
    CheckSingleSetStreamingCommand("set streaming 0",   false, false, 2048, 0, 0, 0, false);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOffMixedCase)
{
    CheckSingleSetStreamingCommand("SET Streaming OfF", false, false, 2048, 0, 0, 0, false);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOn)
{
    CheckSingleSetStreamingCommand("set streaming on", true, false, 2048, 0, 0, 0, false);
    CheckSingleSetStreamingCommand("set streaming 1",  true, false, 2048, 0, 0, 0, false);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOnAllowOverwriteOn)
{
    CheckSingleSetStreamingCommand("set streaming on allow_overwrite on", true, true, 2048, 0, 0, 0, false);
    CheckSingleSetStreamingCommand("set streaming on allow_overwrite 1", true, true, 2048, 0, 0, 0, false);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOnAllowOverwriteOff)
{
    CheckSingleSetStreamingCommand("set streaming on allow_overwrite off", true, false, 2048, 0, 0, 0, false);
    CheckSingleSetStreamingCommand("set streaming on allow_overwrite 0", true, false, 2048, 0, 0, 0, false);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOnPageSize512)
{
    CheckSingleSetStreamingCommand("set streaming on batch_size 512", true, false, 512, 0, 0, 0, false);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOnPerNodeBufferSize500)
{
    CheckSingleSetStreamingCommand("set streaming on per_node_buffer_size 500", true, false, 2048, 500, 0, 0, false);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOnPerNodeParallelOperations4)
{
    CheckSingleSetStreamingCommand("set streaming on per_node_parallel_operations 4", true, false, 2048, 0, 4, 0, false);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOnFlushFrequency100)
{
    CheckSingleSetStreamingCommand("set streaming on flush_frequency 100", true, false, 2048, 0, 0, 100, false);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOnOrdered)
{
    CheckSingleSetStreamingCommand("set streaming on ordered", true, false, 2048, 0, 0, 0, true);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOnAll)
{
    CheckSingleSetStreamingCommand(
        "set streaming 1 "
        "allow_overwrite on "
        "batch_size 512 "
        "per_node_buffer_size 500 "
        "per_node_parallel_operations 4 "
        "flush_frequency 100 "
        "ordered",
        true, true, 512, 500, 4, 100, true);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOnAllDisorder)
{
    CheckSingleSetStreamingCommand(
        "set streaming 1 "
        "batch_size 512 "
        "allow_overwrite on "
        "ordered "
        "per_node_buffer_size 500 "
        "flush_frequency 100 "
        "per_node_parallel_operations 4 ",
        true, true, 512, 500, 4, 100, true);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOnAllBackward)
{
    CheckSingleSetStreamingCommand(
        "set streaming 1 "
        "ordered "
        "flush_frequency 100 "
        "per_node_parallel_operations 4 "
        "per_node_buffer_size 500 "
        "batch_size 512 "
        "allow_overwrite on ",
        true, true, 512, 500, 4, 100, true);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOnOff)
{
    std::string sql("set streaming 1; set streaming off");

    odbc::SqlParser parser(sql);

    CheckSetStreamingCommand(parser, true, false, 2048, 0, 0, 0, false);
    CheckSetStreamingCommand(parser, false, false, 2048, 0, 0, 0, false);

    std::auto_ptr<odbc::SqlCommand> cmd = parser.GetNextCommand();

    BOOST_CHECK(cmd.get() == 0);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOnUnexpectedTokenError)
{
    CheckUnexpectedTokenError("set streaming 1 ololo", "ololo");
    CheckUnexpectedTokenError("set streaming 1 ordered lorem_ipsum", "lorem_ipsum");
    CheckUnexpectedTokenError("set streaming ON lorem_ipsum ordered", "lorem_ipsum");

    CheckUnexpectedTokenError("set streaming some", "some", "ON, OFF, 1 or 0");
    CheckUnexpectedTokenError("some", "some", "");
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOffUnexpectedTokenError)
{
    CheckUnexpectedTokenError("set streaming 0 ololo", "ololo");
    CheckUnexpectedTokenError("set streaming 0 ordered", "ordered", "no parameters with STREAMING OFF command");
    CheckUnexpectedTokenError("set streaming OFF lorem_ipsum ordered", "lorem_ipsum");
}

BOOST_AUTO_TEST_CASE(ParserInternalCommand)
{
    BOOST_CHECK(odbc::sql_utils::IsInternalCommand("set streaming 1"));
    BOOST_CHECK(odbc::sql_utils::IsInternalCommand("set streaming 0"));
    BOOST_CHECK(odbc::sql_utils::IsInternalCommand("set streaming on"));
    BOOST_CHECK(odbc::sql_utils::IsInternalCommand("set streaming off"));
    
    BOOST_CHECK(odbc::sql_utils::IsInternalCommand("SET STREAMING 1"));
    BOOST_CHECK(odbc::sql_utils::IsInternalCommand("SET STREAMING 0"));
    BOOST_CHECK(odbc::sql_utils::IsInternalCommand("SET STREAMING ON"));
    BOOST_CHECK(odbc::sql_utils::IsInternalCommand("SET STREAMING OFF"));
    
    BOOST_CHECK(odbc::sql_utils::IsInternalCommand("Set Streaming 1"));
    BOOST_CHECK(odbc::sql_utils::IsInternalCommand("Set Streaming 0"));
    BOOST_CHECK(odbc::sql_utils::IsInternalCommand("Set Streaming On"));
    BOOST_CHECK(odbc::sql_utils::IsInternalCommand("Set Streaming Off"));
    
    BOOST_CHECK(odbc::sql_utils::IsInternalCommand(";SET STREAMING 1"));
    BOOST_CHECK(odbc::sql_utils::IsInternalCommand(";;SET STREAMING 0"));
    BOOST_CHECK(odbc::sql_utils::IsInternalCommand(";;;SET STREAMING ON"));
    BOOST_CHECK(odbc::sql_utils::IsInternalCommand(";;;;SET STREAMING OFF"));

    BOOST_CHECK(odbc::sql_utils::IsInternalCommand("set streaming"));
    BOOST_CHECK(odbc::sql_utils::IsInternalCommand("set streaming blah"));
}

BOOST_AUTO_TEST_CASE(ParserNonInternalCommand)
{
    BOOST_CHECK(!odbc::sql_utils::IsInternalCommand(""));
    BOOST_CHECK(!odbc::sql_utils::IsInternalCommand("Blah"));
    BOOST_CHECK(!odbc::sql_utils::IsInternalCommand("0"));
    BOOST_CHECK(!odbc::sql_utils::IsInternalCommand(";"));
    BOOST_CHECK(!odbc::sql_utils::IsInternalCommand("Lorem ipsum"));
    BOOST_CHECK(!odbc::sql_utils::IsInternalCommand("set some"));
}

BOOST_AUTO_TEST_SUITE_END()
