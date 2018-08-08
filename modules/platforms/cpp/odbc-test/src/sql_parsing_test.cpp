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

#ifdef _WIN32
#   include <Windows.h>
#endif

#include <sql.h>
#include <sqlext.h>

#include <string>

#ifndef _MSC_VER
#   define BOOST_TEST_DYN_LINK
#endif

#include <boost/test/unit_test.hpp>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

#include "ignite/odbc/sql/sql_lexer.h"
#include "ignite/odbc/sql/sql_parser.h"
#include "ignite/odbc/sql/sql_set_streaming_command.h"

#include "odbc_test_suite.h"
#include "test_utils.h"


using namespace ignite;
using namespace ignite_test;

using namespace boost::unit_test;

/**
 * Test setup fixture.
 */
struct SqlParsingTestSuiteFixture : odbc::OdbcTestSuite
{
    /**
     * Constructor.
     */
    SqlParsingTestSuiteFixture()
    {
        grid = StartPlatformNode("queries-test.xml", "NodeMain");
    }

    /**
     * Destructor.
     */
    ~SqlParsingTestSuiteFixture()
    {
        Ignition::StopAll(true);
    }

    /** Node started during the test. */
    Ignite grid;
};

void CheckNextToken(odbc::SqlLexer& lexer, odbc::TokenType::Type tokenType, const std::string& expected)
{
    BOOST_REQUIRE(!lexer.IsEod());

    bool hasNext = lexer.Shift();

    BOOST_CHECK(hasNext);

    const odbc::SqlToken& token = lexer.GetCurrentToken();
    
    BOOST_REQUIRE(token.GetValue());
    BOOST_CHECK_GT(token.GetSize(), 0);
    BOOST_CHECK_EQUAL(token.GetType(), tokenType);
    BOOST_CHECK_EQUAL(token.ToString(), expected);
}

void CheckSetStreamingCommand(
    const std::string& sql,
    bool enabled,
    bool allowOverwrite,
    int32_t batchSize,
    int32_t bufferSizePerNode,
    int32_t parallelOperationsPerNode,
    int64_t flushFrequency,
    bool ordered)
{
    odbc::SqlParser parser(sql);

    std::auto_ptr<odbc::SqlCommand> cmd = parser.GetNextCommand();

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

BOOST_FIXTURE_TEST_SUITE(SqlParsingTestSuite, SqlParsingTestSuiteFixture)

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

    bool hasNext = lexer.Shift();

    BOOST_CHECK(!hasNext);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOff)
{
    CheckSetStreamingCommand("set streaming off", false, false, 2048, 0, 0, 0, false);
    CheckSetStreamingCommand("set streaming 0",   false, false, 2048, 0, 0, 0, false);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOffMixedCase)
{
    CheckSetStreamingCommand("SET Streaming OfF", false, false, 2048, 0, 0, 0, false);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOn)
{
    CheckSetStreamingCommand("set streaming on", true, false, 2048, 0, 0, 0, false);
    CheckSetStreamingCommand("set streaming 1",  true, false, 2048, 0, 0, 0, false);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOnAllowOverwriteOn)
{
    CheckSetStreamingCommand("set streaming on allow_overwrite on", true, true, 2048, 0, 0, 0, false);
    CheckSetStreamingCommand("set streaming on allow_overwrite 1", true, true, 2048, 0, 0, 0, false);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOnAllowOverwriteOff)
{
    CheckSetStreamingCommand("set streaming on allow_overwrite off", true, false, 2048, 0, 0, 0, false);
    CheckSetStreamingCommand("set streaming on allow_overwrite 0", true, false, 2048, 0, 0, 0, false);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOnPageSize512)
{
    CheckSetStreamingCommand("set streaming on batch_size 512", true, false, 512, 0, 0, 0, false);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOnPerNodeBufferSize500)
{
    CheckSetStreamingCommand("set streaming on per_node_buffer_size 500", true, false, 2048, 500, 0, 0, false);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOnPerNodeParallelOperations4)
{
    CheckSetStreamingCommand("set streaming on per_node_parallel_operations 4", true, false, 2048, 0, 4, 0, false);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOnFlushFrequency100)
{
    CheckSetStreamingCommand("set streaming on flush_frequency 100", true, false, 2048, 0, 0, 100, false);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOnOrdered)
{
    CheckSetStreamingCommand("set streaming on ordered", true, false, 2048, 0, 0, 0, true);
}

BOOST_AUTO_TEST_CASE(ParserSetStreamingOnAll)
{
    CheckSetStreamingCommand(
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
    CheckSetStreamingCommand(
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
    CheckSetStreamingCommand(
        "set streaming 1 "
        "ordered "
        "flush_frequency 100 "
        "per_node_parallel_operations 4 "
        "per_node_buffer_size 500 "
        "batch_size 512 "
        "allow_overwrite on ",
        true, true, 512, 500, 4, 100, true);
}

BOOST_AUTO_TEST_SUITE_END()
