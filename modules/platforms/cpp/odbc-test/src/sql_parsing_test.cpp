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
#include "ignite/impl/binary/binary_utils.h"

#include "ignite/odbc/sql/sql_lexer.h"

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

BOOST_FIXTURE_TEST_SUITE(SqlParsingTestSuite, SqlParsingTestSuiteFixture)

BOOST_AUTO_TEST_CASE(AllKindOfTokens)
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

BOOST_AUTO_TEST_SUITE_END()
