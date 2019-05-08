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

#include <ignite/odbc/parser.h>

using namespace ignite::odbc;

struct TestMessage
{
    TestMessage() : a(0), b()
    {
        // No-op.
    }

    TestMessage(int32_t a, const std::string& b) : a(a), b(b)
    {
        // No-op.
    }

    ~TestMessage()
    {
        // No-op.
    }

    void Write(ignite::impl::binary::BinaryWriterImpl& writer, const ProtocolVersion&) const
    {
        writer.WriteInt32(a);
        writer.WriteString(b.data(), static_cast<int32_t>(b.size()));
    }

    void Read(ignite::impl::binary::BinaryReaderImpl& reader, const ProtocolVersion&)
    {
        a = reader.ReadInt32();

        b.resize(reader.ReadString(0, 0));
        reader.ReadString(&b[0], static_cast<int32_t>(b.size()));
    }

    int32_t a;
    std::string b;
};

bool operator==(const TestMessage& lhs, const TestMessage& rhs)
{
    return lhs.a == rhs.a &&
           lhs.b == rhs.b;
}

BOOST_AUTO_TEST_SUITE(ParserTestSuite)

BOOST_AUTO_TEST_CASE(TestParserEncodeDecode)
{
    Parser parser;

    std::vector<int8_t> buffer;

    TestMessage outMsg(42, "Test message");
    TestMessage inMsg;

    parser.Encode(outMsg, buffer);

    parser.Decode(inMsg, buffer);

    BOOST_REQUIRE(outMsg == inMsg);
}

BOOST_AUTO_TEST_SUITE_END()
