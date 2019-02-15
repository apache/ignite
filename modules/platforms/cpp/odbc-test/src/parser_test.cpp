/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
