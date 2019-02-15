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

#include <ignite/impl/binary/binary_writer_impl.h>

#include <ignite/odbc/utility.h>
#include <ignite/common/utils.h>

using namespace ignite::utility;

BOOST_AUTO_TEST_SUITE(UtilityTestSuite)

BOOST_AUTO_TEST_CASE(TestUtilityRemoveSurroundingSpaces)
{
    std::string inStr("   \r \n    \t  some meaningfull data   \n\n   \t  \r  ");
    std::string expectedOutStr("some meaningfull data");

    std::string realOutStr(ignite::common::StripSurroundingWhitespaces(inStr.begin(), inStr.end()));

    BOOST_REQUIRE(expectedOutStr == realOutStr);
}

BOOST_AUTO_TEST_CASE(TestUtilityCopyStringToBuffer)
{
    char buffer[1024];

    std::string str("Some data. And some more data here.");

    CopyStringToBuffer(str, buffer, sizeof(buffer));

    BOOST_REQUIRE(!strcmp(buffer, str.c_str()));

    CopyStringToBuffer(str, buffer, 11);

    BOOST_REQUIRE(!strcmp(buffer, str.substr(0, 10).c_str()));
}

BOOST_AUTO_TEST_CASE(TestUtilityWriteReadString)
{
    using namespace ignite::impl::binary;
    using namespace ignite::impl::interop;

    std::string inStr1("Hello World!");
    std::string inStr2;
    std::string inStr3("Lorem ipsum");

    std::string outStr1;
    std::string outStr2;
    std::string outStr3;
    std::string outStr4;

    ignite::impl::interop::InteropUnpooledMemory mem(1024);
    InteropOutputStream outStream(&mem);
    BinaryWriterImpl writer(&outStream, 0);

    WriteString(writer, inStr1);
    WriteString(writer, inStr2);
    WriteString(writer, inStr3);
    writer.WriteNull();

    outStream.Synchronize();

    InteropInputStream inStream(&mem);
    BinaryReaderImpl reader(&inStream);

    ReadString(reader, outStr1);
    ReadString(reader, outStr2);
    ReadString(reader, outStr3);
    ReadString(reader, outStr4);

    BOOST_REQUIRE(inStr1 == outStr1);
    BOOST_REQUIRE(inStr2 == outStr2);
    BOOST_REQUIRE(inStr3 == outStr3);
    BOOST_REQUIRE(outStr4.empty());
}

BOOST_AUTO_TEST_SUITE_END()
