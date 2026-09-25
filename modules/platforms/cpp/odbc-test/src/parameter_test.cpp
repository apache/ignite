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

#include <vector>

#include <boost/test/unit_test.hpp>

#include <ignite/impl/binary/binary_common.h>
#include <ignite/impl/binary/binary_writer_impl.h>

#include "ignite/odbc/system/odbc_constants.h"
#include "ignite/odbc/app/parameter.h"

using namespace ignite::impl::binary;
using namespace ignite::impl::interop;
using namespace ignite::odbc;
using namespace ignite::odbc::app;

namespace
{
    /** Binary type headers of the values a parameter has been written as. */
    typedef std::vector<int8_t> HeaderVector;

    /**
     * Write a parameter once for every row of a parameter array and collect the
     * binary type header of every written value.
     *
     * @param param Parameter to write.
     * @param rows Number of rows in the parameter array.
     * @param headers Type headers of the written values.
     */
    void WriteParameterRows(const Parameter& param, SqlUlen rows, HeaderVector& headers)
    {
        InteropUnpooledMemory mem(4096);
        InteropOutputStream outStream(&mem);
        BinaryWriterImpl writer(&outStream, 0);

        std::vector<int32_t> positions;

        for (SqlUlen i = 0; i < rows; ++i)
        {
            positions.push_back(outStream.Position());

            param.Write(writer, 0, i);
        }

        outStream.Synchronize();

        InteropInputStream inStream(&mem);

        headers.clear();

        for (size_t i = 0; i < positions.size(); ++i)
        {
            inStream.Position(positions[i]);

            headers.push_back(inStream.ReadInt8());
        }
    }
}

BOOST_AUTO_TEST_SUITE(ParameterTestSuite)

BOOST_AUTO_TEST_CASE(TestStringArrayNullBelowFirstRow)
{
    char values[3][8] = { "r1", "r2", "r3" };
    SqlLen lengths[3] = { SQL_NTS, SQL_NULL_DATA, SQL_NTS };

    ApplicationDataBuffer buffer(type_traits::OdbcNativeType::AI_CHAR,
        &values[0][0], sizeof(values[0]), &lengths[0]);

    Parameter param(buffer, SQL_VARCHAR, sizeof(values[0]), 0);

    HeaderVector headers;

    WriteParameterRows(param, 3, headers);

    BOOST_REQUIRE_EQUAL(headers.size(), 3u);

    BOOST_CHECK_EQUAL(headers[0], IGNITE_TYPE_STRING);
    BOOST_CHECK_EQUAL(headers[1], IGNITE_HDR_NULL);
    BOOST_CHECK_EQUAL(headers[2], IGNITE_TYPE_STRING);
}

BOOST_AUTO_TEST_CASE(TestStringArrayNullInFirstRow)
{
    char values[3][8] = { "r1", "r2", "r3" };
    SqlLen lengths[3] = { SQL_NULL_DATA, SQL_NTS, SQL_NTS };

    ApplicationDataBuffer buffer(type_traits::OdbcNativeType::AI_CHAR,
        &values[0][0], sizeof(values[0]), &lengths[0]);

    Parameter param(buffer, SQL_VARCHAR, sizeof(values[0]), 0);

    HeaderVector headers;

    WriteParameterRows(param, 3, headers);

    BOOST_REQUIRE_EQUAL(headers.size(), 3u);

    BOOST_CHECK_EQUAL(headers[0], IGNITE_HDR_NULL);
    BOOST_CHECK_EQUAL(headers[1], IGNITE_TYPE_STRING);
    BOOST_CHECK_EQUAL(headers[2], IGNITE_TYPE_STRING);
}

BOOST_AUTO_TEST_CASE(TestBinaryArrayNullBelowFirstRow)
{
    int8_t values[3][4] = { { 1, 2, 3, 4 }, { 5, 6, 7, 8 }, { 9, 10, 11, 12 } };
    SqlLen lengths[3] = { 4, SQL_NULL_DATA, 4 };

    ApplicationDataBuffer buffer(type_traits::OdbcNativeType::AI_BINARY,
        &values[0][0], sizeof(values[0]), &lengths[0]);

    Parameter param(buffer, SQL_BINARY, sizeof(values[0]), 0);

    HeaderVector headers;

    WriteParameterRows(param, 3, headers);

    BOOST_REQUIRE_EQUAL(headers.size(), 3u);

    BOOST_CHECK_EQUAL(headers[0], IGNITE_TYPE_ARRAY_BYTE);
    BOOST_CHECK_EQUAL(headers[1], IGNITE_HDR_NULL);
    BOOST_CHECK_EQUAL(headers[2], IGNITE_TYPE_ARRAY_BYTE);
}

BOOST_AUTO_TEST_SUITE_END()
