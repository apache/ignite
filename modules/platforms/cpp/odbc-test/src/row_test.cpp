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

#include <ignite/impl/binary/binary_writer_impl.h>

#include "ignite/odbc/diagnostic/diagnosable_adapter.h"
#include "ignite/odbc/system/odbc_constants.h"
#include "ignite/odbc/row.h"

using namespace ignite::odbc::app;
using namespace ignite::odbc;


std::string GetStrColumnValue(size_t rowIdx)
{
    std::stringstream generator;
    generator << "Column 2 test string, row num: "
              << rowIdx << ". Some trailing bytes";

    return generator.str();
}

void FillMemWithData(ignite::impl::interop::InteropUnpooledMemory& mem, size_t rowNum)
{
    using namespace ignite::impl::binary;
    using namespace ignite::impl::interop;
    
    InteropOutputStream stream(&mem);
    BinaryWriterImpl writer(&stream, 0);

    for (size_t i = 0; i < rowNum; ++i)
    {
        // Number of columns in page.
        writer.WriteInt32(4);

        // First column is int.
        writer.WriteInt8(IGNITE_TYPE_LONG);
        writer.WriteInt64(static_cast<int64_t>(i * 10));

        // Second column is string.
        const std::string& str(GetStrColumnValue(i));

        writer.WriteString(str.data(), static_cast<int32_t>(str.size()));

        // Third column is GUID.
        ignite::Guid guid(0x2b218f63642a4a64UL, 0x9674098f388ac298UL + i);

        writer.WriteGuid(guid);

        // The last column is bool.
        writer.WriteInt8(IGNITE_TYPE_BOOL);
        writer.WriteBool(i % 2 == 1);
    }

    stream.Synchronize();
}

void CheckRowData(Row& row, size_t rowIdx)
{
    SqlLen reslen;

    SQLINTEGER longBuf;
    char strBuf[1024];
    SQLGUID guidBuf;
    char bitBuf;

    ApplicationDataBuffer appLongBuf(type_traits::OdbcNativeType::AI_SIGNED_LONG, &longBuf, sizeof(longBuf), &reslen);
    ApplicationDataBuffer appStrBuf(type_traits::OdbcNativeType::AI_CHAR, &strBuf, sizeof(strBuf), &reslen);
    ApplicationDataBuffer appGuidBuf(type_traits::OdbcNativeType::AI_GUID, &guidBuf, sizeof(guidBuf), &reslen);
    ApplicationDataBuffer appBitBuf(type_traits::OdbcNativeType::AI_BIT, &bitBuf, sizeof(bitBuf), &reslen);

    // Checking size.
    BOOST_REQUIRE(row.GetSize() == 4);

    // Checking 1st column.
    BOOST_REQUIRE(row.ReadColumnToBuffer(1, appLongBuf) == ConversionResult::AI_SUCCESS);
    BOOST_REQUIRE_EQUAL(static_cast<size_t>(longBuf), rowIdx * 10);

    // Checking 2nd column.
    BOOST_REQUIRE(row.ReadColumnToBuffer(2, appStrBuf) == ConversionResult::AI_SUCCESS);

    std::string strReal(strBuf, static_cast<size_t>(reslen));
    std::string strExpected(GetStrColumnValue(rowIdx));

    BOOST_REQUIRE(strReal == strExpected);

    // Checking 3rd column.
    BOOST_REQUIRE(row.ReadColumnToBuffer(3, appGuidBuf) == ConversionResult::AI_SUCCESS);

    BOOST_REQUIRE(guidBuf.Data1 == 0x2b218f63UL);
    BOOST_REQUIRE(guidBuf.Data2 == 0x642aU);
    BOOST_REQUIRE(guidBuf.Data3 == 0x4a64U);

    BOOST_REQUIRE(guidBuf.Data4[0] == 0x96);
    BOOST_REQUIRE(guidBuf.Data4[1] == 0x74);
    BOOST_REQUIRE(guidBuf.Data4[2] == 0x09);
    BOOST_REQUIRE(guidBuf.Data4[3] == 0x8f);
    BOOST_REQUIRE(guidBuf.Data4[4] == 0x38);
    BOOST_REQUIRE(guidBuf.Data4[5] == 0x8a);
    BOOST_REQUIRE(guidBuf.Data4[6] == 0xc2);
    BOOST_REQUIRE(guidBuf.Data4[7] == 0x98 + rowIdx);

    // Checking 4th column.
    BOOST_REQUIRE(row.ReadColumnToBuffer(4, appBitBuf) == ConversionResult::AI_SUCCESS);
    BOOST_REQUIRE_EQUAL(static_cast<size_t>(bitBuf), rowIdx % 2);
}


BOOST_AUTO_TEST_SUITE(RowTestSuite)

BOOST_AUTO_TEST_CASE(TestRowMoveToNext)
{
    ignite::impl::interop::InteropUnpooledMemory mem(4096);

    const size_t rowNum = 32;

    FillMemWithData(mem, rowNum);

    Row row(mem);

    for (size_t i = 0; i < rowNum - 1; ++i)
    {
        BOOST_REQUIRE(row.GetSize() == 4);

        BOOST_REQUIRE(row.MoveToNext());
    }

    BOOST_REQUIRE(row.GetSize() == 4);
}

BOOST_AUTO_TEST_CASE(TestRowRead)
{
    ignite::impl::interop::InteropUnpooledMemory mem(4096);

    const size_t rowNum = 8;

    FillMemWithData(mem, rowNum);

    Row row(mem);

    BOOST_REQUIRE(row.GetSize() == 4);

    for (size_t i = 0; i < rowNum - 1; ++i)
    {
        CheckRowData(row, i);

        BOOST_REQUIRE(row.MoveToNext());
    }

    CheckRowData(row, rowNum - 1);
}

BOOST_AUTO_TEST_CASE(TestSingleRow)
{
    ignite::impl::interop::InteropUnpooledMemory mem(4096);

    FillMemWithData(mem, 1);

    Row row(mem);

    BOOST_REQUIRE(row.GetSize() == 4);

    CheckRowData(row, 0);
}

BOOST_AUTO_TEST_CASE(TestTwoRows)
{
    ignite::impl::interop::InteropUnpooledMemory mem(4096);

    FillMemWithData(mem, 2);

    Row row(mem);

    BOOST_REQUIRE(row.GetSize() == 4);

    CheckRowData(row, 0);

    BOOST_REQUIRE(row.MoveToNext());

    BOOST_REQUIRE(row.GetSize() == 4);

    CheckRowData(row, 1);
}

BOOST_AUTO_TEST_SUITE_END()
