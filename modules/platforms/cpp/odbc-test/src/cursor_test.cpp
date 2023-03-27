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

#include <ignite/odbc/system/odbc_constants.h>
#include <ignite/odbc/cursor.h>

using namespace ignite::odbc;

const int64_t testQueryId = 42;

std::auto_ptr<ResultPage> CreateTestPage(bool last, int32_t size)
{
    using namespace ignite::impl::binary;
    using namespace ignite::impl::interop;

    InteropUnpooledMemory mem(1024);
    InteropOutputStream outStream(&mem);
    BinaryWriterImpl writer(&outStream, 0);

    // Last page flag.
    writer.WriteBool(last);

    //Page size.
    writer.WriteInt32(size);

    for (int32_t i = 0; i < size; ++i)
    {
        // Writing row size = 1 column.
        writer.WriteInt32(1);

        // Writing column type.
        writer.WriteInt8(IGNITE_TYPE_INT);

        // Column value.
        writer.WriteInt32(i);
    }

    outStream.Synchronize();

    std::auto_ptr<ResultPage> res(new ResultPage());

    InteropInputStream inStream(&mem);
    BinaryReaderImpl reader(&inStream);

    res->Read(reader);

    BOOST_REQUIRE(res->GetSize() == size);
    BOOST_REQUIRE(res->IsLast() == last);

    return res;
}

void CheckCursorNeedUpdate(Cursor& cursor)
{
    BOOST_REQUIRE(cursor.NeedDataUpdate());

    BOOST_REQUIRE(cursor.HasData());

    BOOST_REQUIRE(!cursor.Increment());

    BOOST_REQUIRE(!cursor.GetRow());
}

void CheckCursorReady(Cursor& cursor)
{
    BOOST_REQUIRE(!cursor.NeedDataUpdate());

    BOOST_REQUIRE(cursor.HasData());

    BOOST_REQUIRE(cursor.GetRow());
}

void CheckCursorEnd(Cursor& cursor)
{
    BOOST_REQUIRE(!cursor.NeedDataUpdate());

    BOOST_REQUIRE(!cursor.HasData());

    BOOST_REQUIRE(!cursor.Increment());

    BOOST_REQUIRE(!cursor.GetRow());
}

BOOST_AUTO_TEST_SUITE(CursorTestSuite)

BOOST_AUTO_TEST_CASE(TestCursorEmpty)
{
    Cursor cursor(testQueryId);

    BOOST_REQUIRE(cursor.GetQueryId() == testQueryId);

    CheckCursorNeedUpdate(cursor);
}

BOOST_AUTO_TEST_CASE(TestCursorLast)
{
    const int32_t pageSize = 16;

    Cursor cursor(testQueryId);

    std::auto_ptr<ResultPage> resultPage = CreateTestPage(true, pageSize);

    cursor.UpdateData(resultPage);

    BOOST_REQUIRE(cursor.GetQueryId() == testQueryId);

    CheckCursorReady(cursor);

    for (int32_t i = 0; i < pageSize; ++i)
        BOOST_REQUIRE(cursor.Increment());

    CheckCursorEnd(cursor);
}

BOOST_AUTO_TEST_CASE(TestCursorUpdate)
{
    const int32_t pageSize = 16;

    Cursor cursor(testQueryId);

    std::auto_ptr<ResultPage> resultPage = CreateTestPage(false, pageSize);

    cursor.UpdateData(resultPage);

    BOOST_REQUIRE(cursor.GetQueryId() == testQueryId);

    for (int32_t i = 0; i < pageSize; ++i)
    {
        CheckCursorReady(cursor);

        BOOST_REQUIRE(cursor.Increment());
    }

    CheckCursorNeedUpdate(cursor);

    resultPage = CreateTestPage(true, pageSize);

    cursor.UpdateData(resultPage);

    CheckCursorReady(cursor);

    for (int32_t i = 0; i < pageSize; ++i)
    {
        CheckCursorReady(cursor);

        BOOST_REQUIRE(cursor.Increment());
    }

    CheckCursorEnd(cursor);
}

BOOST_AUTO_TEST_CASE(TestCursorUpdateOneRow)
{
    Cursor cursor(testQueryId);

    std::auto_ptr<ResultPage> resultPage = CreateTestPage(false, 1);

    cursor.UpdateData(resultPage);

    BOOST_REQUIRE(cursor.GetQueryId() == testQueryId);

    CheckCursorReady(cursor);

    BOOST_REQUIRE(cursor.Increment());

    CheckCursorNeedUpdate(cursor);

    BOOST_REQUIRE(!cursor.Increment());

    resultPage = CreateTestPage(true, 1);

    cursor.UpdateData(resultPage);

    CheckCursorReady(cursor);

    BOOST_REQUIRE(cursor.Increment());

    CheckCursorEnd(cursor);

    BOOST_REQUIRE(!cursor.Increment());
}

BOOST_AUTO_TEST_SUITE_END()
