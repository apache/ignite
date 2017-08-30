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

#ifndef _MSC_VER
#   define BOOST_TEST_DYN_LINK
#endif

#include <string>

#include <boost/test/unit_test.hpp>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

using namespace ignite;
using namespace impl::interop;
using namespace impl::binary;
using namespace boost::unit_test;

template<typename T>
void WriteReadType(const T& val, void(*wc)(InteropOutputStream*, T), T(*rc)(InteropInputStream*))
{
    InteropUnpooledMemory mem(1024);
    InteropOutputStream out(&mem);

    BOOST_CHECKPOINT("Writing attempt");
    wc(&out, val);

    out.Synchronize();

    InteropInputStream in(&mem);

    BOOST_CHECKPOINT("Reading attempt");
    T res = rc(&in);

    BOOST_CHECKPOINT("Comparation and output");
    BOOST_CHECK_EQUAL(val, res);
}

template<typename T>
void WriteReadType(const T& val, void(*wc)(InteropOutputStream*, const T&), void(*rc)(InteropInputStream*, T&))
{
    InteropUnpooledMemory mem(1024);
    InteropOutputStream out(&mem);

    BOOST_CHECKPOINT("Writing attempt");
    wc(&out, val);

    out.Synchronize();

    InteropInputStream in(&mem);

    T res;
    BOOST_CHECKPOINT("Reading attempt");
    rc(&in, res);

    BOOST_CHECKPOINT("Comparation and output");
    BOOST_CHECK_EQUAL(val, res);
}

void WriteReadUnsignedVarint(int32_t val)
{
    WriteReadType(val, BinaryUtils::WriteUnsignedVarint, BinaryUtils::ReadUnsignedVarint);
}

void WriteReadDecimal(const common::Decimal& val)
{
    WriteReadType(val, BinaryUtils::WriteDecimal, BinaryUtils::ReadDecimal);
}

void WriteReadString(const std::string& val)
{
    WriteReadType(val, BinaryUtils::WriteString, BinaryUtils::ReadString);
}

BOOST_AUTO_TEST_SUITE(BinaryUtilsTestSuite)

BOOST_AUTO_TEST_CASE(UnsignedVarintWriteReadTest_0)
{
    WriteReadUnsignedVarint(0);
}

BOOST_AUTO_TEST_CASE(UnsignedVarintWriteReadTest_1)
{
    WriteReadUnsignedVarint(1);
}

BOOST_AUTO_TEST_CASE(UnsignedVarintWriteReadTest_42)
{
    WriteReadUnsignedVarint(42);
}

BOOST_AUTO_TEST_CASE(UnsignedVarintWriteReadTest_100)
{
    WriteReadUnsignedVarint(100);
}

BOOST_AUTO_TEST_CASE(UnsignedVarintWriteReadTest_100000000)
{
    WriteReadUnsignedVarint(100000000);
}

BOOST_AUTO_TEST_CASE(UnsignedVarintWriteReadTest_999999999)
{
    WriteReadUnsignedVarint(999999999);
}

BOOST_AUTO_TEST_CASE(UnsignedVarintWriteReadTest_1926348257)
{
    WriteReadUnsignedVarint(1926348257);
}

BOOST_AUTO_TEST_CASE(UnsignedVarintWriteReadTest_N1)
{
    WriteReadUnsignedVarint(-1);
}

BOOST_AUTO_TEST_CASE(UnsignedVarintWriteReadTest_N42)
{
    WriteReadUnsignedVarint(-42);
}

BOOST_AUTO_TEST_CASE(UnsignedVarintWriteReadTest_N100)
{
    WriteReadUnsignedVarint(-100);
}

BOOST_AUTO_TEST_CASE(UnsignedVarintWriteReadTest_N100000000)
{
    WriteReadUnsignedVarint(-100000000);
}

BOOST_AUTO_TEST_CASE(UnsignedVarintWriteReadTest_N999999999)
{
    WriteReadUnsignedVarint(-999999999);
}

BOOST_AUTO_TEST_CASE(UnsignedVarintWriteReadTest_N1926348257)
{
    WriteReadUnsignedVarint(-1926348257);
}

BOOST_AUTO_TEST_CASE(DecimalWriteReadTest_Default)
{
    WriteReadDecimal(common::Decimal());
}

BOOST_AUTO_TEST_CASE(DecimalWriteReadTest_0)
{
    WriteReadDecimal(common::Decimal(0));
}

BOOST_AUTO_TEST_CASE(DecimalWriteReadTest_1)
{
    WriteReadDecimal(common::Decimal(1));
}

BOOST_AUTO_TEST_CASE(DecimalWriteReadTest_42)
{
    WriteReadDecimal(common::Decimal(42));
}

BOOST_AUTO_TEST_CASE(DecimalWriteReadTest_100)
{
    WriteReadDecimal(common::Decimal(100));
}

BOOST_AUTO_TEST_CASE(DecimalWriteReadTest_100000000)
{
    WriteReadDecimal(common::Decimal(100000000));
}

BOOST_AUTO_TEST_CASE(DecimalWriteReadTest_999999999)
{
    WriteReadDecimal(common::Decimal(999999999));
}

BOOST_AUTO_TEST_CASE(DecimalWriteReadTest_1926348257)
{
    WriteReadDecimal(common::Decimal(1926348257));
}

BOOST_AUTO_TEST_CASE(DecimalWriteReadTest_23905684076350439_6529643986)
{
    WriteReadDecimal(common::Decimal("23905684076350439.6529643986"));
}

BOOST_AUTO_TEST_CASE(DecimalWriteReadTest_394865902635896426758746893625963458967946783645868346258768374658763284_017395679043650265460235602345)
{
    WriteReadDecimal(common::Decimal("394865902635896426758746893625963458967946783645868346258768374658763284.017395679043650265460235602345"));
}

BOOST_AUTO_TEST_CASE(DecimalWriteReadTest_99999999999999999999999999999999999999999999999999_9999999999999999999999999999999999999999999999999999999)
{
    WriteReadDecimal(common::Decimal("99999999999999999999999999999999999999999999999999.9999999999999999999999999999999999999999999999999999999"));
}

BOOST_AUTO_TEST_CASE(DecimalWriteReadTest_0_00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001)
{
    WriteReadDecimal(common::Decimal("0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"));
}

BOOST_AUTO_TEST_CASE(DecimalWriteReadTest_N1)
{
    WriteReadDecimal(common::Decimal(-1));
}

BOOST_AUTO_TEST_CASE(DecimalWriteReadTest_N42)
{
    WriteReadDecimal(common::Decimal(-42));
}

BOOST_AUTO_TEST_CASE(DecimalWriteReadTest_N100)
{
    WriteReadDecimal(common::Decimal(-100));
}

BOOST_AUTO_TEST_CASE(DecimalWriteReadTest_N100000000)
{
    WriteReadDecimal(common::Decimal(-100000000));
}

BOOST_AUTO_TEST_CASE(DecimalWriteReadTest_N999999999)
{
    WriteReadDecimal(common::Decimal(-999999999));
}

BOOST_AUTO_TEST_CASE(DecimalWriteReadTest_N1926348257)
{
    WriteReadDecimal(common::Decimal(-1926348257));
}

BOOST_AUTO_TEST_CASE(DecimalWriteReadTest_N23905684076350439_6529643986)
{
    WriteReadDecimal(common::Decimal("-23905684076350439.6529643986"));
}

BOOST_AUTO_TEST_CASE(DecimalWriteReadTest_N394865902635896426758746893625963458967946783645868346258768374658763284_017395679043650265460235602345)
{
    WriteReadDecimal(common::Decimal("-394865902635896426758746893625963458967946783645868346258768374658763284.017395679043650265460235602345"));
}

BOOST_AUTO_TEST_CASE(DecimalWriteReadTest_N99999999999999999999999999999999999999999999999999_9999999999999999999999999999999999999999999999999999999)
{
    WriteReadDecimal(common::Decimal("-99999999999999999999999999999999999999999999999999.9999999999999999999999999999999999999999999999999999999"));
}

BOOST_AUTO_TEST_CASE(DecimalWriteReadTest_N0_00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001)
{
    WriteReadDecimal(common::Decimal("-0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"));
}

BOOST_AUTO_TEST_CASE(StringWriteReadTest_Empty)
{
    WriteReadString("");
}

BOOST_AUTO_TEST_CASE(StringWriteReadTest_Short)
{
    WriteReadString("Lorem ipsum");
}

BOOST_AUTO_TEST_CASE(StringWriteReadTest_Long)
{
    WriteReadString("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Fusce vestibulum dui erat. Maecenas "
        "auctor venenatis pharetra. Nunc vitae posuere lectus. Sed nec ante ultricies lorem volutpat vehicula. Vivamus "
        "eu sollicitudin diam. Orci varius natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. "
        "Aenean sagittis auctor ipsum a aliquet. Sed ac lectus ex.");
}

BOOST_AUTO_TEST_CASE(StringWriteReadTest_VeryLong)
{
    std::string val(200000, '.');
    WriteReadString(val);
}

BOOST_AUTO_TEST_SUITE_END()
