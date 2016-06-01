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

#include <boost/test/unit_test.hpp>

#include <ignite/common/bits.h>

#include "ignite/odbc/decimal.h"

using namespace ignite;
using namespace ignite::common::bits;

BOOST_AUTO_TEST_SUITE(DecimalTestSuite)

BOOST_AUTO_TEST_CASE(TestMultiplyBigIntegerArguments)
{
    BigInteger bigInt(12345);

    BigInteger res;

    //152399025
    bigInt.Multiply(BigInteger(12345), res);

    {
        const BigInteger::MagArray &mag = res.GetMagnitude();

        BOOST_CHECK_EQUAL(mag.GetSize(), 1);

        BOOST_CHECK_EQUAL(mag[0], 152399025ULL);
    }

    //152399025
    bigInt.Assign(12345LL);
    bigInt.Multiply(bigInt, res);

    {
        const BigInteger::MagArray &mag = res.GetMagnitude();

        BOOST_CHECK_EQUAL(mag.GetSize(), 1);

        BOOST_CHECK_EQUAL(mag[0], 152399025ULL);
    }

    //152399025
    bigInt.Assign(12345LL);
    bigInt.Multiply(BigInteger(12345), bigInt);

    {
        const BigInteger::MagArray &mag = bigInt.GetMagnitude();

        BOOST_CHECK_EQUAL(mag.GetSize(), 1);

        BOOST_CHECK_EQUAL(mag[0], 152399025ULL);
    }

    //152399025
    bigInt.Assign(12345LL);
    bigInt.Multiply(bigInt, bigInt);

    {
        const BigInteger::MagArray &mag = bigInt.GetMagnitude();

        BOOST_CHECK_EQUAL(mag.GetSize(), 1);

        BOOST_CHECK_EQUAL(mag[0], 152399025ULL);
    }
}

BOOST_AUTO_TEST_CASE(TestMultiplyBigIntegerBigger)
{
    BigInteger bigInt(12345);

    BigInteger buf;

    //152399025
    bigInt.Multiply(bigInt, bigInt);

    buf.Assign(bigInt);

    //3539537889086624823140625 == 0002 ED86  BBC3 30D1  DDC6 6111
    bigInt.Multiply(buf, bigInt);
    bigInt.Multiply(buf, bigInt);

    {
        const BigInteger::MagArray &mag = bigInt.GetMagnitude();

        BOOST_CHECK_EQUAL(mag.GetSize(), 3);

        BOOST_CHECK_EQUAL(mag[0], 0xDDC66111);
        BOOST_CHECK_EQUAL(mag[1], 0xBBC330D1);
        BOOST_CHECK_EQUAL(mag[2], 0x0002ED86);
    }

    // 2698355789040138398691723863616167551412718750 ==
    // 0078 FF9A  F760 4E12  4A1F 3179  D038 D455  630F CC9E
    bigInt.Multiply(BigInteger(32546826734), bigInt);
    bigInt.Multiply(BigInteger(23423079641), bigInt);

    {
        const BigInteger::MagArray &mag = bigInt.GetMagnitude();

        BOOST_CHECK_EQUAL(mag.GetSize(), 5);

        BOOST_CHECK_EQUAL(mag[0], 0x630FCC9E);
        BOOST_CHECK_EQUAL(mag[1], 0xD038D455);
        BOOST_CHECK_EQUAL(mag[2], 0x4A1F3179);
        BOOST_CHECK_EQUAL(mag[3], 0xF7604E12);
        BOOST_CHECK_EQUAL(mag[4], 0x0078FF9A);
    }
}

BOOST_AUTO_TEST_CASE(TestPowBigInteger)
{
    BigInteger bigInt(12345);

    {
        const BigInteger::MagArray &mag = bigInt.GetMagnitude();

        BOOST_CHECK_EQUAL(mag.GetSize(), 1);

        BOOST_CHECK_EQUAL(mag[0], 12345);
    }

    //152399025
    bigInt.Pow(2);

    {
        const BigInteger::MagArray &mag = bigInt.GetMagnitude();

        BOOST_CHECK_EQUAL(mag.GetSize(), 1);

        BOOST_CHECK_EQUAL(mag[0], 152399025ULL);
    }

    //3539537889086624823140625 ==  0002 ED86  BBC3 30D1  DDC6 6111
    bigInt.Pow(3);

    {
        const BigInteger::MagArray &mag = bigInt.GetMagnitude();

        BOOST_CHECK_EQUAL(mag.GetSize(), 3);

        BOOST_CHECK_EQUAL(mag[0], 0xDDC66111);
        BOOST_CHECK_EQUAL(mag[1], 0xBBC330D1);
        BOOST_CHECK_EQUAL(mag[2], 0x0002ED86);
    }

    //3086495556566025694024226933269611093366465997140345415945924110519533775
    //2241867322136254278528975546698722592953892009291022792452635153187272387
    //9105398830363346664660724134489229239181447334384883937966927158758068117
    //094808258116245269775390625
    //
    //                                             0000 B4D0  1355 772E
    // C174 C5F3  B840 74ED  6A54 B544  48E1 E308  6A80 6050  7D37 A56F
    // 54E6 FF91  13FF 7B0A  455C F649  F4CD 37D0  C5B0 0507  1BFD 9083 
    // 8F13 08B4  D962 08FC  FBC0 B5AB  F9F9 06C9  94B3 9715  8C43 C94F
    // 4891 09E5  57AA 66C9  A4F4 3494  A938 89FE  87AF 9056  7D90 17A1
    bigInt.Pow(10);

    {
        const BigInteger::MagArray &mag = bigInt.GetMagnitude();

        BOOST_CHECK_EQUAL(mag.GetSize(), 26);

        BOOST_CHECK_EQUAL(mag[0],  0x7D9017A1);
        BOOST_CHECK_EQUAL(mag[1],  0x87AF9056);
        BOOST_CHECK_EQUAL(mag[2],  0xA93889FE);
        BOOST_CHECK_EQUAL(mag[3],  0xA4F43494);
        BOOST_CHECK_EQUAL(mag[4],  0x57AA66C9);
        BOOST_CHECK_EQUAL(mag[5],  0x489109E5);
        BOOST_CHECK_EQUAL(mag[6],  0x8C43C94F);
        BOOST_CHECK_EQUAL(mag[7],  0x94B39715);
        BOOST_CHECK_EQUAL(mag[8],  0xF9F906C9);
        BOOST_CHECK_EQUAL(mag[9],  0xFBC0B5AB);
        BOOST_CHECK_EQUAL(mag[10], 0xD96208FC);
        BOOST_CHECK_EQUAL(mag[11], 0x8F1308B4);
        BOOST_CHECK_EQUAL(mag[12], 0x1BFD9083);
        BOOST_CHECK_EQUAL(mag[13], 0xC5B00507);
        BOOST_CHECK_EQUAL(mag[14], 0xF4CD37D0);
        BOOST_CHECK_EQUAL(mag[15], 0x455CF649);
        BOOST_CHECK_EQUAL(mag[16], 0x13FF7B0A);
        BOOST_CHECK_EQUAL(mag[17], 0x54E6FF91);
        BOOST_CHECK_EQUAL(mag[18], 0x7D37A56F);
        BOOST_CHECK_EQUAL(mag[19], 0x6A806050);
        BOOST_CHECK_EQUAL(mag[20], 0x48E1E308);
        BOOST_CHECK_EQUAL(mag[21], 0x6A54B544);
        BOOST_CHECK_EQUAL(mag[22], 0xB84074ED);
        BOOST_CHECK_EQUAL(mag[23], 0xC174C5F3);
        BOOST_CHECK_EQUAL(mag[24], 0x1355772E);
        BOOST_CHECK_EQUAL(mag[25], 0x0000B4D0);
    }
}

BOOST_AUTO_TEST_SUITE_END()