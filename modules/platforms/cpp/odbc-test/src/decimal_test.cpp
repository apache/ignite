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

template<typename T>
void CheckOutputSimple(int64_t val)
{
    T dec(val);

    std::stringstream ss1;
    std::stringstream ss2;

    ss1 << val;
    ss2 << dec;

    BOOST_CHECK_EQUAL(ss1.str(), ss2.str());
}


BOOST_AUTO_TEST_SUITE(DecimalTestSuite)

BOOST_AUTO_TEST_CASE(TestMultiplyBigIntegerArguments)
{
    BigInteger bigInt(12345);

    BigInteger res;

    // 152399025
    bigInt.Multiply(BigInteger(12345), res);

    {
        const BigInteger::MagArray &mag = res.GetMagnitude();

        BOOST_CHECK_EQUAL(mag.GetSize(), 1);

        BOOST_CHECK_EQUAL(mag[0], 152399025ULL);
    }

    // 152399025
    bigInt.Assign(12345LL);
    bigInt.Multiply(bigInt, res);

    {
        const BigInteger::MagArray &mag = res.GetMagnitude();

        BOOST_CHECK_EQUAL(mag.GetSize(), 1);

        BOOST_CHECK_EQUAL(mag[0], 152399025ULL);
    }

    // 152399025
    bigInt.Assign(12345LL);
    bigInt.Multiply(BigInteger(12345), bigInt);

    {
        const BigInteger::MagArray &mag = bigInt.GetMagnitude();

        BOOST_CHECK_EQUAL(mag.GetSize(), 1);

        BOOST_CHECK_EQUAL(mag[0], 152399025ULL);
    }

    // 152399025
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

    // 152399025
    bigInt.Multiply(bigInt, bigInt);

    buf.Assign(bigInt);

    // 3539537889086624823140625
    // 0002 ED86  BBC3 30D1  DDC6 6111
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

    // 152399025
    bigInt.Pow(2);

    {
        const BigInteger::MagArray &mag = bigInt.GetMagnitude();

        BOOST_CHECK_EQUAL(mag.GetSize(), 1);

        BOOST_CHECK_EQUAL(mag[0], 152399025ULL);
    }

    // 3539537889086624823140625
    // 0002 ED86  BBC3 30D1  DDC6 6111
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

    bigInt.Assign(-1LL);

    bigInt.Pow(57298735);
    BOOST_REQUIRE_EQUAL(bigInt.ToInt64(), -1);

    bigInt.Pow(325347312);
    BOOST_REQUIRE_EQUAL(bigInt.ToInt64(), 1);

    bigInt.Assign(2LL);

    bigInt.Pow(10);
    BOOST_REQUIRE_EQUAL(bigInt.ToInt64(), 1024);

    bigInt.Assign(-2LL);

    bigInt.Pow(10);
    BOOST_REQUIRE_EQUAL(bigInt.ToInt64(), 1024);

    bigInt.Assign(2LL);

    bigInt.Pow(11);
    BOOST_REQUIRE_EQUAL(bigInt.ToInt64(), 2048);

    bigInt.Assign(-2LL);

    bigInt.Pow(11);
    BOOST_REQUIRE_EQUAL(bigInt.ToInt64(), -2048);
}

BOOST_AUTO_TEST_CASE(TestMultiplyDivideSimple)
{
    BigInteger bigInt(12345);

    // 152399025
    bigInt.Multiply(bigInt, bigInt);

    // 23225462820950625
    bigInt.Multiply(bigInt, bigInt);

    // 23225462820 and 950625
    BigInteger bi1;
    BigInteger bi2;
    bigInt.Divide(BigInteger(1000000), bi1, bi2);

    // 23225 and 462820
    BigInteger bi3;
    BigInteger bi4;
    bi1.Divide(BigInteger(1000000), bi3, bi4);

    BOOST_CHECK_EQUAL(bi2.ToInt64(), 950625LL);
    BOOST_CHECK_EQUAL(bi3.ToInt64(), 23225LL);
    BOOST_CHECK_EQUAL(bi4.ToInt64(), 462820LL);
}

BOOST_AUTO_TEST_CASE(TestDivideBigger)
{
    BigInteger bigInt(69213205262741);

    BigInteger res;
    BigInteger rem;

    // 4790467782742318458842833081
    bigInt.Pow(2);

    // 4790467782 and 742318458842833081
    bigInt.Divide(BigInteger(1000000000000000000LL), res, rem);

    BOOST_CHECK_EQUAL(res.ToInt64(), 4790467782LL);
    BOOST_CHECK_EQUAL(rem.ToInt64(), 742318458842833081LL);

    // 479046778 and 2742318458842833081
    BigInteger div;
    div.Assign(10000000000000000000ULL);

    bigInt.Divide(div, res, rem);

    BOOST_CHECK_EQUAL(res.ToInt64(), 479046778LL);
    BOOST_CHECK_EQUAL(rem.ToInt64(), 2742318458842833081LL);
}

BOOST_AUTO_TEST_CASE(TestOutputSimpleBigInteger)
{
    CheckOutputSimple<BigInteger>(0);

    CheckOutputSimple<BigInteger>(1);
    CheckOutputSimple<BigInteger>(9);
    CheckOutputSimple<BigInteger>(10);
    CheckOutputSimple<BigInteger>(11);
    CheckOutputSimple<BigInteger>(19);
    CheckOutputSimple<BigInteger>(123);
    CheckOutputSimple<BigInteger>(1234);
    CheckOutputSimple<BigInteger>(12345);
    CheckOutputSimple<BigInteger>(123456);
    CheckOutputSimple<BigInteger>(1234567);
    CheckOutputSimple<BigInteger>(12345678);
    CheckOutputSimple<BigInteger>(123456789);
    CheckOutputSimple<BigInteger>(1234567890);
    CheckOutputSimple<BigInteger>(12345678909);
    CheckOutputSimple<BigInteger>(123456789098);
    CheckOutputSimple<BigInteger>(1234567890987);
    CheckOutputSimple<BigInteger>(12345678909876);
    CheckOutputSimple<BigInteger>(123456789098765);
    CheckOutputSimple<BigInteger>(1234567890987654);
    CheckOutputSimple<BigInteger>(12345678909876543);
    CheckOutputSimple<BigInteger>(123456789098765432);
    CheckOutputSimple<BigInteger>(1234567890987654321);
    CheckOutputSimple<BigInteger>(9999999999999999999LL);
    CheckOutputSimple<BigInteger>(9999999990999999999LL);
    CheckOutputSimple<BigInteger>(10000000000000000000LL);
    CheckOutputSimple<BigInteger>(10000000000000000001LL);
    CheckOutputSimple<BigInteger>(10000000050000000000LL);
    CheckOutputSimple<BigInteger>(INT64_MAX);

    CheckOutputSimple<BigInteger>(-1);
    CheckOutputSimple<BigInteger>(-9);
    CheckOutputSimple<BigInteger>(-10);
    CheckOutputSimple<BigInteger>(-11);
    CheckOutputSimple<BigInteger>(-19);
    CheckOutputSimple<BigInteger>(-123);
    CheckOutputSimple<BigInteger>(-1234);
    CheckOutputSimple<BigInteger>(-12345);
    CheckOutputSimple<BigInteger>(-123456);
    CheckOutputSimple<BigInteger>(-1234567);
    CheckOutputSimple<BigInteger>(-12345678);
    CheckOutputSimple<BigInteger>(-123456789);
    CheckOutputSimple<BigInteger>(-1234567890);
    CheckOutputSimple<BigInteger>(-12345678909);
    CheckOutputSimple<BigInteger>(-123456789098);
    CheckOutputSimple<BigInteger>(-1234567890987);
    CheckOutputSimple<BigInteger>(-12345678909876);
    CheckOutputSimple<BigInteger>(-123456789098765);
    CheckOutputSimple<BigInteger>(-1234567890987654);
    CheckOutputSimple<BigInteger>(-12345678909876543);
    CheckOutputSimple<BigInteger>(-123456789098765432);
    CheckOutputSimple<BigInteger>(-1234567890987654321);
    CheckOutputSimple<BigInteger>(-9999999999999999999LL);
    CheckOutputSimple<BigInteger>(-9999999990999999999LL);
    CheckOutputSimple<BigInteger>(-10000000000000000000LL);
    CheckOutputSimple<BigInteger>(-10000000000000000001LL);
    CheckOutputSimple<BigInteger>(-10000000050000000000LL);
    CheckOutputSimple<BigInteger>(INT64_MIN);
}

BOOST_AUTO_TEST_CASE(TestOutputSimpleDecimal)
{
    CheckOutputSimple<Decimal>(0);

    CheckOutputSimple<Decimal>(1);
    CheckOutputSimple<Decimal>(9);
    CheckOutputSimple<Decimal>(10);
    CheckOutputSimple<Decimal>(11);
    CheckOutputSimple<Decimal>(19);
    CheckOutputSimple<Decimal>(123);
    CheckOutputSimple<Decimal>(1234);
    CheckOutputSimple<Decimal>(12345);
    CheckOutputSimple<Decimal>(123456);
    CheckOutputSimple<Decimal>(1234567);
    CheckOutputSimple<Decimal>(12345678);
    CheckOutputSimple<Decimal>(123456789);
    CheckOutputSimple<Decimal>(1234567890);
    CheckOutputSimple<Decimal>(12345678909);
    CheckOutputSimple<Decimal>(123456789098);
    CheckOutputSimple<Decimal>(1234567890987);
    CheckOutputSimple<Decimal>(12345678909876);
    CheckOutputSimple<Decimal>(123456789098765);
    CheckOutputSimple<Decimal>(1234567890987654);
    CheckOutputSimple<Decimal>(12345678909876543);
    CheckOutputSimple<Decimal>(123456789098765432);
    CheckOutputSimple<Decimal>(1234567890987654321);
    CheckOutputSimple<Decimal>(9999999999999999999LL);
    CheckOutputSimple<Decimal>(9999999990999999999LL);
    CheckOutputSimple<Decimal>(10000000000000000000LL);
    CheckOutputSimple<Decimal>(10000000000000000001LL);
    CheckOutputSimple<Decimal>(10000000050000000000LL);
    CheckOutputSimple<Decimal>(INT64_MAX);

    CheckOutputSimple<Decimal>(-1);
    CheckOutputSimple<Decimal>(-9);
    CheckOutputSimple<Decimal>(-10);
    CheckOutputSimple<Decimal>(-11);
    CheckOutputSimple<Decimal>(-19);
    CheckOutputSimple<Decimal>(-123);
    CheckOutputSimple<Decimal>(-1234);
    CheckOutputSimple<Decimal>(-12345);
    CheckOutputSimple<Decimal>(-123456);
    CheckOutputSimple<Decimal>(-1234567);
    CheckOutputSimple<Decimal>(-12345678);
    CheckOutputSimple<Decimal>(-123456789);
    CheckOutputSimple<Decimal>(-1234567890);
    CheckOutputSimple<Decimal>(-12345678909);
    CheckOutputSimple<Decimal>(-123456789098);
    CheckOutputSimple<Decimal>(-1234567890987);
    CheckOutputSimple<Decimal>(-12345678909876);
    CheckOutputSimple<Decimal>(-123456789098765);
    CheckOutputSimple<Decimal>(-1234567890987654);
    CheckOutputSimple<Decimal>(-12345678909876543);
    CheckOutputSimple<Decimal>(-123456789098765432);
    CheckOutputSimple<Decimal>(-1234567890987654321);
    CheckOutputSimple<Decimal>(-9999999999999999999LL);
    CheckOutputSimple<Decimal>(-9999999990999999999LL);
    CheckOutputSimple<Decimal>(-10000000000000000000LL);
    CheckOutputSimple<Decimal>(-10000000000000000001LL);
    CheckOutputSimple<Decimal>(-10000000050000000000LL);
    CheckOutputSimple<Decimal>(INT64_MIN);
}

BOOST_AUTO_TEST_CASE(TestScalingSmall)
{
    Decimal decimal(12345, 2);
    BOOST_CHECK_EQUAL(decimal.GetPrecision(), 5);

    decimal.SetScale(0, decimal);

    BOOST_CHECK_EQUAL(decimal.GetPrecision(), 3);

    BOOST_CHECK_EQUAL(decimal.ToInt64(), 123);
}

BOOST_AUTO_TEST_CASE(TestScalingBig)
{
    BigInteger bigInt(69213205262741);

    Decimal decimal;
    BOOST_CHECK_EQUAL(decimal.GetPrecision(), 1);

    // 4790467782742318458842833081
    bigInt.Multiply(bigInt, bigInt);

    decimal = Decimal(bigInt, 0);
    BOOST_CHECK_EQUAL(decimal.GetPrecision(), 28);

    // 22948581577492104846692006446607391554788985798427952561
    bigInt.Multiply(bigInt, bigInt);

    decimal = Decimal(bigInt, 0);
    BOOST_CHECK_EQUAL(decimal.GetPrecision(), 56);

    // 22948581577492104846692006446607391554.788985798427952561
    decimal = Decimal(bigInt, 18);

    // 22948581577492104846692006446607391554
    decimal.SetScale(0, decimal);

    BOOST_CHECK_EQUAL(decimal.GetPrecision(), 38);

    // 22948581.577492104846692006446607391554788985798427952561
    decimal = Decimal(bigInt, 48);

    // 22948581
    decimal.SetScale(0, decimal);

    BOOST_CHECK_EQUAL(decimal.GetPrecision(), 8);
    BOOST_CHECK_EQUAL(decimal.ToInt64(), 22948581);

    // 636471904553811060306806140254026286906087997856914463925431295610150712
    // 436301540552945788827650832722026963914694916372255230793492080431332686
    // 268324254350022490844698008329270553114204362445999680199136593689695140
    // 0874934591063287320666899465891248127072522251998904759858801
    bigInt.Pow(5);

    // 63647190455381106.030680614025402628690608799785691446392543129561015071
    // 243630154055294578882765083272202696391469491637225523079349208043133268
    // 626832425435002249084469800832927055311420436244599968019913659368969514
    // 00874934591063287320666899465891248127072522251998904759858801
    decimal = Decimal(bigInt, 260);
    BOOST_CHECK_EQUAL(decimal.GetPrecision(), 277);

    // 63647190455381106
    decimal.SetScale(0, decimal);

    BOOST_CHECK_EQUAL(decimal.GetPrecision(), 17);
    BOOST_CHECK_EQUAL(decimal.ToInt64(), 63647190455381106LL);
}

BOOST_AUTO_TEST_CASE(TestPrecisionSimple)
{
    Decimal test(1);

    BOOST_CHECK_EQUAL(Decimal(-9).GetPrecision(), 1);
    BOOST_CHECK_EQUAL(Decimal(-8).GetPrecision(), 1);
    BOOST_CHECK_EQUAL(Decimal(-7).GetPrecision(), 1);
    BOOST_CHECK_EQUAL(Decimal(-6).GetPrecision(), 1);
    BOOST_CHECK_EQUAL(Decimal(-5).GetPrecision(), 1);
    BOOST_CHECK_EQUAL(Decimal(-4).GetPrecision(), 1);
    BOOST_CHECK_EQUAL(Decimal(-3).GetPrecision(), 1);
    BOOST_CHECK_EQUAL(Decimal(-2).GetPrecision(), 1);
    BOOST_CHECK_EQUAL(Decimal(-1).GetPrecision(), 1);
    BOOST_CHECK_EQUAL(Decimal(0).GetPrecision(), 1);
    BOOST_CHECK_EQUAL(Decimal(1).GetPrecision(), 1);
    BOOST_CHECK_EQUAL(Decimal(2).GetPrecision(), 1);
    BOOST_CHECK_EQUAL(Decimal(3).GetPrecision(), 1);
    BOOST_CHECK_EQUAL(Decimal(4).GetPrecision(), 1);
    BOOST_CHECK_EQUAL(Decimal(5).GetPrecision(), 1);
    BOOST_CHECK_EQUAL(Decimal(6).GetPrecision(), 1);
    BOOST_CHECK_EQUAL(Decimal(7).GetPrecision(), 1);
    BOOST_CHECK_EQUAL(Decimal(8).GetPrecision(), 1);
    BOOST_CHECK_EQUAL(Decimal(9).GetPrecision(), 1);

    BOOST_CHECK_EQUAL(Decimal(2147483648LL).GetPrecision(),           10); // 2^31:       10 digits
    BOOST_CHECK_EQUAL(Decimal(-2147483648LL).GetPrecision(),          10); // -2^31:      10 digits
    BOOST_CHECK_EQUAL(Decimal(98893745455LL).GetPrecision(),          11); // random:     11 digits
    BOOST_CHECK_EQUAL(Decimal(3455436789887LL).GetPrecision(),        13); // random:     13 digits
    BOOST_CHECK_EQUAL(Decimal(140737488355328LL).GetPrecision(),      15); // 2^47:       15 digits
    BOOST_CHECK_EQUAL(Decimal(-140737488355328LL).GetPrecision(),     15); // -2^47:      15 digits
    BOOST_CHECK_EQUAL(Decimal(7564232235739573LL).GetPrecision(),     16); // random:     16 digits
    BOOST_CHECK_EQUAL(Decimal(25335434990002322LL).GetPrecision(),    17); // random:     17 digits
    BOOST_CHECK_EQUAL(Decimal(9223372036854775807LL).GetPrecision(),  19); // 2^63 - 1:   19 digits
    BOOST_CHECK_EQUAL(Decimal(-9223372036854775807LL).GetPrecision(), 19); // -2^63 + 1:  19 digits
}

BOOST_AUTO_TEST_CASE(TestPrecisionChange)
{
    BigInteger bigInt(32421);

    //75946938183
    bigInt.Multiply(BigInteger(2342523), bigInt);

    //4244836901495581620
    bigInt.Multiply(BigInteger(55892140), bigInt);

    //1361610054778960404282184020
    bigInt.Multiply(BigInteger(320768521), bigInt);

    //1454144449122723409814375680476734820
    bigInt.Multiply(BigInteger(1067959541), bigInt);

    //117386322514277938455905731466723946155156640
    bigInt.Multiply(BigInteger(80725352), bigInt);

    //1173863225142779384559.05731466723946155156640
    Decimal decimal(bigInt, 23);

    BOOST_CHECK_EQUAL(decimal.GetPrecision(), 45);
    BOOST_CHECK_EQUAL(decimal.GetScale(), 23);

    for (int32_t i = 0; i < decimal.GetScale(); ++i)
    {
        decimal.SetScale(i, decimal);

        BOOST_CHECK_EQUAL(decimal.GetPrecision(), decimal.GetPrecision() - decimal.GetScale() + i);
    }
}

BOOST_AUTO_TEST_SUITE_END()