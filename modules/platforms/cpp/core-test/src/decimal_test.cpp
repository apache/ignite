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

#include <ignite/common/bits.h>
#include <ignite/common/decimal.h>

using namespace ignite;
using namespace ignite::common;
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

template<typename T>
void CheckInputOutput(const std::string& val)
{
    T dec;
    std::string res;

    std::stringstream ss1;
    ss1 << val;
    ss1 >> dec;

    std::stringstream ss2;
    ss2 << dec;
    res = ss2.str();

    BOOST_CHECK_EQUAL(val, res);
}

template<typename T>
void CheckOutputInput(const T& val)
{
    T dec;
    std::stringstream ss;
    ss << val;
    ss >> dec;

    BOOST_CHECK_EQUAL(val, dec);
}

void CheckDoubleCast(double val)
{
    Decimal dec;

    dec.AssignDouble(val);

    BOOST_CHECK_CLOSE(val, dec.ToDouble(), 1E-10);
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

        BOOST_CHECK_EQUAL(mag[0], 152399025UL);
    }

    // 152399025
    bigInt.AssignInt64(12345);
    bigInt.Multiply(bigInt, res);

    {
        const BigInteger::MagArray &mag = res.GetMagnitude();

        BOOST_CHECK_EQUAL(mag.GetSize(), 1);

        BOOST_CHECK_EQUAL(mag[0], 152399025UL);
    }

    // 152399025
    bigInt.AssignInt64(12345);
    bigInt.Multiply(BigInteger(12345), bigInt);

    {
        const BigInteger::MagArray &mag = bigInt.GetMagnitude();

        BOOST_CHECK_EQUAL(mag.GetSize(), 1);

        BOOST_CHECK_EQUAL(mag[0], 152399025UL);
    }

    // 152399025
    bigInt.AssignInt64(12345);
    bigInt.Multiply(bigInt, bigInt);

    {
        const BigInteger::MagArray &mag = bigInt.GetMagnitude();

        BOOST_CHECK_EQUAL(mag.GetSize(), 1);

        BOOST_CHECK_EQUAL(mag[0], 152399025UL);
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

        BOOST_CHECK_EQUAL(mag[0], 152399025UL);
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

    bigInt.AssignInt64(-1);

    bigInt.Pow(57298735);
    BOOST_REQUIRE_EQUAL(bigInt.ToInt64(), -1);

    bigInt.Pow(325347312);
    BOOST_REQUIRE_EQUAL(bigInt.ToInt64(), 1);

    bigInt.AssignInt64(2);

    bigInt.Pow(10);
    BOOST_REQUIRE_EQUAL(bigInt.ToInt64(), 1024);

    bigInt.AssignInt64(-2);

    bigInt.Pow(10);
    BOOST_REQUIRE_EQUAL(bigInt.ToInt64(), 1024);

    bigInt.AssignInt64(2);

    bigInt.Pow(11);
    BOOST_REQUIRE_EQUAL(bigInt.ToInt64(), 2048);

    bigInt.AssignInt64(-2);

    bigInt.Pow(11);
    BOOST_REQUIRE_EQUAL(bigInt.ToInt64(), -2048);
}

BOOST_AUTO_TEST_CASE(TestMultiplyDivideSimple)
{
    BigInteger val;
    BigInteger res;
    BigInteger rem;

    val.AssignInt64(23225462820950625L);

    // 23225462820 and 950625
    BigInteger bi1;
    BigInteger bi2;
    val.Divide(BigInteger(1000000), bi1, bi2);

    // 23225 and 462820
    BigInteger bi3;
    BigInteger bi4;
    bi1.Divide(BigInteger(1000000), bi3, bi4);

    BOOST_CHECK_EQUAL(bi2.ToInt64(), 950625L);
    BOOST_CHECK_EQUAL(bi3.ToInt64(), 23225L);
    BOOST_CHECK_EQUAL(bi4.ToInt64(), 462820L);

    BigInteger(0).Divide(BigInteger(1), res, rem);
    
    BOOST_CHECK_EQUAL(res, BigInteger(0));
    BOOST_CHECK_EQUAL(rem, BigInteger(0));

    BigInteger(0).Divide(BigInteger(100), res, rem);

    BOOST_CHECK_EQUAL(res, BigInteger(0));
    BOOST_CHECK_EQUAL(rem, BigInteger(0));

    BigInteger(0).Divide(BigInteger(-1), res, rem);

    BOOST_CHECK_EQUAL(res, BigInteger(0));
    BOOST_CHECK_EQUAL(rem, BigInteger(0));

    BigInteger(0).Divide(BigInteger(-100), res, rem);

    BOOST_CHECK_EQUAL(res, BigInteger(0));
    BOOST_CHECK_EQUAL(rem, BigInteger(0));

    BigInteger(1).Divide(BigInteger(1), res, rem);

    BOOST_CHECK_EQUAL(res, BigInteger(1));
    BOOST_CHECK_EQUAL(rem, BigInteger(0));

    BigInteger(10).Divide(BigInteger(1), res, rem);

    BOOST_CHECK_EQUAL(res, BigInteger(10));
    BOOST_CHECK_EQUAL(rem, BigInteger(0));

    BigInteger(-1).Divide(BigInteger(1), res, rem);

    BOOST_CHECK_EQUAL(res, BigInteger(-1));
    BOOST_CHECK_EQUAL(rem, BigInteger(0));

    BigInteger(-10).Divide(BigInteger(1), res, rem);

    BOOST_CHECK_EQUAL(res, BigInteger(-10));
    BOOST_CHECK_EQUAL(rem, BigInteger(0));

    BigInteger(1).Divide(BigInteger(-1), res, rem);

    BOOST_CHECK_EQUAL(res, BigInteger(-1));
    BOOST_CHECK_EQUAL(rem, BigInteger(0));

    BigInteger(10).Divide(BigInteger(-1), res, rem);

    BOOST_CHECK_EQUAL(res, BigInteger(-10));
    BOOST_CHECK_EQUAL(rem, BigInteger(0));

    BigInteger(-1).Divide(BigInteger(-1), res, rem);

    BOOST_CHECK_EQUAL(res, BigInteger(1));
    BOOST_CHECK_EQUAL(rem, BigInteger(0));

    BigInteger(-10).Divide(BigInteger(-1), res, rem);

    BOOST_CHECK_EQUAL(res, BigInteger(10));
    BOOST_CHECK_EQUAL(rem, BigInteger(0));

    BigInteger(123456789).Divide(BigInteger(1000), res);
    BOOST_CHECK_EQUAL(res, BigInteger(123456));

    val.AssignInt64(79823695862);
    val.Divide(val, res);

    BOOST_CHECK_EQUAL(res, BigInteger(1));

    val.AssignInt64(28658345673);
    val.Divide(val, val);

    BOOST_CHECK_EQUAL(val, BigInteger(1));

    val.AssignInt64(-97598673406);
    val.Divide(val, res, val);

    BOOST_CHECK_EQUAL(res, BigInteger(1));
    BOOST_CHECK_EQUAL(val, BigInteger(0));

    val.AssignInt64(1);
    val.Divide(val, res, val);

    BOOST_CHECK_EQUAL(res, BigInteger(1));
    BOOST_CHECK_EQUAL(val, BigInteger(0));
}

BOOST_AUTO_TEST_CASE(TestDivideBigger)
{
    BigInteger res;
    BigInteger rem;

    BigInteger("4790467782742318458842833081").Divide(BigInteger(1000000000000000000L), res, rem);

    BOOST_CHECK_EQUAL(res.ToInt64(), 4790467782L);
    BOOST_CHECK_EQUAL(rem.ToInt64(), 742318458842833081L);

    BigInteger("4790467782742318458842833081").Divide(BigInteger("10000000000000000000"), res, rem);

    BOOST_CHECK_EQUAL(res.ToInt64(), 479046778L);
    BOOST_CHECK_EQUAL(rem.ToInt64(), 2742318458842833081L);

    BigInteger("328569986734256745892025106351608546013457217305539845689265945043650274304152384502658961485730864386").Divide(
        BigInteger("759823640567289574925305534862590863490856903465"), res, rem);

    BOOST_CHECK_EQUAL(res, BigInteger("432429275942170114314334535709873138296890293268042448"));
    BOOST_CHECK_EQUAL(rem, BigInteger("725289323707320757244048053769339286218272582066"));

    BigInteger("5789420569340865906230645903456092386459628364580763804659834658960883465807263084659832648768603645").Divide(
        BigInteger("-29064503640646565660609983646665763458768340596340586"), res, rem);

    BOOST_CHECK_EQUAL(res, BigInteger("-199192136253942064949205579447876757418653967046"));
    BOOST_CHECK_EQUAL(rem, BigInteger("-9693519879390725820633207073869515731754969332274689"));

    BigInteger("-107519074510758034695616045096493659264398569023607895679428769875976987594876903458769799098378994985793874569869348579").Divide(
        BigInteger("197290846190263940610876503491586943095983984894898998999751636576150263056012501"), res, rem);

    BOOST_CHECK_EQUAL(res, BigInteger("-544977512069001243499439196429495600701"));
    BOOST_CHECK_EQUAL(rem, BigInteger("-66382358009926062210728796777352226675944219851838448875365359123421443108985378"));

    BigInteger("9739565432896546050040656034658762836457836886868678345021405632645902354063045608267340568346582").Divide(
        BigInteger("8263050146508634250640862503465899340625908694088569038"), res);

    BOOST_CHECK_EQUAL(res, BigInteger("1178688893351540421358600789386475098289416"));
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
    CheckOutputSimple<BigInteger>(999999999999999999L);
    CheckOutputSimple<BigInteger>(999999999099999999L);
    CheckOutputSimple<BigInteger>(1000000000000000000L);
    CheckOutputSimple<BigInteger>(1000000000000000001L);
    CheckOutputSimple<BigInteger>(1000000005000000000L);
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
    CheckOutputSimple<BigInteger>(-999999999999999999L);
    CheckOutputSimple<BigInteger>(-999999999999999999L);
    CheckOutputSimple<BigInteger>(-1000000000000000000L);
    CheckOutputSimple<BigInteger>(-1000000000000000001L);
    CheckOutputSimple<BigInteger>(-1000000000000000000L);
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
    CheckOutputSimple<Decimal>(999999999999999999L);
    CheckOutputSimple<Decimal>(999999999099999999L);
    CheckOutputSimple<Decimal>(1000000000000000000L);
    CheckOutputSimple<Decimal>(1000000000000000001L);
    CheckOutputSimple<Decimal>(1000000005000000000L);
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
    CheckOutputSimple<Decimal>(-999999999999999999L);
    CheckOutputSimple<Decimal>(-999999999099999999L);
    CheckOutputSimple<Decimal>(-1000000000000000000L);
    CheckOutputSimple<Decimal>(-1000000000000000001L);
    CheckOutputSimple<Decimal>(-1000000005000000000L);
    CheckOutputSimple<Decimal>(INT64_MIN);
}
BOOST_AUTO_TEST_CASE(TestInputOutputSimpleBigInteger)
{
    CheckInputOutput<BigInteger>("0");
    CheckInputOutput<BigInteger>("1");
    CheckInputOutput<BigInteger>("2");
    CheckInputOutput<BigInteger>("9");
    CheckInputOutput<BigInteger>("10");
    CheckInputOutput<BigInteger>("1123");
    CheckInputOutput<BigInteger>("64539472569345602304");
    CheckInputOutput<BigInteger>("2376926357280573482539570263854");
    CheckInputOutput<BigInteger>("407846050973948576230645736487560936425876349857823587643258934569364587268645394725693456023046037490024067294087609279");

    CheckInputOutput<BigInteger>("1000000000000");
    CheckInputOutput<BigInteger>("1000000000000000000000000000");
    CheckInputOutput<BigInteger>("100000000000000000000000000000000000000000000000000000000000");

    CheckInputOutput<BigInteger>("99999999999999");
    CheckInputOutput<BigInteger>("99999999999999999999999999999999");
    CheckInputOutput<BigInteger>("9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999");

    CheckInputOutput<BigInteger>("-1");
    CheckInputOutput<BigInteger>("-2");
    CheckInputOutput<BigInteger>("-9");
    CheckInputOutput<BigInteger>("-10");
    CheckInputOutput<BigInteger>("-1123");
    CheckInputOutput<BigInteger>("-64539472569345602304");
    CheckInputOutput<BigInteger>("-2376926357280573482539570263854");
    CheckInputOutput<BigInteger>("-407846050973948576230645736487560936425876349857823587643258934569364587268645394725693456023046037490024067294087609279");

    CheckInputOutput<BigInteger>("-1000000000000");
    CheckInputOutput<BigInteger>("-1000000000000000000000000000");
    CheckInputOutput<BigInteger>("-100000000000000000000000000000000000000000000000000000000000");

    CheckInputOutput<BigInteger>("-99999999999999");
    CheckInputOutput<BigInteger>("-99999999999999999999999999999999");
    CheckInputOutput<BigInteger>("-9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999");
}

BOOST_AUTO_TEST_CASE(TestInputOutputSimpleDecimal)
{
    CheckInputOutput<Decimal>("0");
    CheckInputOutput<Decimal>("1");
    CheckInputOutput<Decimal>("2");
    CheckInputOutput<Decimal>("9");
    CheckInputOutput<Decimal>("10");
    CheckInputOutput<Decimal>("1123");
    CheckInputOutput<Decimal>("64539472569345602304");
    CheckInputOutput<Decimal>("2376926357280573482539570263854");
    CheckInputOutput<Decimal>("407846050973948576230645736487560936425876349857823587643258934569364587268645394725693456023046037490024067294087609279");

    CheckInputOutput<Decimal>("1000000000000");
    CheckInputOutput<Decimal>("1000000000000000000000000000");
    CheckInputOutput<Decimal>("100000000000000000000000000000000000000000000000000000000000");

    CheckInputOutput<Decimal>("99999999999999");
    CheckInputOutput<Decimal>("99999999999999999999999999999999");
    CheckInputOutput<Decimal>("9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999");

    CheckInputOutput<Decimal>("-1");
    CheckInputOutput<Decimal>("-2");
    CheckInputOutput<Decimal>("-9");
    CheckInputOutput<Decimal>("-10");
    CheckInputOutput<Decimal>("-1123");
    CheckInputOutput<Decimal>("-64539472569345602304");
    CheckInputOutput<Decimal>("-2376926357280573482539570263854");
    CheckInputOutput<Decimal>("-407846050973948576230645736487560936425876349857823587643258934569364587268645394725693456023046037490024067294087609279");

    CheckInputOutput<Decimal>("-1000000000000");
    CheckInputOutput<Decimal>("-1000000000000000000000000000");
    CheckInputOutput<Decimal>("-100000000000000000000000000000000000000000000000000000000000");

    CheckInputOutput<Decimal>("-99999999999999");
    CheckInputOutput<Decimal>("-99999999999999999999999999999999");
    CheckInputOutput<Decimal>("-9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999");

    CheckInputOutput<Decimal>("0.1");
    CheckInputOutput<Decimal>("0.2");
    CheckInputOutput<Decimal>("0.3");
    CheckInputOutput<Decimal>("0.4");
    CheckInputOutput<Decimal>("0.5");
    CheckInputOutput<Decimal>("0.6");
    CheckInputOutput<Decimal>("0.7");
    CheckInputOutput<Decimal>("0.8");
    CheckInputOutput<Decimal>("0.9");
    CheckInputOutput<Decimal>("0.01");
    CheckInputOutput<Decimal>("0.001");
    CheckInputOutput<Decimal>("0.0001");
    CheckInputOutput<Decimal>("0.00001");
    CheckInputOutput<Decimal>("0.000001");
    CheckInputOutput<Decimal>("0.0000001");

    CheckInputOutput<Decimal>("0.00000000000000000000000000000000001");
    CheckInputOutput<Decimal>("0.10000000000000000000000000000000001");
    CheckInputOutput<Decimal>("0.10101010101010101010101010101010101");
    CheckInputOutput<Decimal>("0.99999999999999999999999999999999999");
    CheckInputOutput<Decimal>("0.79287502687354897253590684568634528762");

    CheckInputOutput<Decimal>("0.00000000000000000000000000000000000000000000000000000001");
    CheckInputOutput<Decimal>("0.10000000000000000000000000000000000000000000000000000001");
    CheckInputOutput<Decimal>("0.1111111111111111111111111111111111111111111111111111111111");
    CheckInputOutput<Decimal>("0.9999999999999999999999999999999999999999999999999999999999999999999");
    CheckInputOutput<Decimal>("0.436589746389567836745873648576289634589763845768268457683762864587684635892768346589629");

    CheckInputOutput<Decimal>("0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001");
    CheckInputOutput<Decimal>("0.10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001");
    CheckInputOutput<Decimal>("0.111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111");
    CheckInputOutput<Decimal>("0.999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999");
    CheckInputOutput<Decimal>("0.436589746389567836745873648576289634589763845768268457683762864587684635892768346549385700256032605603246580726384075680247634627357023645889629");

    CheckInputOutput<Decimal>("-0.1");
    CheckInputOutput<Decimal>("-0.2");
    CheckInputOutput<Decimal>("-0.3");
    CheckInputOutput<Decimal>("-0.4");
    CheckInputOutput<Decimal>("-0.5");
    CheckInputOutput<Decimal>("-0.6");
    CheckInputOutput<Decimal>("-0.7");
    CheckInputOutput<Decimal>("-0.8");
    CheckInputOutput<Decimal>("-0.9");
    CheckInputOutput<Decimal>("-0.01");
    CheckInputOutput<Decimal>("-0.001");
    CheckInputOutput<Decimal>("-0.0001");
    CheckInputOutput<Decimal>("-0.00001");
    CheckInputOutput<Decimal>("-0.000001");
    CheckInputOutput<Decimal>("-0.0000001");

    CheckInputOutput<Decimal>("-0.00000000000000000000000000000000001");
    CheckInputOutput<Decimal>("-0.10000000000000000000000000000000001");
    CheckInputOutput<Decimal>("-0.10101010101010101010101010101010101");
    CheckInputOutput<Decimal>("-0.99999999999999999999999999999999999");
    CheckInputOutput<Decimal>("-0.79287502687354897253590684568634528762");

    CheckInputOutput<Decimal>("-0.00000000000000000000000000000000000000000000000000000001");
    CheckInputOutput<Decimal>("-0.10000000000000000000000000000000000000000000000000000001");
    CheckInputOutput<Decimal>("-0.1111111111111111111111111111111111111111111111111111111111");
    CheckInputOutput<Decimal>("-0.9999999999999999999999999999999999999999999999999999999999999999999");
    CheckInputOutput<Decimal>("-0.436589746389567836745873648576289634589763845768268457683762864587684635892768346589629");

    CheckInputOutput<Decimal>("-0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001");
    CheckInputOutput<Decimal>("-0.10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001");
    CheckInputOutput<Decimal>("-0.111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111");
    CheckInputOutput<Decimal>("-0.999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999");
    CheckInputOutput<Decimal>("-0.436589746389567836745873648576289634589763845768268457683762864587684635892768346549385700256032605603246580726384075680247634627357023645889629");

    CheckInputOutput<Decimal>("1.1");
    CheckInputOutput<Decimal>("12.21");
    CheckInputOutput<Decimal>("123.321");
    CheckInputOutput<Decimal>("1234.4321");
    CheckInputOutput<Decimal>("12345.54321");
    CheckInputOutput<Decimal>("123456.654321");
    CheckInputOutput<Decimal>("1234567.7654321");
    CheckInputOutput<Decimal>("12345678.87654321");
    CheckInputOutput<Decimal>("123456789.987654321");
    CheckInputOutput<Decimal>("1234567890.0987654321");
    CheckInputOutput<Decimal>("12345678909.90987654321");
    CheckInputOutput<Decimal>("123456789098.890987654321");
    CheckInputOutput<Decimal>("1234567890987.7890987654321");
    CheckInputOutput<Decimal>("12345678909876.67890987654321");
    CheckInputOutput<Decimal>("123456789098765.567890987654321");
    CheckInputOutput<Decimal>("1234567890987654.4567890987654321");
    CheckInputOutput<Decimal>("12345678909876543.34567890987654321");
    CheckInputOutput<Decimal>("123456789098765432.234567890987654321");
    CheckInputOutput<Decimal>("1234567890987654321.1234567890987654321");
    CheckInputOutput<Decimal>("12345678909876543210.01234567890987654321");
    CheckInputOutput<Decimal>("10000000000000000000000000000000000000000000000000000000000000.000000000000000000000000000000000000000000000000000000000000001");
    CheckInputOutput<Decimal>("111111111111111111111111111111111111111111111111111111111111111111111.11111111111111111111111111111111111111111111111111111111111111");
    CheckInputOutput<Decimal>("99999999999999999999999999999999999999999999999999999999999999999999.99999999999999999999999999999999999999999999999999999999999999999999");
    CheckInputOutput<Decimal>("458796987658934265896483756892638456782376482605002747502306790283640563.12017054126750641065780784583204650763485718064875683468568360506340563042567");

    CheckInputOutput<Decimal>("-1.1");
    CheckInputOutput<Decimal>("-12.21");
    CheckInputOutput<Decimal>("-123.321");
    CheckInputOutput<Decimal>("-1234.4321");
    CheckInputOutput<Decimal>("-12345.54321");
    CheckInputOutput<Decimal>("-123456.654321");
    CheckInputOutput<Decimal>("-1234567.7654321");
    CheckInputOutput<Decimal>("-12345678.87654321");
    CheckInputOutput<Decimal>("-123456789.987654321");
    CheckInputOutput<Decimal>("-1234567890.0987654321");
    CheckInputOutput<Decimal>("-12345678909.90987654321");
    CheckInputOutput<Decimal>("-123456789098.890987654321");
    CheckInputOutput<Decimal>("-1234567890987.7890987654321");
    CheckInputOutput<Decimal>("-12345678909876.67890987654321");
    CheckInputOutput<Decimal>("-123456789098765.567890987654321");
    CheckInputOutput<Decimal>("-1234567890987654.4567890987654321");
    CheckInputOutput<Decimal>("-12345678909876543.34567890987654321");
    CheckInputOutput<Decimal>("-123456789098765432.234567890987654321");
    CheckInputOutput<Decimal>("-1234567890987654321.1234567890987654321");
    CheckInputOutput<Decimal>("-12345678909876543210.01234567890987654321");
    CheckInputOutput<Decimal>("-10000000000000000000000000000000000000000000000000000000000000.000000000000000000000000000000000000000000000000000000000000001");
    CheckInputOutput<Decimal>("-111111111111111111111111111111111111111111111111111111111111111111111.11111111111111111111111111111111111111111111111111111111111111");
    CheckInputOutput<Decimal>("-99999999999999999999999999999999999999999999999999999999999999999999.99999999999999999999999999999999999999999999999999999999999999999999");
    CheckInputOutput<Decimal>("-458796987658934265896483756892638456782376482605002747502306790283640563.12017054126750641065780784583204650763485718064875683468568360506340563042567");
}

BOOST_AUTO_TEST_CASE(TestInputSimpleDecimal)
{
    CheckOutputInput(Decimal(0));

    CheckOutputInput(Decimal(1));
    CheckOutputInput(Decimal(9));
    CheckOutputInput(Decimal(10));
    CheckOutputInput(Decimal(11));
    CheckOutputInput(Decimal(19));
    CheckOutputInput(Decimal(123));
    CheckOutputInput(Decimal(1234));
    CheckOutputInput(Decimal(12345));
    CheckOutputInput(Decimal(123456));
    CheckOutputInput(Decimal(1234567));
    CheckOutputInput(Decimal(12345678));
    CheckOutputInput(Decimal(123456789));
    CheckOutputInput(Decimal(1234567890));
    CheckOutputInput(Decimal(12345678909));
    CheckOutputInput(Decimal(123456789098));
    CheckOutputInput(Decimal(1234567890987));
    CheckOutputInput(Decimal(12345678909876));
    CheckOutputInput(Decimal(123456789098765));
    CheckOutputInput(Decimal(1234567890987654));
    CheckOutputInput(Decimal(12345678909876543));
    CheckOutputInput(Decimal(123456789098765432));
    CheckOutputInput(Decimal(1234567890987654321));
    CheckOutputInput(Decimal(999999999999999999L));
    CheckOutputInput(Decimal(999999999099999999L));
    CheckOutputInput(Decimal(1000000000000000000L));
    CheckOutputInput(Decimal(1000000000000000001L));
    CheckOutputInput(Decimal(1000000005000000000L));
    CheckOutputInput(Decimal(INT64_MAX));

    CheckOutputInput(Decimal(-1));
    CheckOutputInput(Decimal(-9));
    CheckOutputInput(Decimal(-10));
    CheckOutputInput(Decimal(-11));
    CheckOutputInput(Decimal(-19));
    CheckOutputInput(Decimal(-123));
    CheckOutputInput(Decimal(-1234));
    CheckOutputInput(Decimal(-12345));
    CheckOutputInput(Decimal(-123456));
    CheckOutputInput(Decimal(-1234567));
    CheckOutputInput(Decimal(-12345678));
    CheckOutputInput(Decimal(-123456789));
    CheckOutputInput(Decimal(-1234567890));
    CheckOutputInput(Decimal(-12345678909));
    CheckOutputInput(Decimal(-123456789098));
    CheckOutputInput(Decimal(-1234567890987));
    CheckOutputInput(Decimal(-12345678909876));
    CheckOutputInput(Decimal(-123456789098765));
    CheckOutputInput(Decimal(-1234567890987654));
    CheckOutputInput(Decimal(-12345678909876543));
    CheckOutputInput(Decimal(-123456789098765432));
    CheckOutputInput(Decimal(-1234567890987654321));
    CheckOutputInput(Decimal(-999999999999999999L));
    CheckOutputInput(Decimal(-999999999099999999L));
    CheckOutputInput(Decimal(-1000000000000000000L));
    CheckOutputInput(Decimal(-1000000000000000001L));
    CheckOutputInput(Decimal(-1000000005000000000L));
    CheckOutputInput(Decimal(INT64_MIN));
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
    BOOST_CHECK_EQUAL(decimal.ToInt64(), 63647190455381106L);
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

    BOOST_CHECK_EQUAL(Decimal(2147483648L).GetPrecision(),           10); // 2^31:       10 digits
    BOOST_CHECK_EQUAL(Decimal(-2147483647L).GetPrecision(),          10); // -2^31+1:    10 digits
    BOOST_CHECK_EQUAL(Decimal(98893745455L).GetPrecision(),          11); // random:     11 digits
    BOOST_CHECK_EQUAL(Decimal(3455436789887L).GetPrecision(),        13); // random:     13 digits
    BOOST_CHECK_EQUAL(Decimal(140737488355328L).GetPrecision(),      15); // 2^47:       15 digits
    BOOST_CHECK_EQUAL(Decimal(-140737488355328L).GetPrecision(),     15); // -2^47:      15 digits
    BOOST_CHECK_EQUAL(Decimal(7564232235739573L).GetPrecision(),     16); // random:     16 digits
    BOOST_CHECK_EQUAL(Decimal(25335434990002322L).GetPrecision(),    17); // random:     17 digits
    BOOST_CHECK_EQUAL(Decimal(9223372036854775807L).GetPrecision(),  19); // 2^63 - 1:   19 digits
    BOOST_CHECK_EQUAL(Decimal(-9223372036854775807L).GetPrecision(), 19); // -2^63 + 1:  19 digits
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

BOOST_AUTO_TEST_CASE(TestDoubleCast)
{
    CheckDoubleCast(100000000000000.0);
    CheckDoubleCast(10000000000000.0);
    CheckDoubleCast(1000000000000.0);
    CheckDoubleCast(100000000000.0);
    CheckDoubleCast(10000000000.0);
    CheckDoubleCast(1000000000.0);
    CheckDoubleCast(100000000.0);
    CheckDoubleCast(10000000.0);
    CheckDoubleCast(1000000.0);
    CheckDoubleCast(100000.0);
    CheckDoubleCast(10000.0);
    CheckDoubleCast(1000.0);
    CheckDoubleCast(100.0);
    CheckDoubleCast(10.0);
    CheckDoubleCast(1.0);
    CheckDoubleCast(0.1);
    CheckDoubleCast(0.01);
    CheckDoubleCast(0.001);
    CheckDoubleCast(0.0001);
    CheckDoubleCast(0.00001);
    CheckDoubleCast(0.000001);
    CheckDoubleCast(0.0000001);
    CheckDoubleCast(0.00000001);
    CheckDoubleCast(0.000000001);
    CheckDoubleCast(0.0000000001);
    CheckDoubleCast(0.00000000001);
    CheckDoubleCast(0.000000000001);
    CheckDoubleCast(0.00000000000001);
    CheckDoubleCast(0.000000000000001);

    CheckDoubleCast(2379506093465806.);
    CheckDoubleCast(172650963870256.3);
    CheckDoubleCast(63506206502638.57);
    CheckDoubleCast(8946589364589.763);
    CheckDoubleCast(896258976348.9568);
    CheckDoubleCast(28967349256.39428);
    CheckDoubleCast(2806348972.689369);
    CheckDoubleCast(975962354.2835845);
    CheckDoubleCast(96342568.93542342);
    CheckDoubleCast(6875825.934892387);
    CheckDoubleCast(314969.6543969458);
    CheckDoubleCast(32906.02476506344);
    CheckDoubleCast(9869.643965396453);
    CheckDoubleCast(779.6932689452953);
    CheckDoubleCast(75.93592963492326);
    CheckDoubleCast(8.719654293587452);
    CheckDoubleCast(.90001236587463439);

    CheckDoubleCast(-235235E-100);
    CheckDoubleCast(-235235E-90);
    CheckDoubleCast(-235235E-80);
    CheckDoubleCast(-235235E-70);
    CheckDoubleCast(-235235E-60);
    CheckDoubleCast(-235235E-50);
    CheckDoubleCast(-235235E-40);
    CheckDoubleCast(-235235E-30);
    CheckDoubleCast(-235235E-25);
    CheckDoubleCast(-235235E-20);
    CheckDoubleCast(-235235E-15);
    CheckDoubleCast(-235235E-10);
    CheckDoubleCast(-235235E-9);
    CheckDoubleCast(-235235E-8);
    CheckDoubleCast(-235235E-7);
    CheckDoubleCast(-235235E-6);
    CheckDoubleCast(-235235E-5);
    CheckDoubleCast(-235235E-4);
    CheckDoubleCast(-235235E-2);
    CheckDoubleCast(-235235E-1);
    CheckDoubleCast(-235235);
    CheckDoubleCast(-235235E+1);
    CheckDoubleCast(-235235E+2);
    CheckDoubleCast(-235235E+3);
    CheckDoubleCast(-235235E+4);
    CheckDoubleCast(-235235E+5);
    CheckDoubleCast(-235235E+6);
    CheckDoubleCast(-235235E+7);
    CheckDoubleCast(-235235E+8);
    CheckDoubleCast(-235235E+9);
    CheckDoubleCast(-235235E+10);
    CheckDoubleCast(-235235E+15);
    CheckDoubleCast(-235235E+20);
    CheckDoubleCast(-235235E+25);
    CheckDoubleCast(-235235E+30);
    CheckDoubleCast(-235235E+40);
    CheckDoubleCast(-235235E+50);
    CheckDoubleCast(-235235E+60);
    CheckDoubleCast(-235235E+70);
    CheckDoubleCast(-235235E+80);
    CheckDoubleCast(-235235E+90);
    CheckDoubleCast(-235235E+100);

    CheckDoubleCast(-2379506093465806.);
    CheckDoubleCast(-172650963870256.3);
    CheckDoubleCast(-63506206502638.57);
    CheckDoubleCast(-8946589364589.763);
    CheckDoubleCast(-896258976348.9568);
    CheckDoubleCast(-28967349256.39428);
    CheckDoubleCast(-2806348972.689369);
    CheckDoubleCast(-975962354.2835845);
    CheckDoubleCast(-96342568.93542342);
    CheckDoubleCast(-6875825.934892387);
    CheckDoubleCast(-314969.6543969458);
    CheckDoubleCast(-32906.02476506344);
    CheckDoubleCast(-9869.643965396453);
    CheckDoubleCast(-779.6932689452953);
    CheckDoubleCast(-75.93592963492326);
    CheckDoubleCast(-8.719654293587452);
    CheckDoubleCast(-.90001236587463439);

    CheckDoubleCast(-100000000000000.0);
    CheckDoubleCast(-10000000000000.0);
    CheckDoubleCast(-1000000000000.0);
    CheckDoubleCast(-100000000000.0);
    CheckDoubleCast(-10000000000.0);
    CheckDoubleCast(-1000000000.0);
    CheckDoubleCast(-100000000.0);
    CheckDoubleCast(-10000000.0);
    CheckDoubleCast(-1000000.0);
    CheckDoubleCast(-100000.0);
    CheckDoubleCast(-10000.0);
    CheckDoubleCast(-1000.0);
    CheckDoubleCast(-100.0);
    CheckDoubleCast(-10.0);
    CheckDoubleCast(-1.0);
    CheckDoubleCast(-0.1);
    CheckDoubleCast(-0.01);
    CheckDoubleCast(-0.001);
    CheckDoubleCast(-0.0001);
    CheckDoubleCast(-0.00001);
    CheckDoubleCast(-0.000001);
    CheckDoubleCast(-0.0000001);
    CheckDoubleCast(-0.00000001);
    CheckDoubleCast(-0.000000001);
    CheckDoubleCast(-0.0000000001);
    CheckDoubleCast(-0.00000000001);
    CheckDoubleCast(-0.000000000001);
    CheckDoubleCast(-0.00000000000001);
    CheckDoubleCast(-0.000000000000001);
}

BOOST_AUTO_TEST_SUITE_END()
