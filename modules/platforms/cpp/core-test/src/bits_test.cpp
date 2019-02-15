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

#include <ignite/common/bits.h>

using namespace ignite;
using namespace ignite::common::bits;

BOOST_AUTO_TEST_SUITE(BitsTestSuite)

BOOST_AUTO_TEST_CASE(TestNumberOfLeadingZeroes)
{
    BOOST_CHECK_EQUAL(32, NumberOfLeadingZerosI32(0));

    BOOST_CHECK_EQUAL(31, NumberOfLeadingZerosI32(1));

    BOOST_CHECK_EQUAL(30, NumberOfLeadingZerosI32(2));
    BOOST_CHECK_EQUAL(30, NumberOfLeadingZerosI32(3));

    BOOST_CHECK_EQUAL(29, NumberOfLeadingZerosI32(4));
    BOOST_CHECK_EQUAL(29, NumberOfLeadingZerosI32(5));
    BOOST_CHECK_EQUAL(29, NumberOfLeadingZerosI32(7));

    BOOST_CHECK_EQUAL(28, NumberOfLeadingZerosI32(8));
    BOOST_CHECK_EQUAL(28, NumberOfLeadingZerosI32(12));
    BOOST_CHECK_EQUAL(28, NumberOfLeadingZerosI32(15));

    BOOST_CHECK_EQUAL(0, NumberOfLeadingZerosI32(0xFFFFFFFF));
    BOOST_CHECK_EQUAL(0, NumberOfLeadingZerosI32(0x80000000));

    BOOST_CHECK_EQUAL(1, NumberOfLeadingZerosI32(0x7FFFFFFF));
    BOOST_CHECK_EQUAL(1, NumberOfLeadingZerosI32(0x40000000));

    BOOST_CHECK_EQUAL(8, NumberOfLeadingZerosI32(0x00FFFFFF));
    BOOST_CHECK_EQUAL(8, NumberOfLeadingZerosI32(0x00F00000));
    BOOST_CHECK_EQUAL(8, NumberOfLeadingZerosI32(0x00800000));

    BOOST_CHECK_EQUAL(9, NumberOfLeadingZerosI32(0x00700000));
    BOOST_CHECK_EQUAL(9, NumberOfLeadingZerosI32(0x00400000));
    BOOST_CHECK_EQUAL(9, NumberOfLeadingZerosI32(0x006C0395));
}

BOOST_AUTO_TEST_CASE(TestNumberOfTrailingZeroes)
{
    BOOST_CHECK_EQUAL(0, NumberOfTrailingZerosI32(1));
    BOOST_CHECK_EQUAL(0, NumberOfTrailingZerosI32(3));
    BOOST_CHECK_EQUAL(0, NumberOfTrailingZerosI32(5));
    BOOST_CHECK_EQUAL(0, NumberOfTrailingZerosI32(7));
    BOOST_CHECK_EQUAL(0, NumberOfTrailingZerosI32(15));
    BOOST_CHECK_EQUAL(0, NumberOfTrailingZerosI32(0xFFFFFFFF));
    BOOST_CHECK_EQUAL(0, NumberOfTrailingZerosI32(0x7FFFFFFF));
    BOOST_CHECK_EQUAL(0, NumberOfTrailingZerosI32(0x00FFFFFF));
    BOOST_CHECK_EQUAL(0, NumberOfTrailingZerosI32(0x006C0395));

    BOOST_CHECK_EQUAL(1, NumberOfTrailingZerosI32(2));

    BOOST_CHECK_EQUAL(2, NumberOfTrailingZerosI32(4));
    BOOST_CHECK_EQUAL(2, NumberOfTrailingZerosI32(12));

    BOOST_CHECK_EQUAL(3, NumberOfTrailingZerosI32(8));

    BOOST_CHECK_EQUAL(20, NumberOfTrailingZerosI32(0xFFF00000));
    BOOST_CHECK_EQUAL(20, NumberOfTrailingZerosI32(0x00F00000));
    BOOST_CHECK_EQUAL(20, NumberOfTrailingZerosI32(0x00700000));
    BOOST_CHECK_EQUAL(20, NumberOfTrailingZerosI32(0x80700000));

    BOOST_CHECK_EQUAL(22, NumberOfTrailingZerosI32(0x00400000));
    BOOST_CHECK_EQUAL(22, NumberOfTrailingZerosI32(0x80400000));
    BOOST_CHECK_EQUAL(22, NumberOfTrailingZerosI32(0x10400000));

    BOOST_CHECK_EQUAL(23, NumberOfTrailingZerosI32(0x00800000));
    BOOST_CHECK_EQUAL(23, NumberOfTrailingZerosI32(0x80800000));
    BOOST_CHECK_EQUAL(23, NumberOfTrailingZerosI32(0xFF800000));

    BOOST_CHECK_EQUAL(30, NumberOfTrailingZerosI32(0x40000000));
    BOOST_CHECK_EQUAL(30, NumberOfTrailingZerosI32(0xC0000000));

    BOOST_CHECK_EQUAL(31, NumberOfTrailingZerosI32(0x80000000));

    BOOST_CHECK_EQUAL(32, NumberOfTrailingZerosI32(0));
}

BOOST_AUTO_TEST_CASE(TestBitCount)
{
    BOOST_CHECK_EQUAL(0, BitCountI32(0));

    for (int j = 0; j < 32; ++j)
    {
        const int32_t testNum = 0xFFFFFFFFUL >> (31 - j);

        for (int i = 0; i < (32 - j); ++i)
            BOOST_CHECK_EQUAL(j + 1, BitCountI32(testNum));
    }

    for (int j = 0; j < 32; j += 2)
    {
        const int32_t testNum = 0xAAAAAAAAUL >> (31 - j);

        for (int i = 0; i < (32 - j); ++i)
            BOOST_CHECK_EQUAL((j / 2) + 1, BitCountI32(testNum));
    }
}

BOOST_AUTO_TEST_SUITE_END()
