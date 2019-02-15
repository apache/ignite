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

#include <ignite/common/fixed_size_array.h>

using namespace ignite;
using namespace ignite::common;


struct TestStruct
{
    int32_t one;
    int32_t two;

    TestStruct() :
        one(1),
        two(2)
    {
        // No-op.
    }

    void Clear()
    {
        one = two = 0;
    }
};


BOOST_AUTO_TEST_SUITE(FixedSizeArrayTestSuite)

BOOST_AUTO_TEST_CASE(ConstructionInt)
{
    FixedSizeArray<int> zeroed(16);

    for (int i = 0; i < zeroed.GetSize(); ++i)
        BOOST_CHECK_EQUAL(zeroed[i], 0);
}

BOOST_AUTO_TEST_CASE(ConstructionBool)
{
    FixedSizeArray<bool> fbool(16);

    for (int i = 0; i < fbool.GetSize(); ++i)
        BOOST_CHECK_EQUAL(fbool[i], false);
}

BOOST_AUTO_TEST_CASE(ConstructionStruct)
{
    FixedSizeArray<TestStruct> test(16);

    for (int i = 0; i < test.GetSize(); ++i)
    {
        BOOST_CHECK_EQUAL(test[i].one, 1);
        BOOST_CHECK_EQUAL(test[i].two, 2);
    }
}

BOOST_AUTO_TEST_CASE(ConstructionArray)
{
    int someVals[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

    FixedSizeArray<int> test(someVals, 10);

    for (int i = 0; i < test.GetSize(); ++i)
        BOOST_CHECK_EQUAL(test[i], i);
}

BOOST_AUTO_TEST_CASE(ConstructionCopy)
{
    FixedSizeArray<int> test1(10);
    
    for (int i = 0; i < test1.GetSize(); ++i)
        test1[i] = i * 5;

    FixedSizeArray<int> test2(test1);

    for (int i = 0; i < test2.GetSize(); ++i)
        BOOST_CHECK_EQUAL(test2[i], i * 5);
}

BOOST_AUTO_TEST_CASE(ResetInt)
{
    FixedSizeArray<int> test(16);

    for (int i = 0; i < test.GetSize(); ++i)
        test[i] = 42;

    test.Reset(16);

    for (int i = 0; i < test.GetSize(); ++i)
        BOOST_CHECK_EQUAL(test[i], 0);
}

BOOST_AUTO_TEST_CASE(ResetBool)
{
    FixedSizeArray<bool> test(16);

    for (int i = 0; i < test.GetSize(); ++i)
        test[i] = true;

    test.Reset(16);

    for (int i = 0; i < test.GetSize(); ++i)
        BOOST_CHECK_EQUAL(test[i], false);
}

BOOST_AUTO_TEST_CASE(ResetStruct)
{
    FixedSizeArray<TestStruct> test(16);

    for (int i = 0; i < test.GetSize(); ++i)
        test[i].Clear();

    test.Reset(16);

    for (int i = 0; i < test.GetSize(); ++i)
    {
        BOOST_CHECK_EQUAL(test[i].one, 1);
        BOOST_CHECK_EQUAL(test[i].two, 2);
    }
}

BOOST_AUTO_TEST_CASE(ResetSizeChange)
{
    FixedSizeArray<int> test(4);

    BOOST_CHECK_EQUAL(test.GetSize(), 4);

    for (int i = 0; i < test.GetSize(); ++i)
        test[i] = 42;

    test.Reset(16);

    BOOST_CHECK_EQUAL(test.GetSize(), 16);

    for (int i = 0; i < test.GetSize(); ++i)
    {
        BOOST_CHECK_EQUAL(test[i], 0);

        test[i] = 100500;
    }
}

BOOST_AUTO_TEST_CASE(IsEmpty)
{
    FixedSizeArray<int> test;

    BOOST_CHECK(test.IsEmpty());

    test.Reset(16);

    BOOST_CHECK(!test.IsEmpty());

    test.Reset();

    BOOST_CHECK(test.IsEmpty());
}

BOOST_AUTO_TEST_CASE(Swap)
{
    FixedSizeArray<std::string> test1(3);
    FixedSizeArray<std::string> test2(2);

    test1[0] = "Hello";
    test1[1] = "World";
    test1[2] = "!!!";

    test2[0] = "Hi";
    test2[1] = "!";

    BOOST_CHECK_EQUAL(test1[0], std::string("Hello"));
    BOOST_CHECK_EQUAL(test1[1], std::string("World"));
    BOOST_CHECK_EQUAL(test1[2], std::string("!!!"));

    BOOST_CHECK_EQUAL(test2[0], std::string("Hi"));
    BOOST_CHECK_EQUAL(test2[1], std::string("!"));

    test1.Swap(test2);

    BOOST_CHECK_EQUAL(test2[0], std::string("Hello"));
    BOOST_CHECK_EQUAL(test2[1], std::string("World"));
    BOOST_CHECK_EQUAL(test2[2], std::string("!!!"));

    BOOST_CHECK_EQUAL(test1[0], std::string("Hi"));
    BOOST_CHECK_EQUAL(test1[1], std::string("!"));
}

BOOST_AUTO_TEST_SUITE_END()
