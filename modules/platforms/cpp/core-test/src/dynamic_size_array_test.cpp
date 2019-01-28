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

#include <ignite/common/dynamic_size_array.h>

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

    TestStruct(int32_t a, int32_t b) :
        one(a),
        two(b)
    {
        // No-op.
    }

    void Clear()
    {
        one = two = 0;
    }
};


BOOST_AUTO_TEST_SUITE(DynamicSizeArrayTestSuite)

BOOST_AUTO_TEST_CASE(BasicConstruction)
{
    DynamicSizeArray<int> test(16);

    BOOST_CHECK_EQUAL(test.GetSize(), 0);
    BOOST_CHECK(test.GetCapasity() >= 16);
}

BOOST_AUTO_TEST_CASE(ResizeInt)
{
    DynamicSizeArray<int> test;

    test.Resize(16);

    for (int i = 0; i < test.GetSize(); ++i)
        BOOST_CHECK_EQUAL(test[i], 0);
}

BOOST_AUTO_TEST_CASE(ResizeBool)
{
    DynamicSizeArray<bool> test;

    test.Resize(16);

    for (int i = 0; i < test.GetSize(); ++i)
        BOOST_CHECK_EQUAL(test[i], false);
}

BOOST_AUTO_TEST_CASE(ResizeStruct)
{
    DynamicSizeArray<TestStruct> test;

    test.Resize(16);

    for (int i = 0; i < test.GetSize(); ++i)
    {
        BOOST_CHECK_EQUAL(test[i].one, 1);
        BOOST_CHECK_EQUAL(test[i].two, 2);
    }
}

BOOST_AUTO_TEST_CASE(PushBack)
{
    DynamicSizeArray<int> test;

    test.PushBack(1);
    test.PushBack(2);
    test.PushBack(42);
    test.PushBack(88);

    BOOST_CHECK_EQUAL(test.GetSize(), 4);
    BOOST_CHECK(test.GetCapasity() >= 4);

    BOOST_CHECK_EQUAL(test[0], 1);
    BOOST_CHECK_EQUAL(test[1], 2);
    BOOST_CHECK_EQUAL(test[2], 42);
    BOOST_CHECK_EQUAL(test[3], 88);

    BOOST_CHECK_EQUAL(test.Back(), 88);
    BOOST_CHECK_EQUAL(test.Front(), 1);
}

BOOST_AUTO_TEST_CASE(ResizeGrowing)
{
    DynamicSizeArray<TestStruct> test;

    test.PushBack(TestStruct(3, 7));
    test.PushBack(TestStruct(5, 6));
    test.PushBack(TestStruct(42, 55));
    test.PushBack(TestStruct(47334, 3));

    BOOST_CHECK_EQUAL(test.GetSize(), 4);

    test.Resize(8);

    BOOST_CHECK_EQUAL(test[0].one, 3);
    BOOST_CHECK_EQUAL(test[1].one, 5);
    BOOST_CHECK_EQUAL(test[2].one, 42);
    BOOST_CHECK_EQUAL(test[3].one, 47334);

    BOOST_CHECK_EQUAL(test[0].two, 7);
    BOOST_CHECK_EQUAL(test[1].two, 6);
    BOOST_CHECK_EQUAL(test[2].two, 55);
    BOOST_CHECK_EQUAL(test[3].two, 3);

    for (int i = 4; i < test.GetSize(); ++i)
    {
        BOOST_CHECK_EQUAL(test[i].one, 1);
        BOOST_CHECK_EQUAL(test[i].two, 2);

        test[i] = TestStruct(i * 3, i * 32508 + i);
    }
}

BOOST_AUTO_TEST_CASE(ResizeShrinking)
{
    DynamicSizeArray<TestStruct> test;

    test.PushBack(TestStruct(3, 7));
    test.PushBack(TestStruct(5, 6));
    test.PushBack(TestStruct(42, 55));
    test.PushBack(TestStruct(47334, 3));

    BOOST_CHECK_EQUAL(test.GetSize(), 4);

    test.Resize(2);

    BOOST_CHECK_EQUAL(test.GetSize(), 2);

    BOOST_CHECK_EQUAL(test[0].one, 3);
    BOOST_CHECK_EQUAL(test[1].one, 5);

    BOOST_CHECK_EQUAL(test[0].two, 7);
    BOOST_CHECK_EQUAL(test[1].two, 6);
}

BOOST_AUTO_TEST_CASE(ResizeKeep)
{
    DynamicSizeArray<TestStruct> test;

    test.PushBack(TestStruct(3, 7));
    test.PushBack(TestStruct(5, 6));
    test.PushBack(TestStruct(42, 55));
    test.PushBack(TestStruct(47334, 3));

    BOOST_CHECK_EQUAL(test.GetSize(), 4);

    test.Resize(4);

    BOOST_CHECK_EQUAL(test[0].one, 3);
    BOOST_CHECK_EQUAL(test[1].one, 5);
    BOOST_CHECK_EQUAL(test[2].one, 42);
    BOOST_CHECK_EQUAL(test[3].one, 47334);

    BOOST_CHECK_EQUAL(test[0].two, 7);
    BOOST_CHECK_EQUAL(test[1].two, 6);
    BOOST_CHECK_EQUAL(test[2].two, 55);
    BOOST_CHECK_EQUAL(test[3].two, 3);
}

BOOST_AUTO_TEST_CASE(ReserveMore)
{
    DynamicSizeArray<TestStruct> test;

    test.PushBack(TestStruct(3, 7));
    test.PushBack(TestStruct(5, 6));
    test.PushBack(TestStruct(42, 55));
    test.PushBack(TestStruct(47334, 3));

    BOOST_CHECK_EQUAL(test.GetSize(), 4);

    int32_t capasity = test.GetCapasity();

    test.Reserve(capasity + 1);

    BOOST_CHECK(test.GetCapasity() > capasity);
    BOOST_CHECK(test.GetCapasity() >= capasity + 1);
    BOOST_CHECK_EQUAL(test.GetSize(), 4);

    BOOST_CHECK_EQUAL(test[0].one, 3);
    BOOST_CHECK_EQUAL(test[1].one, 5);
    BOOST_CHECK_EQUAL(test[2].one, 42);
    BOOST_CHECK_EQUAL(test[3].one, 47334);

    BOOST_CHECK_EQUAL(test[0].two, 7);
    BOOST_CHECK_EQUAL(test[1].two, 6);
    BOOST_CHECK_EQUAL(test[2].two, 55);
    BOOST_CHECK_EQUAL(test[3].two, 3);
}

BOOST_AUTO_TEST_CASE(ReserveLess)
{
    DynamicSizeArray<TestStruct> test;

    test.PushBack(TestStruct(3, 7));
    test.PushBack(TestStruct(5, 6));
    test.PushBack(TestStruct(42, 55));
    test.PushBack(TestStruct(47334, 3));

    BOOST_CHECK_EQUAL(test.GetSize(), 4);

    int32_t capasity = test.GetCapasity();

    test.Reserve(capasity - 1);

    BOOST_CHECK(test.GetCapasity() == capasity);
    BOOST_CHECK_EQUAL(test.GetSize(), 4);

    BOOST_CHECK_EQUAL(test[0].one, 3);
    BOOST_CHECK_EQUAL(test[1].one, 5);
    BOOST_CHECK_EQUAL(test[2].one, 42);
    BOOST_CHECK_EQUAL(test[3].one, 47334);

    BOOST_CHECK_EQUAL(test[0].two, 7);
    BOOST_CHECK_EQUAL(test[1].two, 6);
    BOOST_CHECK_EQUAL(test[2].two, 55);
    BOOST_CHECK_EQUAL(test[3].two, 3);
}

BOOST_AUTO_TEST_CASE(Append)
{
    DynamicSizeArray<TestStruct> test1;
    DynamicSizeArray<TestStruct> test2;

    test1.PushBack(TestStruct(3, 7));
    test1.PushBack(TestStruct(5, 6));

    test2.PushBack(TestStruct(42, 55));
    test2.PushBack(TestStruct(47334, 3));

    BOOST_CHECK_EQUAL(test1.GetSize(), 2);
    BOOST_CHECK_EQUAL(test2.GetSize(), 2);

    test1.Append(test2.GetData(), test2.GetSize());

    BOOST_CHECK_EQUAL(test1.GetSize(), 4);
    BOOST_CHECK_EQUAL(test2.GetSize(), 2);

    BOOST_CHECK_EQUAL(test1[0].one, 3);
    BOOST_CHECK_EQUAL(test1[1].one, 5);
    BOOST_CHECK_EQUAL(test1[2].one, 42);
    BOOST_CHECK_EQUAL(test1[3].one, 47334);

    BOOST_CHECK_EQUAL(test1[0].two, 7);
    BOOST_CHECK_EQUAL(test1[1].two, 6);
    BOOST_CHECK_EQUAL(test1[2].two, 55);
    BOOST_CHECK_EQUAL(test1[3].two, 3);

    BOOST_CHECK_EQUAL(test2[0].one, 42);
    BOOST_CHECK_EQUAL(test2[1].one, 47334);

    BOOST_CHECK_EQUAL(test2[0].two, 55);
    BOOST_CHECK_EQUAL(test2[1].two, 3);
}

BOOST_AUTO_TEST_CASE(ConstructionArray)
{
    int someVals[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

    DynamicSizeArray<int> test(someVals, 10);

    for (int i = 0; i < test.GetSize(); ++i)
        BOOST_CHECK_EQUAL(test[i], i);
}

BOOST_AUTO_TEST_CASE(ConstructionCopy)
{
    DynamicSizeArray<int> test1(10);
    
    for (int i = 0; i < test1.GetSize(); ++i)
        test1[i] = i * 5;

    DynamicSizeArray<int> test2(test1);

    for (int i = 0; i < test2.GetSize(); ++i)
        BOOST_CHECK_EQUAL(test2[i], i * 5);
}

BOOST_AUTO_TEST_CASE(IsEmpty)
{
    DynamicSizeArray<int> test;

    BOOST_CHECK(test.IsEmpty());

    test.Resize(16);

    BOOST_CHECK(!test.IsEmpty());

    test.Clear();

    BOOST_CHECK(test.IsEmpty());
}

BOOST_AUTO_TEST_CASE(Swap)
{
    DynamicSizeArray<std::string> test1(3);
    DynamicSizeArray<std::string> test2(2);

    test1.PushBack("Hello");
    test1.PushBack("World");
    test1.PushBack("!!!");

    test2.PushBack("Hi");
    test2.PushBack("!");

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
