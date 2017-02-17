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
    #define BOOST_TEST_DYN_LINK
#endif

#include <memory>

#include <boost/test/unit_test.hpp>
#include <boost/smart_ptr.hpp>
#include <boost/interprocess/smart_ptr/unique_ptr.hpp>

#include <ignite/reference.h>

using namespace ignite;
using namespace boost::unit_test;

class LivenessMarker
{
public:
    LivenessMarker(bool& flag) :
        flag(flag)
    {
        flag = true;
    }

    LivenessMarker(const LivenessMarker& other) :
        flag(other.flag)
    {
        // No-op.
    }

    LivenessMarker& operator=(const LivenessMarker& other)
    {
        flag = other.flag;

        return *this;
    }

    ~LivenessMarker()
    {
        flag = false;
    }

private:
    bool& flag;
};

class InstanceCounter
{
public:
    InstanceCounter(int& counter) :
        counter(&counter)
    {
        ++(*this->counter);
    }

    InstanceCounter(const InstanceCounter& other) :
        counter(other.counter)
    {
        ++(*counter);
    }

    InstanceCounter& operator=(const InstanceCounter& other)
    {
        counter = other.counter;

        ++(*counter);

        return *this;
    }

    ~InstanceCounter()
    {
        --(*counter);
    }

private:
    int* counter;
};


void TestFunction(Reference<LivenessMarker> ptr)
{
    Reference<LivenessMarker> copy(ptr);
    Reference<LivenessMarker> copy2(ptr);
}

struct C1
{
    int c1;
};

struct C2
{
    int c2;
};

struct C3 : C1, C2
{
    int c3;
};

void TestFunction1(Reference<C1> c1, int expected)
{
    BOOST_CHECK_EQUAL(c1.Get().c1, expected);
}

void TestFunction2(Reference<C2> c2, int expected)
{
    BOOST_CHECK_EQUAL(c2.Get().c2, expected);
}

void TestFunction3(Reference<C3> c3, int expected)
{
    BOOST_CHECK_EQUAL(c3.Get().c3, expected);
}

void TestFunctionConst1(ConstReference<C1> c1, int expected)
{
    BOOST_CHECK_EQUAL(c1.Get().c1, expected);
}

void TestFunctionConst2(ConstReference<C2> c2, int expected)
{
    BOOST_CHECK_EQUAL(c2.Get().c2, expected);
}

void TestFunctionConst3(ConstReference<C3> c3, int expected)
{
    BOOST_CHECK_EQUAL(c3.Get().c3, expected);
}

BOOST_AUTO_TEST_SUITE(ReferenceTestSuite)


BOOST_AUTO_TEST_CASE(StdSharedPointerTestBefore)
{
#if !defined(BOOST_NO_CXX11_SMART_PTR)
    bool objAlive = false;

    std::shared_ptr<LivenessMarker> shared = std::make_shared<LivenessMarker>(objAlive);

    BOOST_CHECK(objAlive);

    {
        Reference<LivenessMarker> smart = MakeReferenceFromSmartPointer(shared);

        BOOST_CHECK(objAlive);

        shared.reset();

        BOOST_CHECK(objAlive);
    }

    BOOST_CHECK(!objAlive);
#endif
}

BOOST_AUTO_TEST_CASE(StdSharedPointerTestAfter)
{
#if !defined(BOOST_NO_CXX11_SMART_PTR)
    bool objAlive = false;

    std::shared_ptr<LivenessMarker> shared = std::make_shared<LivenessMarker>(objAlive);

    BOOST_CHECK(objAlive);

    {
        Reference<LivenessMarker> smart = MakeReferenceFromSmartPointer(shared);

        BOOST_CHECK(objAlive);
    }

    BOOST_CHECK(objAlive);

    shared.reset();

    BOOST_CHECK(!objAlive);
#endif
}

BOOST_AUTO_TEST_CASE(StdAutoPointerTest)
{
    bool objAlive = false;

    std::auto_ptr<LivenessMarker> autop(new LivenessMarker(objAlive));

    BOOST_CHECK(objAlive);

    {
        Reference<LivenessMarker> smart = MakeReferenceFromSmartPointer(autop);

        BOOST_CHECK(objAlive);
    }

    BOOST_CHECK(!objAlive);
}

BOOST_AUTO_TEST_CASE(StdUniquePointerTest)
{
#if !defined(BOOST_NO_CXX11_SMART_PTR)
    bool objAlive = false;

    std::unique_ptr<LivenessMarker> unique(new LivenessMarker(objAlive));

    BOOST_CHECK(objAlive);

    {
        Reference<LivenessMarker> smart = MakeReferenceFromSmartPointer(std::move(unique));

        BOOST_CHECK(objAlive);
    }

    BOOST_CHECK(!objAlive);
#endif
}

BOOST_AUTO_TEST_CASE(BoostSharedPointerTestBefore)
{
    bool objAlive = false;

    boost::shared_ptr<LivenessMarker> shared = boost::make_shared<LivenessMarker>(boost::ref(objAlive));

    BOOST_CHECK(objAlive);

    {
        Reference<LivenessMarker> smart = MakeReferenceFromSmartPointer(shared);

        BOOST_CHECK(objAlive);

        shared.reset();

        BOOST_CHECK(objAlive);
    }

    BOOST_CHECK(!objAlive);
}

BOOST_AUTO_TEST_CASE(BoostSharedPointerTestAfter)
{
    bool objAlive = false;

    boost::shared_ptr<LivenessMarker> shared = boost::make_shared<LivenessMarker>(boost::ref(objAlive));

    BOOST_CHECK(objAlive);

    {
        Reference<LivenessMarker> smart = MakeReferenceFromSmartPointer(shared);

        BOOST_CHECK(objAlive);
    }

    BOOST_CHECK(objAlive);

    shared.reset();

    BOOST_CHECK(!objAlive);
}


BOOST_AUTO_TEST_CASE(PassingToFunction)
{
#if !defined(BOOST_NO_CXX11_SMART_PTR)
    bool objAlive = false;

    std::shared_ptr<LivenessMarker> stdShared = std::make_shared<LivenessMarker>(objAlive);
    std::unique_ptr<LivenessMarker> stdUnique(new LivenessMarker(objAlive));
    std::auto_ptr<LivenessMarker> stdAuto(new LivenessMarker(objAlive));

    boost::shared_ptr<LivenessMarker> boostShared = boost::make_shared<LivenessMarker>(objAlive);

    TestFunction(MakeReferenceFromSmartPointer(stdShared));
    TestFunction(MakeReferenceFromSmartPointer(std::move(stdUnique)));
    TestFunction(MakeReferenceFromSmartPointer(stdAuto));

    TestFunction(MakeReferenceFromSmartPointer(boostShared));
#endif
}

BOOST_AUTO_TEST_CASE(CopyTest)
{
    int instances = 0;

    {
        InstanceCounter counter(instances);

        BOOST_CHECK_EQUAL(instances, 1);

        {
            Reference<InstanceCounter> copy = MakeReferenceFromCopy(counter);

            BOOST_CHECK_EQUAL(instances, 2);
        }

        BOOST_CHECK_EQUAL(instances, 1);
    }

    BOOST_CHECK_EQUAL(instances, 0);
}

BOOST_AUTO_TEST_CASE(OwningPointerTest)
{
    int instances = 0;

    {
        InstanceCounter *counter = new InstanceCounter(instances);

        BOOST_CHECK_EQUAL(instances, 1);

        {
            Reference<InstanceCounter> owned = MakeReferenceFromOwningPointer(counter);

            BOOST_CHECK_EQUAL(instances, 1);
        }

        BOOST_CHECK_EQUAL(instances, 0);
    }

    BOOST_CHECK_EQUAL(instances, 0);
}

BOOST_AUTO_TEST_CASE(NonOwningPointerTest1)
{
    int instances = 0;

    {
        InstanceCounter counter(instances);

        BOOST_CHECK_EQUAL(instances, 1);

        {
            Reference<InstanceCounter> copy = MakeReference(counter);

            BOOST_CHECK_EQUAL(instances, 1);
        }

        BOOST_CHECK_EQUAL(instances, 1);
    }

    BOOST_CHECK_EQUAL(instances, 0);
}

BOOST_AUTO_TEST_CASE(NonOwningPointerTest2)
{
    int instances = 0;

    InstanceCounter* counter = new InstanceCounter(instances);

    BOOST_CHECK_EQUAL(instances, 1);

    {
        Reference<InstanceCounter> copy = MakeReference(*counter);

        BOOST_CHECK_EQUAL(instances, 1);

        delete counter;

        BOOST_CHECK_EQUAL(instances, 0);
    }

    BOOST_CHECK_EQUAL(instances, 0);
}

BOOST_AUTO_TEST_CASE(CastTest)
{
    C3 testVal;

    testVal.c1 = 1;
    testVal.c2 = 2;
    testVal.c3 = 3;

    TestFunction1(MakeReference(testVal), 1);
    TestFunction2(MakeReference(testVal), 2);
    TestFunction3(MakeReference(testVal), 3);

    TestFunction1(MakeReferenceFromCopy(testVal), 1);
    TestFunction2(MakeReferenceFromCopy(testVal), 2);
    TestFunction3(MakeReferenceFromCopy(testVal), 3);
}

BOOST_AUTO_TEST_CASE(ConstTest)
{
    C3 testVal;

    testVal.c1 = 1;
    testVal.c2 = 2;
    testVal.c3 = 3;

    TestFunctionConst1(MakeConstReference(testVal), 1);
    TestFunctionConst2(MakeConstReference(testVal), 2);
    TestFunctionConst3(MakeConstReference(testVal), 3);

    TestFunctionConst1(MakeConstReferenceFromCopy(testVal), 1);
    TestFunctionConst2(MakeConstReferenceFromCopy(testVal), 2);
    TestFunctionConst3(MakeConstReferenceFromCopy(testVal), 3);

    TestFunctionConst1(MakeReference(testVal), 1);
    TestFunctionConst2(MakeReference(testVal), 2);
    TestFunctionConst3(MakeReference(testVal), 3);

    TestFunctionConst1(MakeReferenceFromCopy(testVal), 1);
    TestFunctionConst2(MakeReferenceFromCopy(testVal), 2);
    TestFunctionConst3(MakeReferenceFromCopy(testVal), 3);
}

BOOST_AUTO_TEST_SUITE_END()
