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

#include "ignite/smart_pointer.h"

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

    ~LivenessMarker()
    {
        flag = false;
    }

private:
    bool& flag;
};


template<typename T>
SmartPointer<typename T::element_type>* Pack(T ptr)
{
    return new SmartPointerHolder<T>(std::move(ptr));
}

BOOST_AUTO_TEST_SUITE(SmartPointerTestSuite)

BOOST_AUTO_TEST_CASE(StdSharedPointerTestBefore)
{
    bool objAlive = false;

    std::shared_ptr<LivenessMarker> shared = std::make_shared<LivenessMarker>(objAlive);

    BOOST_CHECK(objAlive);

    SmartPointer<LivenessMarker>* smart = Pack(shared);

    BOOST_CHECK(objAlive);

    shared.reset();

    BOOST_CHECK(objAlive);

    delete smart;

    BOOST_CHECK(!objAlive);
}

BOOST_AUTO_TEST_CASE(StdSharedPointerTestAfter)
{
    bool objAlive = false;

    std::shared_ptr<LivenessMarker> shared = std::make_shared<LivenessMarker>(objAlive);

    BOOST_CHECK(objAlive);

    SmartPointer<LivenessMarker>* smart = Pack(shared);

    BOOST_CHECK(objAlive);

    delete smart;

    BOOST_CHECK(objAlive);

    shared.reset();

    BOOST_CHECK(!objAlive);
}

BOOST_AUTO_TEST_CASE(StdAutoPointerTest)
{
    bool objAlive = false;

    std::auto_ptr<LivenessMarker> autop(new LivenessMarker(objAlive));

    BOOST_CHECK(objAlive);

    SmartPointer<LivenessMarker>* smart = Pack(autop);

    BOOST_CHECK(objAlive);

    delete smart;

    BOOST_CHECK(!objAlive);
}

BOOST_AUTO_TEST_CASE(StdUniquePointerTest)
{
    bool objAlive = false;

    std::unique_ptr<LivenessMarker> unique(new LivenessMarker(objAlive));

    BOOST_CHECK(objAlive);

    SmartPointer<LivenessMarker>* smart = Pack(std::move(unique));
    
    BOOST_CHECK(objAlive);

    delete smart;

    BOOST_CHECK(!objAlive);
}

BOOST_AUTO_TEST_CASE(BoostSharedPointerTestBefore)
{
    bool objAlive = false;

    boost::shared_ptr<LivenessMarker> shared = boost::make_shared<LivenessMarker>(objAlive);

    BOOST_CHECK(objAlive);

    SmartPointer<LivenessMarker>* smart = Pack(shared);

    BOOST_CHECK(objAlive);

    shared.reset();

    BOOST_CHECK(objAlive);

    delete smart;

    BOOST_CHECK(!objAlive);
}

BOOST_AUTO_TEST_CASE(BoostSharedPointerTestAfter)
{
    bool objAlive = false;

    boost::shared_ptr<LivenessMarker> shared = boost::make_shared<LivenessMarker>(objAlive);

    BOOST_CHECK(objAlive);

    SmartPointer<LivenessMarker>* smart = Pack(shared);

    BOOST_CHECK(objAlive);

    delete smart;

    BOOST_CHECK(objAlive);

    shared.reset();

    BOOST_CHECK(!objAlive);
}

BOOST_AUTO_TEST_CASE(BoostUniquePointerTest)
{
    bool objAlive = false;

    boost::interprocess::unique_ptr<LivenessMarker> unique(new LivenessMarker(objAlive));

    BOOST_CHECK(objAlive);

    SmartPointer<LivenessMarker>* smart = Pack(boost::move(unique));

    BOOST_CHECK(objAlive);

    delete smart;

    BOOST_CHECK(!objAlive);
}

BOOST_AUTO_TEST_SUITE_END()