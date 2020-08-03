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

#include <ignite/common/promise.h>
#include <ignite/future.h>

using namespace ignite;
using namespace ignite::common;

/**
 * Utility to make auto pointer from value.
 *
 * @param val Value.
 * @return Auto pointer.
 */
template<typename T>
std::auto_ptr<T> MakeAuto(const T& val)
{
    return std::auto_ptr<T>(new T(val));
}

/**
 * Checks if the error is of type IgniteError::IGNITE_ERR_FUTURE_STATE.
 */
inline bool IsFutureError(const IgniteError& err)
{
    return err.GetCode() == IgniteError::IGNITE_ERR_FUTURE_STATE;
}

/**
* Checks if the error is of type IgniteError::IGNITE_ERR_UNKNOWN.
*/
inline bool IsUnknownError(const IgniteError& err)
{
    return err.GetCode() == IgniteError::IGNITE_ERR_UNKNOWN;
}

BOOST_AUTO_TEST_SUITE(FutureTestSuite)

BOOST_AUTO_TEST_CASE(SharedStateIntValue)
{
    SharedState<int> sharedState;
    int expected = 42;

    bool set = sharedState.WaitFor(100);
    BOOST_CHECK(!set);

    sharedState.SetValue(MakeAuto(expected));

    set = sharedState.WaitFor(100);
    BOOST_REQUIRE(set);

    set = sharedState.WaitFor(100);
    BOOST_REQUIRE(set);

    sharedState.Wait();
    int val = sharedState.GetValue();

    BOOST_CHECK_EQUAL(val, expected);

    int val2 = sharedState.GetValue();

    BOOST_CHECK_EQUAL(val2, expected);

    BOOST_CHECK_EXCEPTION(sharedState.SetValue(MakeAuto(0)), IgniteError, IsFutureError);
    BOOST_CHECK_EXCEPTION(sharedState.SetValue(MakeAuto(expected)), IgniteError, IsFutureError);
    BOOST_CHECK_EXCEPTION(sharedState.SetError(IgniteError()), IgniteError, IsFutureError);
}

BOOST_AUTO_TEST_CASE(SharedStateStringValue)
{
    SharedState<std::string> sharedState;
    std::string expected = "Lorem ipsum";

    bool ready = sharedState.WaitFor(100);
    BOOST_CHECK(!ready);

    sharedState.SetValue(MakeAuto(expected));

    ready = sharedState.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = sharedState.WaitFor(100);
    BOOST_REQUIRE(ready);

    sharedState.Wait();
    std::string val = sharedState.GetValue();

    BOOST_CHECK_EQUAL(val, expected);

    std::string val2 = sharedState.GetValue();

    BOOST_CHECK_EQUAL(val2, expected);

    BOOST_CHECK_EXCEPTION(sharedState.SetError(IgniteError()), IgniteError, IsFutureError);
    BOOST_CHECK_EXCEPTION(sharedState.SetValue(MakeAuto(expected)), IgniteError, IsFutureError);
    BOOST_CHECK_EXCEPTION(sharedState.SetValue(MakeAuto(std::string("Hello world"))), IgniteError, IsFutureError);
}

BOOST_AUTO_TEST_CASE(SharedStateVoidValue)
{
    SharedState<void> sharedState;

    bool ready = sharedState.WaitFor(100);
    BOOST_CHECK(!ready);

    sharedState.SetValue();

    ready = sharedState.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = sharedState.WaitFor(100);
    BOOST_REQUIRE(ready);

    sharedState.Wait();
    sharedState.GetValue();
    sharedState.GetValue();

    BOOST_CHECK_EXCEPTION(sharedState.SetError(IgniteError()), IgniteError, IsFutureError);
    BOOST_CHECK_EXCEPTION(sharedState.SetValue(), IgniteError, IsFutureError);
}

BOOST_AUTO_TEST_CASE(SharedStateIntError)
{
    SharedState<int> sharedState;

    bool ready = sharedState.WaitFor(100);
    BOOST_CHECK(!ready);

    sharedState.SetError(IgniteError(IgniteError::IGNITE_ERR_UNKNOWN, "Test"));

    ready = sharedState.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = sharedState.WaitFor(100);
    BOOST_REQUIRE(ready);

    sharedState.Wait();

    BOOST_CHECK_EXCEPTION(sharedState.GetValue(), IgniteError, IsUnknownError);

    BOOST_CHECK_EXCEPTION(sharedState.SetValue(MakeAuto(42)), IgniteError, IsFutureError);
    BOOST_CHECK_EXCEPTION(sharedState.SetError(IgniteError()), IgniteError, IsFutureError);
}

BOOST_AUTO_TEST_CASE(SharedStateVoidError)
{
    SharedState<void> sharedState;

    bool ready = sharedState.WaitFor(100);
    BOOST_CHECK(!ready);

    sharedState.SetError(IgniteError(IgniteError::IGNITE_ERR_UNKNOWN, "Test"));

    ready = sharedState.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = sharedState.WaitFor(100);
    BOOST_REQUIRE(ready);

    sharedState.Wait();

    BOOST_CHECK_EXCEPTION(sharedState.GetValue(), IgniteError, IsUnknownError);
    BOOST_CHECK_EXCEPTION(sharedState.SetError(IgniteError()), IgniteError, IsFutureError);
    BOOST_CHECK_EXCEPTION(sharedState.SetValue(), IgniteError, IsFutureError);
}

BOOST_AUTO_TEST_CASE(FutureIntValue)
{
    Promise<int> promise;
    int expected = 42;

    Future<int> future1 = promise.GetFuture();
    Future<int> future2 = promise.GetFuture();

    bool ready = future1.WaitFor(100);
    BOOST_CHECK(!ready);

    ready = future2.WaitFor(100);
    BOOST_CHECK(!ready);

    promise.SetValue(MakeAuto(expected));

    ready = future1.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = future1.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = future2.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = future2.WaitFor(100);
    BOOST_REQUIRE(ready);

    future2.Wait();
    future1.Wait();

    int val1 = future1.GetValue();

    BOOST_CHECK_EQUAL(val1, expected);

    int val2 = future1.GetValue();

    BOOST_CHECK_EQUAL(val2, expected);

    int val3 = future2.GetValue();

    BOOST_CHECK_EQUAL(val3, expected);

    int val4 = future2.GetValue();

    BOOST_CHECK_EQUAL(val4, expected);

    BOOST_CHECK_EXCEPTION(promise.SetValue(MakeAuto(0)), IgniteError, IsFutureError);
    BOOST_CHECK_EXCEPTION(promise.SetValue(MakeAuto(expected)), IgniteError, IsFutureError);
    BOOST_CHECK_EXCEPTION(promise.SetError(IgniteError()), IgniteError, IsFutureError);
}

BOOST_AUTO_TEST_CASE(FutureStringValue)
{
    Promise<std::string> promise;
    std::string expected = "Lorem Ipsum";

    Future<std::string> future1 = promise.GetFuture();
    Future<std::string> future2 = promise.GetFuture();

    bool ready = future1.WaitFor(100);
    BOOST_CHECK(!ready);

    ready = future2.WaitFor(100);
    BOOST_CHECK(!ready);

    promise.SetValue(MakeAuto(expected));

    ready = future1.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = future1.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = future2.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = future2.WaitFor(100);
    BOOST_REQUIRE(ready);

    future2.Wait();
    future1.Wait();

    std::string val1 = future1.GetValue();

    BOOST_CHECK_EQUAL(val1, expected);

    std::string val2 = future1.GetValue();

    BOOST_CHECK_EQUAL(val2, expected);

    std::string val3 = future2.GetValue();

    BOOST_CHECK_EQUAL(val3, expected);

    std::string val4 = future2.GetValue();

    BOOST_CHECK_EQUAL(val4, expected);

    BOOST_CHECK_EXCEPTION(promise.SetValue(MakeAuto(std::string("Hello Ignite"))), IgniteError, IsFutureError);
    BOOST_CHECK_EXCEPTION(promise.SetValue(MakeAuto(expected)), IgniteError, IsFutureError);
    BOOST_CHECK_EXCEPTION(promise.SetError(IgniteError()), IgniteError, IsFutureError);
}

BOOST_AUTO_TEST_CASE(FutureVoidValue)
{
    Promise<void> promise;

    Future<void> future1 = promise.GetFuture();
    Future<void> future2 = promise.GetFuture();

    bool ready = future1.WaitFor(100);
    BOOST_CHECK(!ready);

    ready = future2.WaitFor(100);
    BOOST_CHECK(!ready);

    promise.SetValue();

    ready = future1.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = future1.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = future2.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = future2.WaitFor(100);
    BOOST_REQUIRE(ready);

    future2.Wait();
    future1.Wait();

    future1.GetValue();
    future1.GetValue();
    future2.GetValue();
    future2.GetValue();

    BOOST_CHECK_EXCEPTION(promise.SetValue(), IgniteError, IsFutureError);
    BOOST_CHECK_EXCEPTION(promise.SetValue(), IgniteError, IsFutureError);
    BOOST_CHECK_EXCEPTION(promise.SetError(IgniteError()), IgniteError, IsFutureError);
}

BOOST_AUTO_TEST_CASE(FutureIntError)
{
    Promise<int> promise;

    Future<int> future1 = promise.GetFuture();
    Future<int> future2 = promise.GetFuture();

    bool ready = future1.WaitFor(100);
    BOOST_CHECK(!ready);

    ready = future2.WaitFor(100);
    BOOST_CHECK(!ready);

    promise.SetError(IgniteError(IgniteError::IGNITE_ERR_UNKNOWN, "Test"));

    ready = future1.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = future1.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = future2.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = future2.WaitFor(100);
    BOOST_REQUIRE(ready);

    future2.Wait();
    future1.Wait();

    BOOST_CHECK_EXCEPTION(future1.GetValue(), IgniteError, IsUnknownError);
    BOOST_CHECK_EXCEPTION(future2.GetValue(), IgniteError, IsUnknownError);

    BOOST_CHECK_EXCEPTION(promise.SetValue(MakeAuto(42)), IgniteError, IsFutureError);
    BOOST_CHECK_EXCEPTION(promise.SetError(IgniteError()), IgniteError, IsFutureError);
}

BOOST_AUTO_TEST_CASE(FutureVoidError)
{
    Promise<void> promise;

    Future<void> future1 = promise.GetFuture();
    Future<void> future2 = promise.GetFuture();

    bool ready = future1.WaitFor(100);
    BOOST_CHECK(!ready);

    ready = future2.WaitFor(100);
    BOOST_CHECK(!ready);

    promise.SetError(IgniteError(IgniteError::IGNITE_ERR_UNKNOWN, "Test"));

    ready = future1.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = future1.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = future2.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = future2.WaitFor(100);
    BOOST_REQUIRE(ready);

    future2.Wait();
    future1.Wait();

    BOOST_CHECK_EXCEPTION(future1.GetValue(), IgniteError, IsUnknownError);
    BOOST_CHECK_EXCEPTION(future2.GetValue(), IgniteError, IsUnknownError);

    BOOST_CHECK_EXCEPTION(promise.SetValue(), IgniteError, IsFutureError);
    BOOST_CHECK_EXCEPTION(promise.SetError(IgniteError()), IgniteError, IsFutureError);
}

BOOST_AUTO_TEST_CASE(FutureIntBroken)
{
    Promise<int>* promise = new Promise<int>();

    Future<int> future1 = promise->GetFuture();
    Future<int> future2 = promise->GetFuture();

    bool ready = future1.WaitFor(100);
    BOOST_CHECK(!ready);

    ready = future2.WaitFor(100);
    BOOST_CHECK(!ready);

    delete promise;

    ready = future1.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = future1.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = future2.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = future2.WaitFor(100);
    BOOST_REQUIRE(ready);

    future2.Wait();
    future1.Wait();

    BOOST_CHECK_EXCEPTION(future1.GetValue(), IgniteError, IsFutureError);
    BOOST_CHECK_EXCEPTION(future2.GetValue(), IgniteError, IsFutureError);
}

BOOST_AUTO_TEST_CASE(FutureVoidBroken)
{
    Promise<void>* promise = new Promise<void>();

    Future<void> future1 = promise->GetFuture();
    Future<void> future2 = promise->GetFuture();

    bool ready = future1.WaitFor(100);
    BOOST_CHECK(!ready);

    ready = future2.WaitFor(100);
    BOOST_CHECK(!ready);

    delete promise;

    ready = future1.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = future1.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = future2.WaitFor(100);
    BOOST_REQUIRE(ready);

    ready = future2.WaitFor(100);
    BOOST_REQUIRE(ready);

    future2.Wait();
    future1.Wait();

    BOOST_CHECK_EXCEPTION(future1.GetValue(), IgniteError, IsFutureError);
    BOOST_CHECK_EXCEPTION(future2.GetValue(), IgniteError, IsFutureError);
}

BOOST_AUTO_TEST_SUITE_END()
