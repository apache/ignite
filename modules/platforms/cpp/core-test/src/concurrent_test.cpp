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

#include <ignite/common/concurrent.h>

using namespace ignite;
using namespace ignite::common::concurrent;

BOOST_AUTO_TEST_SUITE(ConcurrentTestSuite)

BOOST_AUTO_TEST_CASE(TestAtomic32)
{
    int32_t val = 1;

    BOOST_REQUIRE(Atomics::CompareAndSet32(&val, 1, 2));
    BOOST_REQUIRE(val == 2);

    BOOST_REQUIRE(!Atomics::CompareAndSet32(&val, 3, 1));
    BOOST_REQUIRE(val == 2);

    BOOST_REQUIRE(Atomics::CompareAndSet32Val(&val, 2, 3) == 2);
    BOOST_REQUIRE(val == 3);

    BOOST_REQUIRE(Atomics::CompareAndSet32Val(&val, 4, 2) == 3);
    BOOST_REQUIRE(val == 3);

    BOOST_REQUIRE(Atomics::IncrementAndGet32(&val) == 4);
    BOOST_REQUIRE(val == 4);

    BOOST_REQUIRE(Atomics::DecrementAndGet32(&val) == 3);
    BOOST_REQUIRE(val == 3);
}

BOOST_AUTO_TEST_CASE(TestAtomic64)
{
    int64_t val = 1;

    BOOST_REQUIRE(Atomics::CompareAndSet64(&val, 1, 2));
    BOOST_REQUIRE(val == 2);

    BOOST_REQUIRE(!Atomics::CompareAndSet64(&val, 3, 1));
    BOOST_REQUIRE(val == 2);

    BOOST_REQUIRE(Atomics::CompareAndSet64Val(&val, 2, 3) == 2);
    BOOST_REQUIRE(val == 3);

    BOOST_REQUIRE(Atomics::CompareAndSet64Val(&val, 4, 2) == 3);
    BOOST_REQUIRE(val == 3);

    BOOST_REQUIRE(Atomics::IncrementAndGet64(&val) == 4);
    BOOST_REQUIRE(val == 4);

    BOOST_REQUIRE(Atomics::DecrementAndGet64(&val) == 3);
    BOOST_REQUIRE(val == 3);
}

BOOST_AUTO_TEST_CASE(TestThreadLocal)
{
    int32_t idx1 = ThreadLocal::NextIndex();
    int32_t idx2 = ThreadLocal::NextIndex();
    BOOST_REQUIRE(idx2 > idx1);

    BOOST_REQUIRE(ThreadLocal::Get<int32_t>(idx1) == 0);

    ThreadLocal::Set(idx1, 1);
    BOOST_REQUIRE(ThreadLocal::Get<int32_t>(idx1) == 1);

    ThreadLocal::Set(idx1, 2);
    BOOST_REQUIRE(ThreadLocal::Get<int32_t>(idx1) == 2);

    ThreadLocal::Remove(idx1);
    BOOST_REQUIRE(ThreadLocal::Get<int32_t>(idx1) == 0);
    
    ThreadLocal::Set(idx1, 1);
    BOOST_REQUIRE(ThreadLocal::Get<int32_t>(idx1) == 1);

    ThreadLocal::Remove(idx1);
}

BOOST_AUTO_TEST_CASE(TestThreadLocalInstance)
{
    ThreadLocalInstance<int32_t> val;

    BOOST_REQUIRE(val.Get() == 0);

    val.Set(1);
    BOOST_REQUIRE(val.Get() == 1);

    val.Set(2);
    BOOST_REQUIRE(val.Get() == 2);

    val.Remove();
    BOOST_REQUIRE(val.Get() == 0);

    val.Set(1);
    BOOST_REQUIRE(val.Get() == 1);

    val.Remove();
}

struct SharedPointerTarget
{
    bool deleted;

    SharedPointerTarget() : deleted(false)
    {
        // No-op.
    }
};

void DeleteSharedPointerTarget(SharedPointerTarget* ptr)
{
    ptr->deleted = true;
}

BOOST_AUTO_TEST_CASE(TestSharedPointer)
{
    // 1. Test the simples scenario.
    SharedPointerTarget* target = new SharedPointerTarget();

    SharedPointer<SharedPointerTarget>* ptr1 = 
        new SharedPointer<SharedPointerTarget>(target, DeleteSharedPointerTarget);

    delete ptr1;
    BOOST_REQUIRE(target->deleted);

    target->deleted = false;

    // 2. Test copy ctor.
    ptr1 = new SharedPointer<SharedPointerTarget>(target, DeleteSharedPointerTarget);
    SharedPointer<SharedPointerTarget>* ptr2 = new SharedPointer<SharedPointerTarget>(*ptr1);

    delete ptr1;
    BOOST_REQUIRE(!target->deleted);

    delete ptr2;
    BOOST_REQUIRE(target->deleted);

    target->deleted = false;

    // 3. Test assignment logic.
    ptr1 = new SharedPointer<SharedPointerTarget>(target, DeleteSharedPointerTarget);

    SharedPointer<SharedPointerTarget> ptr3 = *ptr1;

    delete ptr1;
    BOOST_REQUIRE(!target->deleted);

    ptr3 = SharedPointer<SharedPointerTarget>();
    BOOST_REQUIRE(target->deleted);

    target->deleted = false;

    // 4. Test self-assignment.
    ptr1 = new SharedPointer<SharedPointerTarget>(target, DeleteSharedPointerTarget);

    *ptr1 = *ptr1;

    delete ptr1;

    BOOST_REQUIRE(target->deleted);

    // 5. Tear-down.
    delete target;    
}

struct SharedPointerTargetFromThis : public EnableSharedFromThis<SharedPointerTargetFromThis>
{
    bool& deleted;

    SharedPointerTargetFromThis(bool& deleted) : deleted(deleted)
    {
        deleted = false;
    }
};

void DeleteSharedPointerTarget(SharedPointerTargetFromThis* ptr)
{
    ptr->deleted = true;
    delete ptr;
}

BOOST_AUTO_TEST_CASE(TestEnableSharedFromThis)
{
    typedef SharedPointerTargetFromThis TestT;

    bool deleted;

    // 1. Test the simple scenario.
    TestT* target = new TestT(deleted);
    BOOST_CHECK(!deleted);

    SharedPointer<TestT>* ptr1 = new SharedPointer<TestT>(target, DeleteSharedPointerTarget);
    BOOST_CHECK(!deleted);

    delete ptr1;
    BOOST_CHECK(deleted);

    // 2. Test copy ctor.
    target = new TestT(deleted);
    BOOST_CHECK(!deleted);

    ptr1 = new SharedPointer<TestT>(target, DeleteSharedPointerTarget);
    BOOST_CHECK(!deleted);

    SharedPointer<TestT>* ptr2 = new SharedPointer<TestT>(*ptr1);
    BOOST_CHECK(!deleted);

    delete ptr1;
    BOOST_CHECK(!deleted);

    delete ptr2;
    BOOST_CHECK(deleted);

    // 3. Test assignment logic.
    target = new TestT(deleted);
    BOOST_CHECK(!deleted);

    ptr1 = new SharedPointer<TestT>(target, DeleteSharedPointerTarget);
    BOOST_CHECK(!deleted);

    SharedPointer<TestT> ptr3 = *ptr1;
    BOOST_CHECK(!deleted);

    delete ptr1;
    BOOST_CHECK(!deleted);

    ptr3 = SharedPointer<TestT>();
    BOOST_CHECK(deleted);

    // 4. Test self-assignment.
    target = new TestT(deleted);
    BOOST_CHECK(!deleted);

    ptr1 = new SharedPointer<TestT>(target, DeleteSharedPointerTarget);

    *ptr1 = *ptr1;

    delete ptr1;

    BOOST_CHECK(deleted);

    // 5. Test shared from this
    target = new TestT(deleted);
    BOOST_CHECK(!deleted);

    ptr1 = new SharedPointer<TestT>(target, DeleteSharedPointerTarget);
    BOOST_CHECK(!deleted);

    ptr3 = target->SharedFromThis();
    BOOST_CHECK(!deleted);

    delete ptr1;
    BOOST_CHECK(!deleted);

    ptr3 = SharedPointer<TestT>();
    BOOST_CHECK(deleted);
}

BOOST_AUTO_TEST_CASE(ConditionVariableBasic)
{
    CriticalSection cs;
    ConditionVariable cv;

    CsLockGuard guard(cs);

    bool notified = cv.WaitFor(cs, 100);

    BOOST_REQUIRE(!notified);

    cv.NotifyOne();

    notified = cv.WaitFor(cs, 100);

    BOOST_REQUIRE(!notified);

    cv.NotifyAll();

    notified = cv.WaitFor(cs, 100);

    BOOST_REQUIRE(!notified);
}

BOOST_AUTO_TEST_CASE(ManualEventBasic)
{
    ManualEvent evt;

    bool triggered = evt.WaitFor(100);
    BOOST_CHECK(!triggered);

    evt.Set();

    triggered = evt.WaitFor(100);
    BOOST_REQUIRE(triggered);

    triggered = evt.WaitFor(100);
    BOOST_REQUIRE(triggered);

    evt.Wait();
    evt.Reset();

    triggered = evt.WaitFor(100);
    BOOST_CHECK(!triggered);
}

BOOST_AUTO_TEST_SUITE_END()
