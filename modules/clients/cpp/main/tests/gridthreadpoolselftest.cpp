/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include <boost/test/unit_test.hpp>

#include "gridgain/impl/utils/gridthreadpool.hpp"

static const unsigned int DEFAULT_NTHREADS = 5;

class TestGridThreadPoolCountingTask: public GridThreadPoolTask {
public:
    virtual void run() {
        counter++;
    }

    virtual void cancel() {}

    static TGridAtomicInt counter;
};

TGridAtomicInt TestGridThreadPoolCountingTask::counter;

BOOST_AUTO_TEST_SUITE(GridThreadPoolSelfTest)

BOOST_AUTO_TEST_CASE(testAllTasksExecuted) {
    GridThreadPool tp(DEFAULT_NTHREADS);

    TestGridThreadPoolCountingTask::counter = 0; //reset the counter

    const int ntasks = 1000;

    for (int i = 0; i < ntasks; i++) {
        TGridThreadPoolTaskPtr task(new TestGridThreadPoolCountingTask());

        tp.execute(task);
    }

    tp.waitForEmptyQueue();

    tp.shutdown(); //we also need to join running tasks

    BOOST_CHECK_EQUAL( TestGridThreadPoolCountingTask::counter, ntasks );
    BOOST_CHECK_EQUAL( tp.queueSize(), 0 );
}

class TestGridThreadPoolExceptionThrowingTask: public GridThreadPoolTask {
public:
    virtual void run() {
        throw std::exception();
    }

    virtual void cancel() {}
};

BOOST_AUTO_TEST_CASE(testTaskThrowingException) {
    GridThreadPool tp(1);

    TGridThreadPoolTaskPtr task(new TestGridThreadPoolExceptionThrowingTask());

    tp.execute(task);
}

class TestGridThreadPoolCleanupCheckingTask: public GridThreadPoolTask {
public:
    TestGridThreadPoolCleanupCheckingTask(TGridAtomicBool& cleanupFlag): flag(cleanupFlag) {}

    virtual void run() {
        //do nothing
    }

    ~TestGridThreadPoolCleanupCheckingTask() {
        flag = true;
    }

    virtual void cancel() {}

private:
    TGridAtomicBool& flag;
};

BOOST_AUTO_TEST_CASE(testTaskProperlyCleanup) {
    GridThreadPool tp(1);

    TGridAtomicBool cleanupFlag;
    cleanupFlag = false;

    {
        TGridThreadPoolTaskPtr task(new TestGridThreadPoolCleanupCheckingTask(cleanupFlag));

        tp.execute(task);
    }

    tp.waitForEmptyQueue();

    tp.shutdown(); //wait for task to complete

    BOOST_CHECK( cleanupFlag );
}

BOOST_AUTO_TEST_SUITE_END()
