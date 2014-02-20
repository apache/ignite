// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include <iostream>

#include "gridgain/impl/utils/gridthreadpool.hpp"
#include "gridtestcommon.hpp"

using namespace std;

/**
 * Thread pool load test task that simply calculates factorial for a given number.
 */
class GridThreadPoolLoadTestTask: public GridSimpleThreadPoolTask {
public:
    GridThreadPoolLoadTestTask(unsigned int n): niters(n) {}

    /**
     * Function that is run by a thread in a pool.
     */
    virtual void run() {
        unsigned int fact = 1;

        for(unsigned int i = 0; i < niters; i++) {
            fact *= i;
        }
    }

private:
    /** Number of iterations in a counting loop. */
    unsigned int niters;
};

/**
 * Load test for GridThreadPool class.
 *
 * This test creates ntasks number of tasks,
 * each of which performs niters iterations
 * of a simple calculation.
 *
 * These tasks are then run over GridThreadPool
 * with unlimited queue and nthreads threads.
 *
 * The test measures the time taken to complete
 * all the tasks.
 */
int main() {
    const unsigned int nthreads = 5;
    //const unsigned int ntasks = 60000000; //100000000; //100M
    const unsigned int ntasks = 6000000; //6M
    const unsigned int niters = 100000; //1M

    GridThreadPool tp(nthreads); //5 threads, unlimited queue

    double startTime = currentTimeMillis();

    cout << "Starting " << ntasks << " tasks, " << niters << " iterations each...\n"
            << "Using " << nthreads << " threads.\n";

    for(unsigned int i = 1; i < ntasks; i++) {
        TGridThreadPoolTaskPtr task(new GridThreadPoolLoadTestTask(niters));

        tp.execute(task);

        if(i % 1000000 == 0) {
            cout << "Started " << i << " tasks.\n";
        }
    }

    cout << "Waiting for tasks to complete...\n";

    //tp.waitForCompletion();
    int currSize;
    while((currSize = tp.queueSize()) > 0) {
        cout << "Current queue size: " << currSize << "\n";
        ::sleep(1);
    }

    cout << "All tasks completed. Total time: " << ::round(currentTimeMillis() - startTime) << "ms.\n"
            << "Shutting down threads...\n";

    tp.shutdown();

    cout << "Threads shut down. Test complete.\n";

    return 0;
}
