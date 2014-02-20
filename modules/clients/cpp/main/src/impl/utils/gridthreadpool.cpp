// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include "gridgain/impl/utils/gridthreadpool.hpp"


GridThreadPoolIsShutdownException::GridThreadPoolIsShutdownException():
    std::logic_error("Thread pool is shut down.") {
}

GridThreadPool::GridThreadPool(unsigned int nthreads) {
    isShutdown = false;

    if (nthreads == 0)
        throw std::invalid_argument("GridThreadPool: number of threads cannot be 0.");

    for (unsigned int i = 0; i < nthreads; i++) {
        boost::thread t(boost::bind(&GridThreadPool::worker, this)); // Starts the thread.

        threads.push_back(boost::move(t));
    }
}

GridThreadPool::~GridThreadPool() {
    this->shutdown();
}

void GridThreadPool::execute(TGridThreadPoolTaskPtr& task) {
    if (isShutdown)
        return;

    tasks.offer(task);
}

void GridThreadPool::waitForEmptyQueue() {
    tasks.waitUntilEmpty();
}

void GridThreadPool::shutdown() {
    if (isShutdown)
        return;

    isShutdown = true;

    // Interrupt all threads.
    for (auto i = threads.begin(); i != threads.end(); i++)
        i->interrupt();

    // Wait for all threads to finish.
    for (auto i = threads.begin(); i != threads.end(); i++)
        i->join();

    threads.clear();

    tasks.forEach([] (TGridThreadPoolTaskPtr task) {
        task->cancel();
    });
}

void GridThreadPool::worker() {
    while(!boost::this_thread::interruption_requested() && !isShutdown) {
        // Get the task from queue, block if empty.
        TGridThreadPoolTaskPtr task = tasks.poll();

        try {
            task->run();
        }
        catch (boost::thread_interrupted&) {
            return;
        }
        catch (...) {
            // We can do nothing if the task fails.
            // User can only rely on handling exceptions inside the task.
        }
    }
}

size_t GridThreadPool::queueSize() const {
    return tasks.size();
}
