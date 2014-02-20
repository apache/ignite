// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDTHREADPOOL_HPP_
#define GRIDTHREADPOOL_HPP_

#include <vector>
#include <queue>
#include <memory>
#include <stdexcept>

#include <boost/thread/thread.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/function.hpp>

#include "gridgain/gridclienttypedef.hpp"

/**
 * Exception, that is thrown on attempt of performing
 * operations whit already shutdown thread pool.
 */
class GridThreadPoolIsShutdownException: public std::logic_error {
public:
    GridThreadPoolIsShutdownException();
};

/**
 * A task, that can be performed by GridThreadPool.
 *
 * A run() method, which concrete implementation
 * should override, will be run by a pool thread.
 */
class GridThreadPoolTask {
public:
    virtual ~GridThreadPoolTask() {}

    /**
     * A method that is run by a pool thread.
     */
    virtual void run() = 0;

    /**
     * A method that is upcalled when this task is
     * cancelled.
     */
    virtual void cancel() = 0;
};

/** A thread pool task pointer. */
typedef std::shared_ptr<GridThreadPoolTask> TGridThreadPoolTaskPtr;

/**
 * An abstract class for simple tasks with no cancellation
 * capability.
 */
class GridSimpleThreadPoolTask: public GridThreadPoolTask {
    /**
     * Does nothing.
     */
    virtual void cancel() {}
};

/**
 * A thread pool that is capable of performing
 * tasks over a number of pre-launched threads.
 *
 * All tasks are put into a queue and taken out
 * by the pool threads.
 *
 * The queue size is unbounded.
 *
 */
class GridThreadPool: private boost::noncopyable {
public:
    /**
     * Constructs a thread pool and starts the threads.
     *
     * @param nthreads Number of threads in a pool.
     */
    GridThreadPool(unsigned int nthreads);

    /**
     * Destroys the thread pool.
     *
     * shutdown() method is called prior to destruction.
     */
    virtual ~GridThreadPool();

    /**
     * Puts task to a queue for further execution by
     * a pool thread.
     *
     * @param task A task to execute.
     * @throws GridThreadPoolIsShutdownException If thread pool is already shut down.
     */
    void execute(TGridThreadPoolTaskPtr& task);

    /**
     * Returns a current number of tasks in task queue.
     */
    size_t queueSize() const;

    /**
     * Blocks current thread until task queue is empty
     * (all tasks are executed).
     *
     * Made for testing purposes.
     *
     * @throws GridThreadPoolIsShutdownException If thread pool is already shut down.
     */
    void waitForEmptyQueue();

    /**
     * Shuts down this thread pool.
     *
     * Upon completion all pool threads are stopped,
     * and the pool cannot be used anymore.
     *
     * This method may be called any number of times, and is called automatically
     * from destructor.
     *
     * @throws GridThreadPoolIsShutdownException If thread pool is already shut down.
     */
    void shutdown();

private:
    /**
     * A thread safe queue that provides blocking
     * operations (offer(), poll()...).
     *
     * @param T A contained element type.
     * @param ImplT A container type, that is used as
     * an underlying implementation.
     */
    template<class T, class ImplT = std::list<T> >
    class SynchronizedBlockingQueue {
        /** Reverse iterator type. */
        typedef typename ImplT::iterator Iterator;

    public:
        /**
         * Adds an element to the end of the queue.
         *
         * If the queue is full, blocks until a
         * room appears (another thread has taken
         * an element from the queue).
         *
         * @param elem An element to put.
         */
        void offer(const T& elem) {
            {
                boost::lock_guard<boost::mutex> guard(mux);

                impl.push_back(elem);
            }

            //TODO: GG-3998: do this only if the queue was really empty
            emptyCond.notify_one(); // Notify the thread that is waiting on empty queue.
        }

        /**
         * Takes an element from the head of the queue.
         *
         * The element is removed from the queue.
         * If the queue is empty, blocks until an element
         * appears in the queue (another thread has put it).
         */
        T poll() {
            boost::unique_lock<boost::mutex> lock(mux);

            // Wait if the queue is empty.
            while (impl.empty())
                emptyCond.wait(lock); // The mutex is unlocked on entry and locked on exit.

            T ret = impl.front();
            impl.pop_front();

            if (impl.empty())
                // Notify the thread that is waiting for queue to become empty.
                nonEmptyCond.notify_one();

            return ret; // The mutex is unlocked on return.
        }

        /**
         * Returns the current number of elements in a queue.
         */
        size_t size() const {
            boost::lock_guard<boost::mutex> guard(mux);

            return impl.size();
        }

        /**
         * Blocks current thread until queue empties
         * (another threads have taken all the elements off it).
         */
        void waitUntilEmpty() {
            boost::unique_lock<boost::mutex> lock(mux);

            // Wait if the queue is not empty.
            while (!impl.empty())
                nonEmptyCond.wait(lock); //the mutex is unlocked on entry and locked on exit

            // The mutex is unlocked on return.
        }

        /**
         * Performs a given operation for each element in the queue.
         *
         * @param f An operation to perform.
         */
        void forEach(std::function<void (T)> f) {
            boost::lock_guard<boost::mutex> guard(mux);

            for (typename ImplT::iterator i = impl.begin(); i != impl.end(); i++)
                f(*i);
        }

    private:
        /** Container implementation. */
        ImplT impl;

        /** Mutex that provides concurrency. */
        mutable boost::mutex mux;

        /** Condition that allows to wait on empty queue. */
        boost::condition_variable emptyCond;

        /** Condition that allows to wait on empty queue. */
        boost::condition_variable nonEmptyCond;
    };

    /**
     * A function that is run by worker threads.
     */
    void worker();

    /** Worker threads. */
    typedef std::vector<boost::thread> TThreadList;
    TThreadList threads;

    /** Tasks queue. */
    SynchronizedBlockingQueue<TGridThreadPoolTaskPtr> tasks;

    /** Shutdown flag. */
    TGridAtomicBool isShutdown;
};

typedef std::shared_ptr<GridThreadPool> TGridThreadPoolPtr;

#endif /* GRIDTHREADPOOL_HPP_ */
