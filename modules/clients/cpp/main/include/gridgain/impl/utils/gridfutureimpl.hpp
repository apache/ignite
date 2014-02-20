// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDFUTURE_IMPL_INCLUDED
#define GRIDFUTURE_IMPL_INCLUDED

#include <boost/thread/future.hpp>
#include <boost/variant.hpp>

#include "gridgain/gridfuture.hpp"
#include "gridgain/gridclientexception.hpp"
#include "gridgain/impl/utils/gridthreadpool.hpp"

/**
 * Adapter to run Boost packaged tasks in GridThreadPool.
 */
template<class T>
class GridThreadPoolTaskPackagedTaskAdapter: public GridThreadPoolTask {
public:
    /**
     * Constructs the adapter from rvalue reference to packaged task.
     *
     * Original packaged task will be cleared.
     */
    GridThreadPoolTaskPackagedTaskAdapter(boost::packaged_task<T>&& pt):
        pt(static_cast<boost::packaged_task<T>&&>(pt)) {
    }

    /**
     * Runs the packaged task.
     */
    virtual void run() {
        pt();
    }

    /**
     * Destroys the packaged task.
     */
    virtual void cancel() {
        // Replace pt with an empty packaged_task and,
        // thus, invoke it's destruction.
        pt = boost::packaged_task<T>();
    }

private:
    /** A packaged task to run. */
    boost::packaged_task<T> pt;
};

/**
 * Boost-based implementation of the Grid future.
 */
class GridBoolFutureImpl: public GridBoolFuture {
public:
    GridBoolFutureImpl(TGridThreadPoolPtr& threadPool): threadPool(threadPool) {}

    /**
     * Waits for future to finish and returns the result of operation.
     *
     * @return true if the operation succeeded, false otherwise.
     */
    virtual bool get() const {
        return const_cast<GridBoolFutureImpl*>(this)->fut_.get();
    }

    /**
     * Waits for future to finish and returns the result of operation.
     *
     * @param timeout Time to wait for result.
     * @return <tt>true</tt> if the operation succeeded, <tt>false</tt> otherwise.
     * @throws GridClientException In case an operation has thrown exception (it is rethrown).
     * @throws GridClientFutureTimeoutException If timed out before future finishes.
     */
    virtual bool get(const boost::posix_time::time_duration& timeout) const {
        bool waitRes = const_cast<GridBoolFutureImpl*>(this)->fut_.timed_wait(timeout);

        if(waitRes) {
            return const_cast<GridBoolFutureImpl*>(this)->fut_.get();
        }
        else {
            throw GridClientFutureTimeoutException();
        }
    }

    /**
     * Initializes future based on boost packaged task.
     *
     * @param pt Boost packaged task.
     */
    void task(boost::packaged_task<bool>& pt) {
        fut_ = pt.get_future();

        TGridThreadPoolTaskPtr task(new GridThreadPoolTaskPackagedTaskAdapter<bool>(boost::move(pt)));

        threadPool->execute(task);
    }

private:
    /** Wrapped boost future. */
    boost::unique_future<bool> fut_;

    /** Thread pool to run the task. */
    TGridThreadPoolPtr threadPool;
};

/**
 * Special future for reporting errors, which are initialy known
 * (such as, when client is already stopped).
 *
 * This future throws exception when client calls get().
 *
 * @param ExT Type of exception to throw.
 */
template<class ExT>
class GridBoolFailFutureImpl: public GridBoolFuture {
public:
    GridBoolFailFutureImpl() {}

    /**
     * Throws exception right away, when this method is called.
     *
     * @throws Exception of type ExT.
     */
    virtual bool get() const {
        throw ExT();
    }

    /**
     * Throws exception right away, when this method is called.
     *
     * @throws Exception of type ExT.
     */
    virtual bool get(const boost::posix_time::time_duration& timeout) const {
        throw ExT();
    }
};

/**
 * Generic future implementation.
 */
template<class T>
class GridFutureImpl: public GridFuture<T> {
public:

    /** Default constructor. */
    GridFutureImpl(TGridThreadPoolPtr& threadPool)
            : success_(false), threadPool(threadPool) {
    }

    virtual ~GridFutureImpl() {
    }

    /**
     * Was the operation successful or not. If it was not successful, result is not valid.
     *
     * @return Flag indicating the  success of operation.
     */
    virtual bool success() const {
        return success_;
    }

    /**
     * Result of operation. If the operation was not successful, it is not valid.
     *
     * @return Result value.
     */
    virtual T result() const {
        return t_;
    }

    /**
     * Waits for future to complete and fills success and result values.
     *
     * @return Operation result.
     */
    virtual T get() {
        t_ = const_cast<GridFutureImpl*>(this)->fut_.get();

        success_ = true;

        return t_;
    }

    /**
     * Waits for future to finish and returns the result of operation.
     *
     * @param timeout Time to wait for result.
     * @return Operation result.
     * @throws GridClientException In case an operation has thrown exception (it is rethrown).
     * @throws GridClientFutureTimeoutException If timed out before future finishes.
     */
    virtual T get(const boost::posix_time::time_duration& timeout) {
        bool waitRes = const_cast<GridFutureImpl*>(this)->fut_.timed_wait(timeout);

        if(waitRes) {
            t_ = const_cast<GridFutureImpl*>(this)->fut_.get();

            success_ = true;

            return t_;
        }
        else {
            success_ = false;

            throw GridClientFutureTimeoutException();
        }
    }

    /**
     * Init future from Boost packaged task with typed return type.
     *
     * @param pt Packaged task to use for this future.
     */
    void task(boost::packaged_task<T>& pt) {
        fut_ = pt.get_future();

        TGridThreadPoolTaskPtr task(new GridThreadPoolTaskPackagedTaskAdapter<T>(boost::move(pt)));

        threadPool->execute(task);
    }

private:
    /** Value returned from operation. */
    T t_;

    /** Boost future holder. */
    boost::unique_future<T> fut_;

    /** Success/failure flag. */
    bool success_;

    /** Thread pool to run the task. */
    TGridThreadPoolPtr threadPool;
};

/**
 * Special future for reporting errors, which are initialy known
 * (such as, when client is already stopped).
 *
 * This future throws exception when client calls get().
 *
 * @param T Type of result, this future is expected to return.
 * @param ExT Type of exception to throw.
 */
template<class T, class ExT>
class GridFailFutureImpl: public GridFuture<T> {
public:
    /**
     * Always returns false - the operation was initially invalid.
     */
    virtual bool success() const {
        return false;
    }

    /**
     * Result of operation. An exception of type ExT will be thrown.
     */
    virtual T result() const {
        throw ExT();
    }

    /**
     * Throws exception right away.
     *
     * @return Nothing, exception is thrown.
     */
    virtual T get() {
        throw ExT();
    }

    /**
     * Throws exception right away.
     *
     * @return Nothing, exception is thrown.
     */
    virtual T get(const boost::posix_time::time_duration& timeout) {
        throw ExT();
    }
};

#endif 
