/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_TOPOLOGY_REFRESH_THREAD_HPP_INCLUDED
#define GRID_CLIENT_TOPOLOGY_REFRESH_THREAD_HPP_INCLUDED

#include <boost/asio.hpp>
#include <boost/thread/thread.hpp>
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

/**
 * Service class for threads with periodic actions.
 */
class GridClientRecurringEventThread {
public:
    /** Event listener for this thread. */
    class EventListener {
    public:
        /** Virtual destructor - this class will be extended. */
        virtual ~EventListener() {
        }

        /** Method that will be called when timer event arrives. */
        virtual void onTimerEvent() = 0;
    };

    /**
     * Constructor for thread - after this thread will notify listener on a periodic basis.
     *
     * @param pRefreshIntervalInSec Interval in milliseconds between notifications.
     * @param pLstnr Listener to notify.
     */
    GridClientRecurringEventThread(long refreshIntervalInMillis, EventListener& pLstnr)
            : work(new boost::asio::io_service::work(ioSrv)),
              thread(boost::bind(&boost::asio::io_service::run, &ioSrv)),
              refreshIntervalInMillis(refreshIntervalInMillis),
              timer(ioSrv, boost::posix_time::millisec(refreshIntervalInMillis)),
              lstnr(pLstnr) {
        timer.async_wait(boost::bind(&GridClientRecurringEventThread::onTimerHandler, this,
                        boost::asio::placeholders::error));
    }

    /**
     * Virtual destructor - stops event thread before leave.
     */
    virtual ~GridClientRecurringEventThread() {
        timer.cancel();

        delete work;

        thread.interrupt();
        thread.join();
    }

protected:
    /** Handler for Boost timer. */
    void onTimerHandler(const boost::system::error_code& error) {
        if (error == boost::asio::error::operation_aborted || thread.interruption_requested())
            return;

        lstnr.onTimerEvent();

        timer.expires_at(timer.expires_at() + boost::posix_time::milliseconds(refreshIntervalInMillis));

        timer.async_wait(boost::bind(&GridClientRecurringEventThread::onTimerHandler, this,
                        boost::asio::placeholders::error));
    }

private:
    /** Boost IO service. */
    boost::asio::io_service ioSrv;

    boost::asio::io_service::work* work;

    /** Internal Boost thread. */
    boost::thread thread;

    /** Refresh interval in milliseconds. */
    long refreshIntervalInMillis;

    /** Boost timer. */
    boost::asio::deadline_timer timer;

    /** Listener to notify when timer event arrives. */
    EventListener& lstnr;
};

#endif
