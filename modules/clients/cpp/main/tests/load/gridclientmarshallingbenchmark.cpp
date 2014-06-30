/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include <vector>
#include <iostream>
#include <iomanip>

#include <gridgain/gridgain.hpp>
#include <boost/thread.hpp>
#include <boost/program_options.hpp>
#include "boost/date_time/posix_time/posix_time.hpp"
#include <boost/shared_ptr.hpp>

#include "gridtestcommon.hpp"
#include "gridgain/gridclientnodemetricsbean.hpp"
#include "gridgain/impl/cmd/gridclientmessagelogrequestcommand.hpp"
#include "gridgain/impl/cmd/gridclientmessagelogresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagetopologyrequestcommand.hpp"
#include "gridgain/impl/cmd/gridclientmessagetopologyresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagecacheresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagecacherequestcommand.hpp"
#include "gridgain/impl/cmd/gridclientmessagecachemodifyresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagecachemetricsresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagecachegetresult.hpp"
#include "gridgain/impl/cmd/gridclientmessagetaskrequestcommand.hpp"
#include "gridgain/impl/cmd/gridclientmessagetaskresult.hpp"
#include "gridgain/impl/marshaller/portable/gridportablemarshaller.hpp"

/** Map of command line arguments. */
boost::program_options::variables_map vm;

/**
 * Class representing one thread testing the marshalling.
 */
class TestThread : private boost::noncopyable {

public:
    /**
     * Constructs the test thread.
     *
     * @param iterationCnt How many iterations to perform.
     */
    TestThread() {
        thread = boost::thread(boost::bind(&TestThread::run, this));
    }

    void run() {
        try {
            GridPortableMarshaller marsh;

            GridCacheRequestCommand cmd = GridCacheRequestCommand(GridCacheRequestCommand::GridCacheOperation::PUT);

            vector<int8_t> token(100, 1);

            int64_t key = 1000;
            int64_t val = 1000;
            //std::string val("string string string");

            cmd.sessionToken(token);
            cmd.setCacheName("partitioned");
            cmd.setClientId(GridClientUuid("550e8400-e29b-41d4-a716-446655440000"));
            cmd.setDestinationId(GridClientUuid("550e8400-e29b-41d4-a716-446655440000"));
            cmd.setKey(key);
            cmd.setRequestId(100);
            cmd.setValue(val);

            long maxiterations = vm["nummessages"].as<long>();

            iters = 0;

            while (++iters != maxiterations) {
                GridClientCacheRequest msg;

                marsh.createMessage(msg, cmd);

                boost::shared_ptr<std::vector<int8_t>> bytes = marsh.marshalSystemObject(msg);
            }
        }
        catch (GridClientException& e) {
            std::cerr << "GridClientException: " << e.what() << "\n";
            exit(1);
        }
        catch (...) {
            std::cerr << "Unknown exception.\n";
            exit(1);
        }
    }

    /** Joins the test thread. */
    void join() {
        thread.join();
    }

    /** Returns number of iterations completed. */
    long getIters() {
        return iters;
    }

private:
    /** Thread implementation. */
    boost::thread thread;

    /** Number of completed iterations. */
    int iters;

    /** A random seed used as a state for thread-safe random functions. */
    unsigned int seed;
};

typedef std::shared_ptr<TestThread> TestThreadPtr;

int main(int argc, const char** argv) {
    using namespace std;
    using namespace boost::program_options;

    // Declare the supported options.
    options_description desc("Allowed options");
    desc.add_options()("help", "produce help message")
        ("threads", value<int>()->required(), "Number of threads")
        ("nummessages", value<long>()->required(), "Number of messages to process per thread");

    store(parse_command_line(argc, argv, desc), vm);
    notify(vm);

    if (!vm["help"].empty()) {
        std::cout << desc;
        exit(1);
    }

    cout << "Number of threads: " << vm["threads"].as<int>() << "\n";
    cout << "Number of iterations per threads: " << vm["nummessages"].as<long>() << "\n";

    long totalOps = 0;
    double totalSecs = 0.0f;

    std::vector<TestThreadPtr> workers;

    for (int i = 0; i < vm["threads"].as<int>(); i++)
        workers.push_back(TestThreadPtr(new TestThread()));

    boost::posix_time::ptime time_start(boost::posix_time::microsec_clock::local_time());

    for (std::vector<TestThreadPtr>::iterator i = workers.begin(); i != workers.end(); i++) {
        (*i)->join();
        totalOps += (*i)->getIters();
    }

    boost::posix_time::ptime time_end(boost::posix_time::microsec_clock::local_time());
    boost::posix_time::time_duration duration(time_end - time_start);
    totalSecs += duration.total_milliseconds() / 1000.0f;

    workers.clear();

    double res = (totalOps / totalSecs);

    std::cout << "Total ops/sec " << std::fixed << std::setw(10) << std::setprecision(3) << res << std::endl;

    return EXIT_SUCCESS;
}
