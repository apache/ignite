/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDTESTCOMMON_HPP_
#define GRIDTESTCOMMON_HPP_

#include <string>
#include <vector>
#include <map>

#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>

#include "gridgain/gridclientconfiguration.hpp"
#include "gridgain/gridclienttopologylistener.hpp"
#include "gridgain/gridclientnode.hpp"

using std::string;
using std::vector;

/** Cache name to use. */
const string CACHE_NAME = "partitioned";

/** Number of configured caches. */
const int CACHE_CNT = 5;

/** TCP port. */
const int TEST_TCP_PORT = 10080;

/** Host name. */
const string TEST_HOST = "127.0.0.1";

/** Default thread count for multithreaded tests. */
const int DLFT_THREADS_CNT = 20;

/** Client credentials. */
const string CREDS = "s3cret";

/**
 * Topology listener, that forms a node map according
 * to topology events.
 */
class GridTestTopologyListener : public GridClientTopologyListener {
public:
    /** Virtual destructor. */
    virtual ~GridTestTopologyListener() {}

    /**
     * Callback for new nodes joining the remote grid.
     *
     * @param node New remote node.
     */
    virtual void onNodeAdded(const GridClientNode& node) {
        boost::lock_guard<boost::mutex> g(nodesMux);

        nodes[node.getNodeId()] = node;
    }

    /**
     * Callback for nodes leaving the remote grid.
     *
     * @param node Left node.
     */
    virtual void onNodeRemoved(const GridClientNode& node) {
        boost::lock_guard<boost::mutex> g(nodesMux);

        nodes.erase(node.getNodeId());
    }

    /**
     * @return Number of nodes in map.
     */
    size_t getNodesCount() const {
        boost::lock_guard<boost::mutex> g(nodesMux);

        return nodes.size();
    }

private:
    /** Nodes map. */
    std::map<GridClientUuid, GridClientNode> nodes;

    /** Nodes map mutex. */
    mutable boost::mutex nodesMux;
};

string genRandomUniqueString(string prefix = "");

/**
 * Returns current system time in milliseconds.
 */
int64_t currentTimeMillis();

/**
 * Returns a random string, based on the prefix.
 *
 * @param prefix A prefix for random string.
 * @return Random string.
 */
string getRandomName(string prefix);

/**
 * Returns a client configuration for the test.
 */
GridClientConfiguration clientConfig();

/**
 * Tries to read the value of command line arg from provided
 * argc-argv pair. The value of an arg is expected to be
 * next to it's name. For exampe, if we run the program in
 * the following way:
 *
 * <pre>./program -param 1234</pre>
 *
 * "-param" will be the name of an argument, and "1234" will
 * be the value.
 *
 * @param argc The argc parameter in main().
 * @param argv The argv parameter in main().
 * @param name The name of the argument to read.
 * @param defaultValue The default value, if the argument (or value) was not found.
 */
template<class T>
T readArg(int argc, const char** argv, const string& name, const T& defaultValue) {
    for (int i = 1; i < argc - 1; i++) {
        if (name == argv[i]) {
            return boost::lexical_cast<T>(argv[i + 1]);
        }
    }

    return defaultValue;
}

/**********************************************************************
 * Stuff for multithreaded tests.                                     *
 **********************************************************************/

extern boost::mutex checkMux;

/**
 * Thread-safe expression check.
 *
 * This macro assumes the expression is boolean.
 *
 * @param P Predicate to check.
 */
#define SYNC_CHECK(E) {                            \
    bool v = E;                                    \
    boost::lock_guard<boost::mutex> g(checkMux);   \
    BOOST_CHECK(v);                                \
}

/**
 * Thread-safe equality check.
 *
 * @param L Left operand.
 * @param R Right operand.
 */
#define SYNC_CHECK_EQUAL(L, R) {                 \
    boost::lock_guard<boost::mutex> g(checkMux); \
    BOOST_CHECK_EQUAL(L, R);                     \
}

/**
 * Thread-safe exception check.
 *
 * @param E Expression to run.
 * @param T Expected exception type.
 */
#define SYNC_CHECK_THROW(E, T) {                 \
    boost::lock_guard<boost::mutex> g(checkMux); \
    BOOST_CHECK_THROW(E, T);                     \
}

/**
 * Runs the specified callable within multiple threads.
 *
 * @param f A callable to run (function, bind, function object or any class with operator() ).
 * @param nthreads Number of running threads.
 */
template<class F>
void multithreaded(F f, int nthreads = DLFT_THREADS_CNT) {
    vector<boost::thread> threads;
    vector<boost::unique_future<void> > futures;

    for (int i = 0; i < nthreads; i++) {
        boost::packaged_task<void> pt(f);
        futures.push_back(pt.get_future());

        threads.push_back(boost::thread(boost::move(pt)));
    }

    // wait for all threads to finish
    for (auto i = threads.begin(); i != threads.end(); i++)
        i->join();

    // gather all futures to possibly get an exception
    for (auto i = futures.begin(); i != futures.end(); i++)
        i->get();
}

#endif /* GRIDTESTCOMMON_HPP_ */
