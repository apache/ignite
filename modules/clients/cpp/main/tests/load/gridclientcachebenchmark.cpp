// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include <vector>

#include <gridgain/gridgain.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>
#include <boost/foreach.hpp>

#include "gridtestcommon.hpp"

/** Number of keys, used in PUT/GET operations. */
const int KEY_COUNT = 1000;

/** Size of arrays used as stored values. */
const int VALUE_LENGTH = 1024*4;

/** Probability of put operation in percents; probability of get is 100 - WRITE_PROB. */
const int WRITE_PROB_PERCENT = 20;

/** Cached values for store. */
std::vector<std::vector<int8_t> > values;

/** GridGain client interface. */
TGridClientPtr client;

/**
 * Generates random values into the vector.
 *
 * This function is NOT thread-safe.
 *
 * @param vec A vector to generate random values into.
 * @param length A number of values to generate.
 */
void genRandomBytes(std::vector<int8_t>& vec, int length) {
    for (int i = 0; i < length; i++) {
        vec.push_back((int8_t)rand());
    }
}

/**
 * Returns a random int between 0 and max.
 *
 * @param max A maximum value of a random integer.
 * @param seed A seed to use. Modifiable. Needs to be passed each time.
 */
int randomInt(int max, unsigned int* seed) {
    return rand_r(seed) % (max + 1);
}

class TestThread: private boost::noncopyable {
public:
    /**
     * Constructes the test thread.
     *
     * @param iterationCnt How many iterations to perform.
     */
    TestThread(int iterationCnt) {
        iters = 0;

        seed = time(NULL);

        thread = boost::thread(boost::bind(&TestThread::run, this, iterationCnt));
    }

    void run(int iterationCnt) {
        try {
            TGridClientDataPtr data = client->data(CACHE_NAME);

            for (int i = 0; i < iterationCnt; i++) {
                if (randomInt(100, &seed) <= WRITE_PROB_PERCENT) {
                    data->put(randomInt(KEY_COUNT - 1, &seed), values.at(randomInt(KEY_COUNT - 1, &seed)));
                }
                else {
                    data->get((int16_t)randomInt(KEY_COUNT - 1, &seed));
                }

                iters++;
            }
        }
        catch (GridClientException& e) {
            std::cerr << "GridClientException: " << e.what() << "\n";
        }
        catch (...) {
            std::cerr << "Unknown exception.\n";
        }
    }

    void join() {
        thread.join();
    }

    int getIters() {
        return iters;
    }

private:
    boost::thread thread;

    int iters;

    /** A random seed used as a state for thread-safe random functions. */
    unsigned int seed;
};

typedef std::shared_ptr<TestThread> TestThreadPtr;

/**
 * Prints tests summary,
 *
 * @param workers Collection of test worker threads.
 * @param startTime Time when test eas started.
 */
void printSummary(std::vector<TestThreadPtr>& workers, long startTime) {
    long total = 0;

    int thCnt = workers.size();

    for (std::vector<TestThreadPtr>::iterator i = workers.begin(); i != workers.end(); i++)
        total += (*i)->getIters();

    double timeSpent = ((double)(currentTimeMillis() - startTime)) / 1000;

    printf("%8d, %12.0f, %12.0f, %12li, %12.2lf\n", thCnt, (double)total/timeSpent,
            (double)total/timeSpent/thCnt, total, timeSpent);
}

/**
 * Runs the cache benchmark.
 *
 * @param threadCnt Number of submitting threads.
 * @param iterationCnt Number of operations per thread.
 */
void gridClientCacheBenchmark(int threadCnt, int iterationCnt) {
    values.clear();

    // initialize values cache
    for (int i = 0; i < KEY_COUNT; i++) {
        std::vector<int8_t> vec;

        genRandomBytes(vec, VALUE_LENGTH);

        assert(vec.size() == (size_t)VALUE_LENGTH);

        values.push_back(vec);
    }

    assert(values.size() == (size_t)KEY_COUNT);

    GridClientConfiguration cfg = clientConfig();

    std::vector<GridSocketAddress> servers;
    servers.push_back(GridSocketAddress("127.0.0.1", 11211));
    cfg.servers(servers);

    client = GridClientFactory::start(cfg);

    std::vector<TestThreadPtr> workers;

    double startTime = currentTimeMillis();

    for(int i = 0; i < threadCnt; i++) {
        workers.push_back(TestThreadPtr(new TestThread(iterationCnt)));
    }

    //join all threads
    for (std::vector<TestThreadPtr>::iterator i = workers.begin(); i != workers.end(); i++) {
        (*i)->join();
    }

    printSummary(workers, startTime);

    workers.clear();
    client.reset();

    GridClientFactory::stopAll();
}

int main(int argc, const char** argv) {
    // initialize random seed
    srand(time(NULL));

    printf("%8s, %12s, %12s, %12s, %12s\n", "Threads", "It./s.", "It./s.*th.", "Iters.", "Time (sec.)");

    for (int i = 4; i <= 128; i *= 2) {
        gridClientCacheBenchmark(i, 10000);
    }

    return EXIT_SUCCESS;
}
